"""One service, two doors to the same tools:
- MCP at /mcp, for agents (any MCP client);
- plain HTTP at /v1/..., for anything else (a dashboard, curl, tests).

Run locally:
    REDIS_PASSWORD=... uvicorn app.main:app --port 8000
"""

import contextlib
import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from . import tools

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

INSTRUCTIONS = """\
Tools for NYC ride-hailing operations, built on a replay of real Uber/Lyft trips.

Rules:
- Time: call `now` first. "Now", "today", "this evening" refer to its replay
  time, never to the real date. Talk in New York local time.
- Places: turn every place name into zones with `resolve_place`; never guess
  zone ids.
- Provider: default is uber; say "Uber data" in the answer. Use lyft or all
  only when asked, and pass on their caveats.
- Window: 30m by default; 15m only when the question is about the last few
  minutes. Say which window you used.
- Driver shortage: there is no driver count. The evidence is a wait that is
  unusual for the zone itself (wait_ratio_pctile >= 95, is_unusual), many
  open requests and old open requests. wait_ratio alone is not enough: some
  zones always swing.
- Cause: only `likely_driver` from the tools. "demand" = requests >= 1.2x
  expected; "unclear" = the data cannot tell. Never say supply side, fewer
  drivers, weather, events or commute as the cause: none is in the data.
- Trust: if a tool says stale, no_activity, low sample or weak baseline, say
  so instead of giving a number. If a topic is not in `get_catalog`, it is not
  available; say that and do not estimate it from other data.
- Capabilities: describe what you can do only from `get_catalog`
  (questions_answerable, not_built_yet), never from general knowledge.
- Coverage: a top list is not the city. For "everywhere?", "whole city", a
  borough, or "is demand up overall", call `get_city_summary`. For open
  requests sitting longest, use find_shortages sort_by=oldest_open_request;
  for demand, sort_by=requests_vs_expected. When a place has several zones,
  check every zone.
- Decisions: give the evidence and options; the person decides (bonuses,
  amounts, actions).
- Counts: use the exact field (zones_scored, zones_unusual,
  zones_above_normal_range); never treat one as another.
- Cite numbers with their unit, window, moment (as_of_local) and sample size.
"""

mcp = MCPServer(
    name="nyc-ride-ops",
    title="NYC ride-hailing operations data",
    instructions=INSTRUCTIONS,
    version="0.1.0",
)

# Every tool only reads; clients may show and call them without a write warning.
READ_ONLY = ToolAnnotations(
    readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False
)
for fn in tools.TOOLS:
    mcp.tool(annotations=READ_ONLY)(fn)

mcp_app = mcp.streamable_http_app(streamable_http_path="/mcp", stateless_http=True, json_response=True)


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    async with mcp.session_manager.run():
        yield


app = FastAPI(title="NYC ride-hailing operations API", version="0.1.0", lifespan=lifespan)


@app.exception_handler(ValueError)
async def bad_input(_req: Request, exc: ValueError) -> JSONResponse:
    """A wrong argument (unknown zone, provider...) is the caller's mistake:
    400 with the reason, not a 500. Over MCP the same message reaches the
    agent as a tool error."""
    return JSONResponse(status_code=400, content={"status": "invalid_input", "detail": str(exc)})


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


app.get("/v1/now")(tools.now)
app.get("/v1/catalog")(tools.get_catalog)
app.get("/v1/places")(tools.resolve_place)
app.get("/v1/shortages")(tools.find_shortages)
app.get("/v1/city")(tools.get_city_summary)
app.get("/v1/zones/{zone_id}/now")(tools.get_zone_now)
app.get("/v1/zones/{zone_id}/history")(tools.get_zone_history)
app.get("/v1/metrics/{name}")(tools.describe_metric)

# MCP last: FastAPI routes above take precedence.
app.mount("/", mcp_app)
