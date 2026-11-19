"""Audit trail: one JSON line per tool call (who asked what, with which
arguments, what came back, how long it took). Written by the server, so it
does not depend on the agent reporting honestly.

Locally the lines go to serving/audit/tool_calls.jsonl; in the cluster to
stdout, where the log pipeline keeps them."""

import functools
import json
import logging
import time
from datetime import datetime, timezone

from . import config

log = logging.getLogger("audit")


def _summary(result) -> dict:
    """Small, safe summary of a result: never the full payload."""
    if not isinstance(result, dict):
        return {"type": type(result).__name__}
    keys = (
        "status",
        "value",
        "unit",
        "zone_id",
        "provider",
        "window",
        "as_of_utc",
        "count",
        "zones_scored",
        "zones_above_normal_range",
    )
    out = {k: result[k] for k in keys if k in result}
    fresh = result.get("freshness")
    if isinstance(fresh, dict):
        out["freshness"] = fresh.get("status")
    return out


def _write(record: dict) -> None:
    line = json.dumps(record, default=str)
    log.info(line)
    try:
        config.AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with config.AUDIT_LOG.open("a") as f:
            f.write(line + "\n")
    except OSError:
        pass  # read-only filesystem in the cluster: stdout is enough


def audited(fn):
    """Wraps a tool so every call is recorded, including failures."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        started = time.perf_counter()
        record = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "tool": fn.__name__,
            "args": kwargs,
        }
        try:
            result = fn(*args, **kwargs)
            record.update(outcome="ok", result=_summary(result))
            return result
        except Exception as e:
            record.update(outcome="error", error=f"{type(e).__name__}: {e}")
            raise
        finally:
            record["duration_ms"] = round((time.perf_counter() - started) * 1000, 1)
            _write(record)

    return wrapper
