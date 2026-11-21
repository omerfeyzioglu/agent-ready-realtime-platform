"""Runs the operations agent on the evaluation questions and records
everything it did: each answer, each tool call with its full output, tokens,
cost and time. Grading is a separate step (it reads these records).

    serving/.venv/bin/python evals/run_agent.py                  # all questions
    serving/.venv/bin/python evals/run_agent.py --only q01 q06   # a few
    serving/.venv/bin/python evals/run_agent.py --model opus

Per moment it starts the ops API frozen at that moment (FROZEN_NOW, values
from the Iceberg minute log; no cluster needed) and asks the agent through
the Claude Code CLI in print mode. The agent is sealed the same way as for a
person: its role (agents/ops/role.md) as the workspace instructions, only the
ops API's MCP tools allowed, everything else denied. Each run uses a fresh
empty workspace so nothing carries over between questions (no memory, no
earlier sessions); only project settings are loaded, so the operator's own
settings, plugins and MCP servers never reach the agent.

Output: evals/runs/<UTC time>-<model>/ with one <qid>.jsonl transcript per
question and results.json.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
API = "http://localhost:8000"
CALL_TIMEOUT_S = 600


def workspace() -> Path:
    """A fresh sealed workspace: role, permissions, and the ops API as the
    only MCP server."""
    d = Path(tempfile.mkdtemp(prefix="nyc-ops-eval-"))
    (d / ".claude").mkdir()
    shutil.copy(ROOT / "agents/ops/role.md", d / "CLAUDE.md")
    shutil.copy(ROOT / "agents/ops/permissions.json", d / ".claude/settings.json")
    (d / ".mcp.json").write_text(
        json.dumps({"mcpServers": {"nyc-ride-ops": {"type": "http", "url": f"{API}/mcp"}}})
    )
    return d


def start_api(frozen_now: str) -> subprocess.Popen:
    env = {**os.environ, "FROZEN_NOW": frozen_now}
    p = subprocess.Popen(
        [str(ROOT / "serving/.venv/bin/uvicorn"), "app.main:app", "--port", "8000"],
        cwd=ROOT / "serving",
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    for _ in range(60):
        try:
            urllib.request.urlopen(f"{API}/health", timeout=2)
            break
        except OSError:
            time.sleep(1)
    # Warm-up: the first read of the minute log's metadata takes about a
    # minute from a laptop; do it here so it does not count against the agent.
    t = time.monotonic()
    urllib.request.urlopen(f"{API}/v1/shortages?top=1", timeout=CALL_TIMEOUT_S).read()
    print(f"  API frozen at {frozen_now}, warm in {time.monotonic() - t:.0f} s")
    return p


def stop_api(p: subprocess.Popen) -> None:
    os.killpg(p.pid, signal.SIGTERM)
    try:
        p.wait(timeout=30)
    except subprocess.TimeoutExpired:  # seen: uvicorn + MCP session manager ignoring SIGTERM
        os.killpg(p.pid, signal.SIGKILL)
        p.wait(timeout=10)


def ask(ws: Path, text: str, model: str, session: str | None) -> tuple[list[dict], dict]:
    """One turn in print mode. Returns the raw stream events and a summary."""
    cmd = [
        "claude", "-p", text,
        "--model", model,
        "--output-format", "stream-json", "--verbose",
        "--setting-sources", "project",
        "--strict-mcp-config", "--mcp-config", str(ws / ".mcp.json"),
        # An allowlist, not a denylist: no built-in tool at all (new ones keep
        # appearing: Artifact, PushNotification, CronCreate...), only the ops
        # API's MCP tools. Flags, unlike the workspace's settings file, do
        # not depend on the folder being trusted.
        "--tools", "",
        "--allowed-tools", "mcp__nyc-ride-ops",
    ]  # fmt: skip
    if session:
        cmd += ["--resume", session]
    started = time.monotonic()
    out = subprocess.run(cmd, cwd=ws, capture_output=True, text=True, timeout=CALL_TIMEOUT_S)
    events = [json.loads(line) for line in out.stdout.splitlines() if line.strip().startswith("{")]
    return events, summarize(events, time.monotonic() - started, out.stderr)


def summarize(events: list[dict], wall_s: float, stderr: str) -> dict:
    calls: dict[str, dict] = {}
    order: list[str] = []
    result: dict = {}
    session = None
    for e in events:
        session = e.get("session_id") or session
        msg = e.get("message") or {}
        for block in msg.get("content") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "tool_use":
                calls[block["id"]] = {"tool": block["name"], "input": block.get("input")}
                order.append(block["id"])
            elif block.get("type") == "tool_result":
                content = block.get("content")
                if isinstance(content, list):
                    content = "".join(c.get("text", "") for c in content if isinstance(c, dict))
                calls.setdefault(block["tool_use_id"], {})["output"] = content
                calls[block["tool_use_id"]]["is_error"] = bool(block.get("is_error"))
        if e.get("type") == "result":
            result = e
    usage = result.get("usage") or {}
    return {
        "session_id": session,
        "answer": result.get("result"),
        "is_error": result.get("is_error"),
        "tool_calls": [calls[i] for i in order],
        "num_turns": result.get("num_turns"),
        "input_tokens": usage.get("input_tokens"),
        "cache_read_tokens": usage.get("cache_read_input_tokens"),
        "cache_write_tokens": usage.get("cache_creation_input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "cost_usd": result.get("total_cost_usd"),
        "wall_s": round(wall_s, 1),
        "stderr": stderr.strip()[-500:] or None,
    }


def save(out_dir: Path, meta: dict, results: list) -> None:
    (out_dir / "results.json").write_text(json.dumps({"meta": meta, "results": results}, indent=1))


def rebuild(out_dir: Path, spec: dict) -> None:
    """results.json from the per-question transcripts (each turn ends with a
    result event), for a run that stopped before writing it."""
    by_id = {q["id"]: q for q in spec["questions"]}
    moments = {q["id"]: q["moment"] for q in spec["questions"]}
    results = []
    for f in sorted(out_dir.glob("q*.jsonl")):
        q = by_id[f.stem]
        events, turns, chunk = [json.loads(x) for x in f.read_text().splitlines()], [], []
        for e in events:
            chunk.append(e)
            if e.get("type") == "result":
                turns.append({"question": q["turns"][len(turns)],
                              **summarize(chunk, (e.get("duration_ms") or 0) / 1000, "")})
                chunk = []
        results.append({"id": q["id"], "moment": moments[q["id"]], "expect": q["expect"],
                        "bait": q.get("bait", False), "turns": turns})
    order = {q["id"]: i for i, q in enumerate(spec["questions"])}
    results.sort(key=lambda r: order[r["id"]])
    save(out_dir, {"run_id": out_dir.name, "rebuilt_from_transcripts": True}, results)
    print(f"rebuilt {len(results)} results in {out_dir.relative_to(ROOT)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", help="question ids")
    ap.add_argument("--model", default="sonnet")
    ap.add_argument("--rebuild", help="run dir: rebuild results.json from its transcripts")
    a = ap.parse_args()

    spec = yaml.safe_load((ROOT / "evals/questions.yaml").read_text())
    if a.rebuild:
        rebuild(Path(a.rebuild).resolve(), spec)
        return
    questions = [q for q in spec["questions"] if not a.only or q["id"] in a.only]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%MZ") + f"-{a.model}"
    out_dir = ROOT / "evals/runs" / run_id
    out_dir.mkdir(parents=True)
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True)
    meta = {
        "run_id": run_id,
        "model": a.model,
        "claude_cli": subprocess.run(["claude", "--version"], capture_output=True, text=True).stdout.strip(),
        "git_commit": commit.stdout.strip(),
        "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    results = []
    for moment, m in spec["moments"].items():
        todo = [q for q in questions if q["moment"] == moment]
        if not todo:
            continue
        print(f"{moment} ({m['local']}): {len(todo)} questions")
        api = start_api(m["frozen_now"])
        try:
            for q in todo:
                ws = workspace()
                session, turns, transcript = None, [], []
                for text in q["turns"]:
                    events, s = ask(ws, text, a.model, session)
                    session = s["session_id"]
                    transcript += events
                    turns.append({"question": text, **s})
                (out_dir / f"{q['id']}.jsonl").write_text("".join(json.dumps(e) + "\n" for e in transcript))
                results.append({"id": q["id"], "moment": moment, "expect": q["expect"],
                                "bait": q.get("bait", False), "turns": turns})
                save(out_dir, meta, results)  # after every question: a crash loses nothing
                calls = sum(len(t["tool_calls"]) for t in turns)
                print(f"  {q['id']}: {len(turns)} turn(s), {calls} tool calls, "
                      f"{sum(t['output_tokens'] or 0 for t in turns)} output tokens")
                shutil.rmtree(ws)
        finally:
            stop_api(api)
    meta["finished_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    save(out_dir, meta, results)
    print(f"wrote {out_dir.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
