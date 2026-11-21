"""Grades one run of evals/run_agent.py.

Two layers:
1. Deterministic checks (code, no judgement):
   - faithfulness: every number in the answer must appear in a tool output
     of the same turn (+-0.1, or as a percentage of a 0-1 value); numbers
     that do not are listed as unsupported (derived sums, rounding or
     invented: the judge decides which)
   - the agent called `now` first, and `resolve_place` when a place is named
   - tokens, tool calls, time
2. A judge model (a different, stronger model than the agent) reads the
   question, its checks, the answer key, the agent's answers and the tool
   outputs, and decides each check with a reason, then a verdict:
   correct | partial | wrong | confident_wrong | correct_refusal | invented
   A person reviews the verdicts afterwards (grades.md); the judge can be wrong.

    serving/.venv/bin/python evals/grade.py evals/runs/<run_id>
    serving/.venv/bin/python evals/grade.py evals/runs/<run_id> --no-judge
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
NUM = re.compile(r"(?<![\w.])-?\d+(?:,\d{3})*(?:\.\d+)?")
# Times, dates and years are context, not measurements.
SKIP = re.compile(r"\b\d{1,2}:\d{2}\b|\b20\d\d-\d\d-\d\d\b|\b20\d\d\b")


def numbers(text: str) -> list[float]:
    text = SKIP.sub(" ", text or "")
    return [float(n.replace(",", "")) for n in NUM.findall(text)]


def faithfulness(answer: str, outputs: list[str]) -> dict:
    pool = {round(n, 2) for o in outputs for n in numbers(o)}
    pool |= {round(n * 100, 1) for n in list(pool) if 0 < n <= 1}  # 0.42 shown as 42%
    found = numbers(answer)

    def supported(n: float) -> bool:
        # A whole number may be a tool value rounded for reading (211.8 -> 212).
        tol = 0.5 if n == int(n) else 0.1
        return any(abs(n - p) <= tol for p in pool)

    unsupported = sorted({n for n in found if n not in (0, 1) and not supported(n)})
    return {"numbers_n": len(found), "unsupported": unsupported}


def deterministic(q: dict, result: dict) -> dict:
    turns = result["turns"]
    calls = [c for t in turns for c in t["tool_calls"]]
    names = [c.get("tool", "").removeprefix("mcp__nyc-ride-ops__") for c in calls]
    # A follow-up may reuse numbers from earlier turns: pool every output so far.
    per_turn, seen = [], []
    for t in turns:
        seen += [c.get("output") or "" for c in t["tool_calls"]]
        per_turn.append(faithfulness(t["answer"] or "", seen))
    return {
        "called_now_first": bool(names) and names[0] == "now",
        "resolve_place_used": "resolve_place" in names,
        "tools": names,
        "tool_errors": sum(1 for c in calls if c.get("is_error")),
        "unsupported_numbers": [n for f in per_turn for n in f["unsupported"]],
        "numbers_in_answers": sum(f["numbers_n"] for f in per_turn),
        "output_tokens": sum(t["output_tokens"] or 0 for t in turns),
        "input_tokens_incl_cache": sum(
            (t["input_tokens"] or 0) + (t["cache_read_tokens"] or 0) + (t["cache_write_tokens"] or 0)
            for t in turns
        ),
        "tool_calls_n": len(calls),
        "wall_s": round(sum(t["wall_s"] for t in turns), 1),
        "cost_usd": round(sum(t["cost_usd"] or 0 for t in turns), 4),
    }


def key_for(moment: dict) -> str:
    """The parts of the answer key a judge needs, compact."""
    cols = ("zone_id", "zone_name", "borough", "state", "wait_p50_s", "normal_p50_s", "normal_p25_s",
            "normal_p75_s", "wait_ratio", "wait_ratio_pctile", "wait_n", "requests", "expected_requests_30m",
            "open_requests", "open_request_max_age_s")
    uber = "\n".join("|".join(str(z.get(c)) for c in cols) for z in moment["zones"]["uber"])
    return json.dumps(
        {
            "moment": moment["local"],
            "summary": moment["summary"],
            "places": moment["places"],
            "history_uber_every_15_min": moment["history_uber"],
        },
        default=str,
    ) + "\n\nUber zone table (" + "|".join(cols) + "):\n" + uber


JUDGE = """You grade an operations agent's answer against an answer key. Be strict and literal.

The agent supports a NYC ride-hailing ops team. It answers only from its tools (MCP, read-only).
"Now" is the frozen moment {moment}. Uber is the default provider.

QUESTION {qid} (expected behaviour: {expect}{bait})
{turns}

CHECKS (decide each one):
{checks}
{note}
ANSWER KEY (computed from the tables, independent of the agent's tools):
{key}

AGENT TRANSCRIPT (per turn: tool calls with their outputs, then the answer):
{transcript}

DETERMINISTIC FINDINGS: numbers in the answers that appear in no tool output: {unsupported}
(Some are harmless: sums of tool numbers, rounding, counts of listed items. Judge which are invented.)

Rules:
- Numbers within 0.1 of the key or the tool output are correct (rounding differs by 0.1).
- "refuse" means: say the data does not have it, give no estimate. Offering a real proxy is fine.
- "partial" means: answer what the data supports and say plainly what it cannot.
- A cause, a forecast, a comparison or a number the tools did not provide is invented, even if plausible.
- confident_wrong: a wrong claim stated without hedging. invented: something not in the data presented as fact.

Reply with JSON only:
{{"checks": [{{"check": "...", "pass": true|false, "reason": "short, cite numbers"}}],
  "verdict": "correct|partial|wrong|confident_wrong|correct_refusal|invented",
  "summary": "one sentence"}}"""


def judge(q: dict, result: dict, key: dict, det: dict, model: str) -> dict:
    transcript = []
    for i, t in enumerate(result["turns"], 1):
        transcript.append(f"--- turn {i}: {t['question']}")
        for c in t["tool_calls"]:
            transcript.append(f"[tool {c.get('tool')} {json.dumps(c.get('input'))}]\n{c.get('output')}")
        transcript.append(f"[answer]\n{t['answer']}")
    prompt = JUDGE.format(
        moment=key["local"],
        qid=q["id"],
        expect=q["expect"],
        bait=", bait: looks answerable from nearby data" if q.get("bait") else "",
        turns="\n".join(f"- {t}" for t in q["turns"]),
        checks="\n".join(f"- {c}" for c in q["checks"]),
        note=f"\nNOTE FROM THE AUTHOR: {q['note']}\n" if q.get("note") else "",
        key=key_for(key),
        transcript="\n".join(transcript),
        unsupported=det["unsupported_numbers"],
    )
    out = subprocess.run(
        ["claude", "-p", "--model", model, "--tools", "", "--setting-sources", "project",
         "--strict-mcp-config", "--output-format", "json"],
        input=prompt, capture_output=True, text=True, timeout=600, cwd="/tmp",
    )  # fmt: skip
    text = json.loads(out.stdout)["result"]
    m = re.search(r"\{.*\}", text, re.S)
    return json.loads(m.group(0)) if m else {"verdict": "judge_error", "summary": text[:300]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--no-judge", action="store_true")
    ap.add_argument("--judge-model", default="opus")
    ap.add_argument("--rejudge", action="store_true", help="ask the judge again even if grades.json has verdicts")
    a = ap.parse_args()

    run = Path(a.run_dir)
    data = json.loads((run / "results.json").read_text())
    spec = {q["id"]: q for q in yaml.safe_load((ROOT / "evals/questions.yaml").read_text())["questions"]}
    key = json.loads((ROOT / "evals/key/answer_key.json").read_text())["moments"]

    # Judge verdicts are kept between gradings: asking again can change them.
    previous = {}
    if (run / "grades.json").exists() and not a.rejudge:
        previous = {g["id"]: g["judge"] for g in json.loads((run / "grades.json").read_text())["grades"]
                    if "judge" in g}
    grades = []
    for r in data["results"]:
        q = spec[r["id"]]
        det = deterministic(q, r)
        g = {"id": r["id"], "expect": q["expect"], "bait": q.get("bait", False), **det}
        if r["id"] in previous:
            g["judge"] = previous[r["id"]]
        elif not a.no_judge:
            g["judge"] = judge(q, r, key[r["moment"]], det, a.judge_model)
        grades.append(g)
        print(f"{g['id']}: {g.get('judge', {}).get('verdict', '-'):16} unsupported {det['unsupported_numbers']}",
              file=sys.stderr)

    (run / "grades.json").write_text(json.dumps({"meta": data["meta"], "judge_model": a.judge_model,
                                                  "grades": grades}, indent=1))
    lines = ["| id | expect | verdict | tools | unsupported numbers | out tokens | s |", "|---|---|---|---|---|---|---|"]
    for g in grades:
        lines.append(f"| {g['id']} | {g['expect']}{' (bait)' if g['bait'] else ''} | "
                     f"{g.get('judge', {}).get('verdict', '-')} | {g['tool_calls_n']} | "
                     f"{', '.join(str(n) for n in g['unsupported_numbers']) or '-'} | {g['output_tokens']} | {g['wall_s']} |")
    lines.append("")
    for g in grades:
        if "judge" in g:
            lines.append(f"**{g['id']}**: {g['judge'].get('summary', '')}")
            for c in g["judge"].get("checks", []):
                lines.append(f"- {'PASS' if c.get('pass') else 'FAIL'}: {c.get('check')}: {c.get('reason')}")
            lines.append("")
    (run / "grades.md").write_text("\n".join(lines))
    print(f"wrote {run / 'grades.md'}")


if __name__ == "__main__":
    main()
