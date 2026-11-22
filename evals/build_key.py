"""Builds the answer key for evals/questions.yaml.

Computed from the tables directly (minute log on Iceberg, batch features on
S3) with the SQL in evals/key/, never through the API, so the agent is not
graded against the code it is being tested on. Output: evals/key/answer_key.json.

    serving/.venv/bin/python evals/build_key.py

Needs AWS credentials that can read the lake (the first Iceberg read takes
about a minute from a laptop: it reads the table's metadata files one by one).
"""

import json
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
HERE = Path(__file__).resolve().parent
BUCKET = "s3://nyc-taxi-raw-969788248726"
NY = ZoneInfo("America/New_York")
WINDOW_MIN = 30  # the agent's default window


def utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def months_back(moment: datetime, weeks: int) -> list[str]:
    """Batch months that hold the weeks before the moment."""
    start, months = moment - timedelta(weeks=weeks), []
    y, m = start.year, start.month
    while (y, m) <= (moment.year, moment.month):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def connect() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    c.sql("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg")
    c.sql("SET TimeZone = 'UTC'")
    c.sql("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')")
    return c


def existing(c, paths: list[str]) -> list[str]:
    """Only months the batch build produced (it ends 2026-07)."""
    out = []
    for p in paths:
        try:
            c.execute(f"SELECT 1 FROM read_parquet('{p}') LIMIT 1").fetchall()
            out.append(p)
        except duckdb.IOException as e:
            if "No files found" not in str(e) and "404 Not Found" not in str(e):
                raise
    return out


def paths(c, moment: datetime) -> dict:
    meta = boto3.client("glue", region_name="us-east-1").get_table(
        DatabaseName="features", Name="feat_zone_minute_live"
    )["Table"]["Parameters"]["metadata_location"]
    zone_hour = existing(
        c,
        [f"{BUCKET}/lake/features/zone_hour/trip_month={m}/data.parquet" for m in months_back(moment, 7)],
    )
    ratios = existing(
        c,
        [
            f"{BUCKET}/lake/features/zone_hour_vs_baseline/trip_month={m}/data.parquet"
            for m in months_back(moment, 9)
        ],
    )
    return {
        "minute_log": meta,
        "zones": f"{BUCKET}/lake/staging/zones/data.parquet",
        "holidays": f"{BUCKET}/lake/staging/holidays/data.parquet",
        "zone_hour_files": "[" + ", ".join(f"'{p}'" for p in zone_hour) + "]",
        "ratio_files": "[" + ", ".join(f"'{p}'" for p in ratios) + "]",
    }


def rows(c, sql: str, params: dict) -> list[dict]:
    cur = c.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def zone_state(c, files: dict, moment: datetime, provider: str) -> list[dict]:
    middle = (moment - timedelta(minutes=WINDOW_MIN / 2)).astimezone(NY)
    sql = (HERE / "key" / "zone_state.sql").read_text().format(**files)
    return rows(
        c,
        sql,
        {
            "moment_utc": moment,
            "local_date": middle.date(),
            "hour": middle.hour,
            "provider": provider,
        },
    )


def history(c, files: dict, moment: datetime, provider: str, zone_ids: list[int], hours: int) -> list[dict]:
    sql = (HERE / "key" / "zone_history.sql").read_text().format(**files)
    return rows(
        c,
        sql,
        {
            "provider": provider,
            "zone_ids": zone_ids,
            "from_utc": moment - timedelta(hours=hours),
            "to_utc": moment,
        },
    )


STATES = ("no_activity", "no_completed_waits", "low_sample", "weak_baseline")
DEMAND_RATIO = 1.2  # requests / expected at or above this = demand (raw ratio)
MIN_EXPECTED = 10  # ranking by requests vs expected only above this


def area(zones: list[dict]) -> dict:
    """City or borough: counts over scored zones, requests summed over zones
    that have an expected count."""
    scored = [z for z in zones if z["state"] == "scored"]
    with_exp = [z for z in zones if z["expected_requests_raw"]]
    req = sum(z["requests"] or 0 for z in with_exp)
    exp = sum(z["expected_requests_raw"] for z in with_exp)
    ratios = sorted(z["wait_ratio_raw"] for z in scored)
    return {
        "zones": len(zones),
        "zones_scored": len(scored),
        "zones_above_normal_range": sum(1 for z in scored if z["wait_p50_s"] > (z["normal_p75_raw"] or 1e9)),
        "zones_unusual": sum(1 for z in scored if (z["wait_ratio_pctile"] or 0) >= 95),
        "median_wait_ratio": round(statistics.median(ratios), 2) if ratios else None,
        "requests": req,
        "expected_requests": round(exp, 1),
        "requests_vs_expected": round(req / exp, 3) if exp else None,
        "demand": (req / exp >= DEMAND_RATIO) if exp else None,
        "open_requests": sum(z["open_requests"] or 0 for z in zones),
        "oldest_open_request_s": max(
            (z["open_request_max_age_s"] for z in zones if z["open_request_max_age_s"] is not None),
            default=None,
        ),
    }


def summary(state: list[dict]) -> dict:
    scored = [z for z in state if z["state"] == "scored"]
    ranked = sorted(
        scored,
        key=lambda z: (
            z["wait_ratio_pctile"] is not None,
            z["wait_ratio_pctile"] or 0,
            z["wait_ratio_raw"],
        ),
        reverse=True,
    )
    oldest = sorted(
        (z for z in state if (z["open_requests"] or 0) > 0 and z["open_request_max_age_s"] is not None),
        key=lambda z: z["open_request_max_age_s"],
        reverse=True,
    )
    demand = sorted(
        (z for z in state if (z["expected_requests_raw"] or 0) >= MIN_EXPECTED),
        key=lambda z: (z["requests"] or 0) / z["expected_requests_raw"],
        reverse=True,
    )
    boroughs = sorted({z["borough"] for z in state})
    return {
        "zones_scored": len(scored),
        "zones_above_normal_range": sum(1 for z in scored if z["wait_p50_s"] > (z["normal_p75_raw"] or 1e9)),
        "zones_unusual": sum(1 for z in scored if (z["wait_ratio_pctile"] or 0) >= 95),
        "left_out": {s: sum(1 for z in state if z["state"] == s) for s in STATES},
        "top": [
            {
                k: z[k]
                for k in (
                    "zone_id",
                    "zone_name",
                    "wait_p50_s",
                    "wait_ratio",
                    "wait_ratio_pctile",
                    "wait_n",
                )
            }
            for z in ranked[:10]
        ],
        "oldest_open_request": [
            {
                k: z[k]
                for k in (
                    "zone_id",
                    "zone_name",
                    "state",
                    "open_requests",
                    "open_request_max_age_s",
                )
            }
            for z in oldest[:10]
        ],
        "requests_vs_expected": [
            {
                k: z[k]
                for k in (
                    "zone_id",
                    "zone_name",
                    "state",
                    "requests",
                    "expected_requests_30m",
                )
            }
            | {"ratio": round((z["requests"] or 0) / z["expected_requests_raw"], 3)}
            for z in demand[:10]
        ],
        "city": area(state),
        "boroughs": {b: area([z for z in state if z["borough"] == b]) for b in boroughs},
    }


def gazetteer() -> dict[str, list[str]]:
    return yaml.safe_load((ROOT / "serving" / "app" / "gazetteer.yaml").read_text())["places"]


def main() -> None:
    q = yaml.safe_load((HERE / "questions.yaml").read_text())
    places = gazetteer()
    c = connect()
    key: dict = {
        "built_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "moments": {},
    }

    for name, m in q["moments"].items():
        moment = utc(m["frozen_now"])
        files = paths(c, moment)
        by_provider = {p: zone_state(c, files, moment, p) for p in ("uber", "lyft")}
        uber = {z["zone_id"]: z for z in by_provider["uber"]}
        by_name = {z["zone_name"].lower(): z["zone_id"] for z in by_provider["uber"]}
        key["moments"][name] = {
            "frozen_now": m["frozen_now"],
            "local": m["local"],
            "summary": {p: summary(s) for p, s in by_provider.items()},
            "zones": by_provider,
            "places": {
                alias: [by_name[n.lower()] for n in names if n.lower() in by_name]
                for alias, names in places.items()
            },
        }
        print(
            f"{name}: uber {key['moments'][name]['summary']['uber']['zones_scored']} scored, "
            f"{key['moments'][name]['summary']['uber']['zones_unusual']} unusual"
        )

        # Histories the "why / since when / premise" questions need.
        wanted = {
            "midtown": [by_name[n.lower()] for n in places["midtown"]],
            "lower_manhattan": [by_name[n.lower()] for n in places["lower manhattan"]],
            "airports": [132, 138],
            "east_village": [by_name["east village"]],
            "top": [z["zone_id"] for z in key["moments"][name]["summary"]["uber"]["top"][:3]],
        }
        key["moments"][name]["history_uber"] = {
            label: history(c, files, moment, "uber", ids, hours=6) for label, ids in wanted.items()
        }
        assert all(i in uber for ids in wanted.values() for i in ids)

    out = HERE / "key" / "answer_key.json"
    out.write_text(json.dumps(key, indent=1, default=str))
    print(f"wrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
