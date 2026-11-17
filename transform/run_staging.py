"""Builds the staging layer partition by partition with dbt.

Every partition is a separate `dbt build` process (model + its tests), so
memory never accumulates across months and a failed month can simply be
rerun. Rebuilding a partition overwrites its file, so reruns are safe.

Order:
  1. stg_zones          (one file)
  2. stg_trip_events    per event month   (reads raw.trip_events, one Iceberg snapshot)
  3. stg_trips          per trip month    (reads staged events of month-1..month+1)
  4. stg_yellow_trips   per file month    (reads source/tlc/yellow)

Usage:
  python run_staging.py --target prod --months all
  python run_staging.py --target dev --months 2026-06 --slice 2026-06-10,2026-06-12
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import duckdb

HERE = Path(__file__).resolve().parent
BUCKET = "nyc-taxi-raw-969788248726"
REGION = "us-east-1"
DBT = str(Path(sys.executable).with_name("dbt"))


def month_add(month, n):
    y, m = map(int, month.split("-"))
    m += n
    y, m = y + (m - 1) // 12, (m - 1) % 12 + 1
    return f"{y}-{m:02d}"


def raw_metadata():
    glue = boto3.client("glue", region_name=REGION)
    return glue.get_table(DatabaseName="raw", Name="trip_events")["Table"]["Parameters"]["metadata_location"]


def raw_event_months(metadata):
    """Months that hold events, read from Iceberg's file list (no data scan)."""
    rows = duckdb_con().execute(f"""
        SELECT DISTINCT regexp_extract(file_path, 'event_day=(\\d{{4}}-\\d{{2}})', 1) AS m
        FROM iceberg_metadata('{metadata}') WHERE status <> 'DELETED' ORDER BY m
    """).fetchall()
    return [r[0] for r in rows]


def yellow_months():
    s3 = boto3.client("s3", region_name=REGION)
    months = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="source/tlc/yellow/"):
        for o in page.get("Contents", []):
            parts = dict(p.split("=") for p in o["Key"].split("/") if "=" in p)
            months.add(f"{parts['year']}-{parts['month']}")
    return sorted(months)


def duckdb_con():
    con = duckdb.connect()
    # INSTALL is a no-op when the extension is already there (fresh machines have none)
    con.execute("INSTALL httpfs; INSTALL iceberg; INSTALL icu; LOAD httpfs; LOAD iceberg; LOAD icu;")
    con.execute(f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION '{REGION}');")
    con.execute("SET TimeZone = 'UTC'; SET enable_progress_bar = false;")
    return con


def exists(path):
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        try:
            boto3.client("s3", region_name=REGION).head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False
    return Path(path).exists()


def row_count(path):
    return duckdb_con().execute(f"SELECT sum(num_rows) FROM parquet_file_metadata('{path}')").fetchone()[0]


def build(model, target, dbt_vars, location):
    if not location.startswith("s3://"):
        Path(location).parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    cmd = [DBT, "build", "--select", model, "--target", target, "--vars", json.dumps(dbt_vars),
           "--project-dir", str(HERE), "--profiles-dir", str(HERE), "--quiet", "--no-use-colors"]
    p = subprocess.run(cmd, capture_output=True, text=True)
    r = {"model": model, "partition": dbt_vars.get("month"), "seconds": round(time.time() - t0, 1)}
    if p.returncode != 0:
        r.update(status="failed", error=(p.stdout + p.stderr)[-3000:])
    else:
        r.update(status="ok", rows=row_count(location))
    print(json.dumps(r), flush=True)
    return r


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--target", choices=["dev", "prod"], required=True)
    a.add_argument("--months", required=True, help="YYYY-MM[,YYYY-MM...] or all")
    a.add_argument("--models", default="zones,events,trips,yellow")
    a.add_argument("--slice", help="dev only: event_time window FROM,TO (dates)")
    a.add_argument("--skip-existing", action="store_true", help="resume: skip partitions already written")
    args = a.parse_args()

    root = (f"s3://{BUCKET}/lake/staging" if args.target == "prod"
            else str(HERE.parent / ".lake" / "staging"))
    os.environ["STAGING_ROOT"] = root
    models = args.models.split(",")
    meta = raw_metadata()
    print(json.dumps({"staging_root": root, "raw_metadata": meta}), flush=True)

    if args.months == "all":
        event_months = raw_event_months(meta)
        ymonths = yellow_months()
    else:
        event_months = ymonths = args.months.split(",")
    base = {"raw_metadata": meta}
    if args.slice:
        base["slice_from"], base["slice_to"] = args.slice.split(",")

    def loc(table, key, m):
        return f"{root}/{table}/{key}={m}/data.parquet"

    plan = []
    if "zones" in models:
        plan.append(("stg_zones", {}, f"{root}/zones/data.parquet"))
    if "events" in models:
        plan += [("stg_trip_events", {"month": m}, loc("trip_events", "event_month", m)) for m in event_months]
    if "trips" in models:
        plan += [("stg_trips", {"month": m}, loc("trips", "trip_month", m)) for m in event_months]
    if "holidays" in models:
        plan.append(("stg_holidays", {}, f"{root}/holidays/data.parquet"))
    if "zone_hour_baseline" in models:
        froot = root.replace("/staging", "/features")
        plan += [("feat_zone_hour_baseline", {"month": m}, f"{froot}/zone_hour_baseline/trip_month={m}/data.parquet")
                 for m in event_months]
    if "zone_hour_vs_baseline" in models:
        froot = root.replace("/staging", "/features")
        plan += [("feat_zone_hour_vs_baseline", {"month": m}, f"{froot}/zone_hour_vs_baseline/trip_month={m}/data.parquet")
                 for m in event_months]
    if "zone_hour" in models:
        froot = root.replace("/staging", "/features")
        plan += [("feat_zone_hour", {"month": m}, f"{froot}/zone_hour/trip_month={m}/data.parquet")
                 for m in event_months]
    if "yellow" in models:
        plan += [("stg_yellow_trips", {"month": m}, loc("yellow_trips", "file_month", m)) for m in ymonths]

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results = []
    try:
        for model, v, location in plan:
            if args.skip_existing and exists(location):
                continue
            if model == "stg_trips":
                # a trip's events sit in its request month and the months next to it;
                # checked now, after the event partitions have been written
                m = v["month"]
                v = {**v, "event_months": [w for w in (month_add(m, -1), m, month_add(m, 1))
                                           if exists(loc("trip_events", "event_month", w))]}
            if model == "feat_zone_hour":
                # dropoffs of a month also come from trips requested the month before
                m = v["month"]
                v = {**v, "trip_months": [w for w in (month_add(m, -1), m)
                                          if exists(loc("trips", "trip_month", w))]}
            if model == "feat_zone_hour_baseline":
                # 6 weeks back from the 1st of a month reaches two months earlier
                m = v["month"]
                froot = root.replace("/staging", "/features")
                v = {**v, "feature_months": [w for w in (month_add(m, -2), month_add(m, -1), m)
                     if exists(f"{froot}/zone_hour/trip_month={w}/data.parquet")]}
            if model == "feat_zone_hour_vs_baseline":
                # 8 weeks back from the 1st of a month reaches two months earlier;
                # a month counts only when both the actual and the baseline exist
                m = v["month"]
                froot = root.replace("/staging", "/features")
                v = {**v, "feature_months": [w for w in (month_add(m, -2), month_add(m, -1), m)
                     if exists(f"{froot}/zone_hour/trip_month={w}/data.parquet")
                     and exists(f"{froot}/zone_hour_baseline/trip_month={w}/data.parquet")]}
            r = build(model, args.target, {**base, **v}, location)
            results.append(r)
            if r["status"] != "ok":
                raise SystemExit(f"{model} {v.get('month')} failed; stopping")
    finally:
        manifest = {"run_id": run_id, "target": args.target, "raw_metadata": meta, "results": results}
        if args.target == "prod":
            boto3.client("s3", region_name=REGION).put_object(
                Bucket=BUCKET, Key=f"ops/manifest/staging_{run_id}.json",
                Body=json.dumps(manifest, indent=1).encode())
            print(f"manifest: s3://{BUCKET}/ops/manifest/staging_{run_id}.json")


if __name__ == "__main__":
    main()
