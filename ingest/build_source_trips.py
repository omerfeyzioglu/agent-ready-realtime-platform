"""Builds the simulated source system: TLC trip files + trip_id.

Reads landing/tlc/{fhvhv,yellow}/year=YYYY/month=MM/*.parquet and writes the
same rows, same columns and same types to source/tlc/... with one extra
column, trip_id (UUID v7). The timestamp part of the UUID is the time the
trip was requested (FHVHV request_datetime, Yellow pickup time; NYC local
converted to UTC), like a ride app that creates the ID on request.

Written once and frozen: a month whose output already exists is skipped.
Every written month is verified (row count, unique non-null trip_id, schema
and content unchanged). The script stops at the first failed check.

Usage: python build_source_trips.py --dataset fhvhv|yellow|all --months 2026-08[,2026-07]|all
"""

import argparse
import json
import time
from datetime import datetime, timezone

import duckdb

BUCKET = "s3://nyc-taxi-raw-969788248726"
ID_TIME = {"fhvhv": "request_datetime", "yellow": "tpep_pickup_datetime"}
MONTHS = [f"{y}-{m:02d}" for y in range(2023, 2027) for m in range(1, 13) if f"{y}-{m:02d}" <= "2026-08"]


def paths(dataset, month):
    y, m = month.split("-")
    rel = f"tlc/{dataset}/year={y}/month={m}/{dataset}_tripdata_{month}.parquet"
    return f"{BUCKET}/landing/{rel}", f"{BUCKET}/source/{rel}"


def exists(con, path):
    # glob() echoes a literal path back without checking it, so list the folder instead
    folder = path.rsplit("/", 1)[0]
    return path in {r[0] for r in con.execute("SELECT file FROM glob(?)", [f"{folder}/*"]).fetchall()}


def checksum(con, path, exclude_id):
    cols = "* EXCLUDE (trip_id)" if exclude_id else "*"
    return con.execute(f"""
        SELECT count(*), sum(hash(*COLUMNS({cols})))
        FROM read_parquet('{path}', hive_partitioning = false)
    """).fetchone()


def schema(con, path, exclude_id):
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}', hive_partitioning = false)").fetchall()
    return [(r[0], r[1]) for r in rows if not (exclude_id and r[0] == "trip_id")]


def build(con, dataset, month):
    src, dst = paths(dataset, month)
    if exists(con, dst):
        return {"dataset": dataset, "month": month, "status": "skipped_exists"}

    t = time.time()
    # UUID v7 = 48-bit unix ms timestamp | version 7 | random bits.
    # Built from a random v4 by replacing its first 48 bits and the version digit.
    con.execute(f"""
        COPY (
            WITH r AS (
                SELECT *,
                       printf('%012x', epoch_ms(timezone('America/New_York', {ID_TIME[dataset]}))) AS _ts,
                       gen_random_uuid()::VARCHAR AS _u
                FROM read_parquet('{src}', hive_partitioning = false)
            )
            SELECT coalesce(
                       (substr(_ts, 1, 8) || '-' || substr(_ts, 9, 4) || '-7' || substr(_u, 16))::UUID,
                       uuidv7()
                   ) AS trip_id,
                   * EXCLUDE (_ts, _u)
            FROM r
        ) TO '{dst}' (FORMAT parquet, COMPRESSION zstd)
    """)
    write_s = time.time() - t

    src_rows, src_sum = checksum(con, src, exclude_id=False)
    dst_rows, dst_sum = checksum(con, dst, exclude_id=True)
    ids, distinct_ids, null_ids, bad_version = con.execute(f"""
        SELECT count(*), count(DISTINCT trip_id), count(*) - count(trip_id),
               count(*) FILTER (WHERE substr(trip_id::VARCHAR, 15, 1) <> '7')
        FROM read_parquet('{dst}', hive_partitioning = false)
    """).fetchone()

    checks = {
        "row_count_equal": src_rows == dst_rows,
        "content_equal": src_sum == dst_sum,
        "schema_equal": schema(con, src, False) == schema(con, dst, True),
        "trip_id_not_null": null_ids == 0,
        "trip_id_unique": distinct_ids == ids,
        "trip_id_v7": bad_version == 0,
    }
    return {
        "dataset": dataset, "month": month, "source": src, "output": dst,
        "rows": dst_rows, "write_s": round(write_s, 1), "total_s": round(time.time() - t, 1),
        "checks": checks, "status": "ok" if all(checks.values()) else "failed",
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", choices=["fhvhv", "yellow", "all"], required=True)
    p.add_argument("--months", required=True)
    a = p.parse_args()
    datasets = ["fhvhv", "yellow"] if a.dataset == "all" else [a.dataset]
    months = MONTHS if a.months == "all" else a.months.split(",")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL icu; LOAD icu;")
    con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');")
    con.execute("SET enable_progress_bar = false; SET temp_directory = '/var/tmp/duck';")

    load_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results = []
    try:
        for d in datasets:
            for m in months:
                r = build(con, d, m)
                results.append(r)
                print(json.dumps(r), flush=True)
                if r["status"] == "failed":
                    raise SystemExit(f"check failed for {d} {m}; stopping, output left in place for inspection")
    finally:
        manifest = f"{BUCKET}/ops/manifest/source_trips_{load_id}.json"
        con.execute("CREATE TEMP TABLE m AS SELECT unnest(?::JSON[]) AS r", [[json.dumps(r) for r in results]])
        con.execute(f"COPY (SELECT r FROM m) TO '{manifest}' (FORMAT json)")
        print(f"manifest: {manifest}")


if __name__ == "__main__":
    main()
