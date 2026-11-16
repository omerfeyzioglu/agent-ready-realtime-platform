"""Backfills FHVHV trip history as events into the Iceberg table raw.trip_events.

Each trip in source/tlc/fhvhv becomes up to five events, shaped like the ones
the live producer sends through Kafka:

    requested    request_datetime   provider, originating base, pickup zone, request flags
    arrived      on_scene_datetime  dispatching base, WAV match flag
    picked_up    pickup_datetime    (time only)
    dropped_off  dropoff_datetime   dropoff zone, distance, duration, fares, fees, driver pay
    tip_added    dropoff + 1-60 min tips (delay seeded by trip_id; skipped when tips = 0)

A missing timestamp means a missing event (e.g. Lyft before 2025-04 has no
on_scene, so no arrived event). Times are converted from NYC local to UTC
the way an app would send them. On the fall-back night (01:00-02:00 happens
twice) each ambiguous time is resolved from the trip itself:
  1. an unambiguous dropoff minus trip_time fixes the pickup (and vice versa);
  2. request and on_scene take the occurrence closest to the next event
     that still keeps request <= on_scene <= pickup;
  3. whenever a choice was not proven by trip_time or the order alone, the
     payload gets "_dst_assumed": true.

One month is written as one Iceberg transaction, tagged with its source
month in the snapshot summary. Months already in the table are skipped.

Usage: python backfill_events.py --months 2026-06[,2026-05]|all
"""

import argparse
import json
import time
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestamptzType

BUCKET = "s3://nyc-taxi-raw-969788248726"
TABLE = "raw.trip_events"
TABLE_LOCATION = f"{BUCKET}/lake/raw/trip_events"
BACKFILL_MONTHS = [f"{y}-{m:02d}" for y in range(2023, 2027) for m in range(1, 13) if f"{y}-{m:02d}" <= "2026-07"]
FALL_BACK_DATES = ["2023-11-05", "2024-11-03", "2025-11-02", "2026-11-01"]
BATCH_ROWS = 5_000_000

SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "trip_id", StringType(), required=True),
    NestedField(3, "event_type", StringType(), required=True),
    NestedField(4, "event_time", TimestamptzType(), required=True),
    NestedField(5, "payload", StringType(), required=True),
    NestedField(6, "ingest_time", TimestamptzType(), required=True),
    NestedField(7, "ingest_path", StringType(), required=True),
    NestedField(8, "kafka_partition", IntegerType(), required=False),
    NestedField(9, "kafka_offset", LongType(), required=False),
)
SPEC = PartitionSpec(PartitionField(source_id=4, field_id=1000, transform=DayTransform(), name="event_day"))

ARROW_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("trip_id", pa.string(), nullable=False),
    pa.field("event_type", pa.string(), nullable=False),
    pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("payload", pa.string(), nullable=False),
    pa.field("ingest_time", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("ingest_path", pa.string(), nullable=False),
    pa.field("kafka_partition", pa.int32()),
    pa.field("kafka_offset", pa.int64()),
])

# Local NYC time -> UTC, with the fall-back rule. All *_utc columns are naive UTC.
# c1/c2 are the two UTC candidates of an ambiguous time (EDT first, EST second).
TRIPS_UTC_SQL = """
CREATE OR REPLACE TABLE trips AS
WITH src AS (
    SELECT * FROM read_parquet('{src}', hive_partitioning = false)
),
c AS (
    SELECT *,
        {amb_cols}
    FROM src
),
a AS (
    SELECT *,
        -- the dropoff can anchor the pickup through trip_time
        (NOT drop_amb AND drop_conv IS NOT NULL AND trip_time IS NOT NULL) AS drop_anchors,
        -- both ambiguous: the local gap is one hour shorter than trip_time only if
        -- the pickup was the first occurrence and the dropoff the second
        coalesce(pick_amb AND drop_amb
                 AND abs(epoch(dropoff_datetime - pickup_datetime) + 3600 - trip_time) < 600, false) AS crosses
    FROM c
),
pd AS (  -- rule 1: pickup via trip_time
    SELECT *,
        CASE
            WHEN NOT pick_amb THEN pick_conv
            WHEN drop_anchors THEN CASE WHEN abs(epoch(drop_conv - INTERVAL (trip_time) SECOND - pick_c1))
                                           <= abs(epoch(drop_conv - INTERVAL (trip_time) SECOND - pick_c2))
                                        THEN pick_c1 ELSE pick_c2 END
            ELSE pick_c1  -- crosses, or no anchor (assumed)
        END AS pick_utc,
        (pick_amb AND NOT drop_anchors AND NOT crosses) OR (drop_amb AND trip_time IS NULL) AS pd_assumed
    FROM a
),
pd2 AS (  -- dropoff via the resolved pickup
    SELECT *,
        CASE
            WHEN dropoff_datetime IS NULL THEN NULL
            WHEN NOT drop_amb THEN drop_conv
            WHEN trip_time IS NULL THEN drop_c1
            WHEN abs(epoch(pick_utc + INTERVAL (trip_time) SECOND - drop_c1))
                 <= abs(epoch(pick_utc + INTERVAL (trip_time) SECOND - drop_c2)) THEN drop_c1
            ELSE drop_c2
        END AS drop_utc
    FROM pd
),
os AS (  -- rule 2: on_scene <= pickup
    SELECT *,
        CASE
            WHEN on_scene_datetime IS NULL THEN NULL
            WHEN NOT scene_amb THEN scene_conv
            -- the occurrence closest to the pickup that still comes before it
            WHEN scene_c2 <= pick_utc THEN scene_c2
            ELSE scene_c1
        END AS scene_utc,
        -- assumed if its own order test is inconclusive, or it was resolved against an assumed pickup
        (scene_amb AND (NOT ((scene_c1 <= pick_utc) <> (scene_c2 <= pick_utc)) OR pd_assumed)) AS scene_assumed
    FROM pd2
)
SELECT *,
    CASE
        WHEN NOT req_amb THEN req_conv
        WHEN req_c2 <= coalesce(scene_utc, pick_utc) THEN req_c2
        ELSE req_c1
    END AS req_utc,
    (req_amb AND (NOT ((req_c1 <= coalesce(scene_utc, pick_utc)) <> (req_c2 <= coalesce(scene_utc, pick_utc)))
                  OR CASE WHEN scene_utc IS NOT NULL THEN coalesce(scene_assumed, false) ELSE pd_assumed END)) AS req_assumed
FROM os
"""


def amb_cols(name, col):
    fall_back = ", ".join(f"DATE '{d}'" for d in FALL_BACK_DATES)
    return f"""
        coalesce({col}::DATE IN ({fall_back}) AND hour({col}) = 1, false) AS {name}_amb,
        timezone('America/New_York', {col})::TIMESTAMP AS {name}_conv,
        {col} + INTERVAL 4 HOUR AS {name}_c1,
        {col} + INTERVAL 5 HOUR AS {name}_c2"""


# Tip delay = 1 + (last 4 hex digits of trip_id, which are random) mod 60 minutes:
# stable across runs and trivial to reproduce in the Go producer.
EVENTS_SQL = """
WITH e AS (
    SELECT trip_id::VARCHAR AS trip_id, 'requested' AS event_type, req_utc AS t, req_assumed AS assumed,
           to_json({{provider: hvfhs_license_num, originating_base: originating_base_num,
                     pickup_zone: PULocationID, shared_request: shared_request_flag,
                     wav_request: wav_request_flag, access_a_ride: access_a_ride_flag}}) AS payload
    FROM trips WHERE req_utc IS NOT NULL
    UNION ALL
    SELECT trip_id::VARCHAR, 'arrived', scene_utc, scene_assumed,
           to_json({{dispatching_base: dispatching_base_num, wav_match: wav_match_flag}})
    FROM trips WHERE scene_utc IS NOT NULL
    UNION ALL
    SELECT trip_id::VARCHAR, 'picked_up', pick_utc, pd_assumed, '{{}}'::JSON
    FROM trips WHERE pick_utc IS NOT NULL
    UNION ALL
    SELECT trip_id::VARCHAR, 'dropped_off', drop_utc, pd_assumed,
           to_json({{dropoff_zone: DOLocationID, trip_miles: trip_miles, trip_time: trip_time,
                     base_passenger_fare: base_passenger_fare, tolls: tolls, bcf: bcf,
                     sales_tax: sales_tax, congestion_surcharge: congestion_surcharge,
                     airport_fee: airport_fee, cbd_congestion_fee: {cbd},
                     driver_pay: driver_pay, shared_match: shared_match_flag}})
    FROM trips WHERE drop_utc IS NOT NULL
    UNION ALL
    SELECT trip_id::VARCHAR, 'tip_added',
           drop_utc + INTERVAL (1 + ('0x' || right(trip_id::VARCHAR, 4))::INTEGER % 60) MINUTE,
           false, to_json({{tips: tips}})
    FROM trips WHERE drop_utc IS NOT NULL AND tips > 0
),
ts AS (
    SELECT *, printf('%012x', epoch_ms(t::TIMESTAMPTZ)) AS _ts, gen_random_uuid()::VARCHAR AS _u FROM e
)
SELECT substr(_ts, 1, 8) || '-' || substr(_ts, 9, 4) || '-7' || substr(_u, 16) AS event_id,
       trip_id,
       event_type,
       t::TIMESTAMPTZ AS event_time,
       CASE WHEN assumed THEN json_merge_patch(payload, '{{"_dst_assumed": true}}') ELSE payload END::VARCHAR AS payload,
       TIMESTAMPTZ '{ingest_time}' AS ingest_time,
       'backfill' AS ingest_path,
       NULL::INTEGER AS kafka_partition,
       NULL::BIGINT AS kafka_offset
FROM ts
WHERE {where}
ORDER BY event_time
"""


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL icu; LOAD icu; INSTALL json; LOAD json;")
    con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');")
    con.execute("SET TimeZone = 'UTC'; SET enable_progress_bar = false;")
    # Cap below the default 80% of RAM so the big ORDER BY spills to disk early
    # and pyarrow/pyiceberg buffers still fit next to it.
    con.execute("SET temp_directory = '/var/tmp/duck'; SET memory_limit = '20GB';")
    return con


def open_table():
    catalog = load_catalog("glue", **{"type": "glue", "glue.region": "us-east-1", "s3.region": "us-east-1"})
    if not catalog.table_exists(TABLE):
        return catalog.create_table(TABLE, schema=SCHEMA, partition_spec=SPEC, location=TABLE_LOCATION)
    return catalog.load_table(TABLE)


def loaded_months(table):
    return {s.summary.additional_properties.get("source_month") for s in table.metadata.snapshots if s.summary}


def backfill_month(con, table, month, load_id, ingest_time):
    y, m = month.split("-")
    src = f"{BUCKET}/source/tlc/fhvhv/year={y}/month={m}/fhvhv_tripdata_{month}.parquet"
    t0 = time.time()

    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{src}', hive_partitioning = false)").fetchall()}
    amb = ",".join(amb_cols(n, c) for n, c in (
        ("req", "request_datetime"), ("scene", "on_scene_datetime"),
        ("pick", "pickup_datetime"), ("drop", "dropoff_datetime")))
    con.execute(TRIPS_UTC_SQL.format(src=src, amb_cols=amb))

    expected = con.execute("""
        SELECT count(*) AS trips,
               count(req_utc) + count(scene_utc) + count(pick_utc) + count(drop_utc)
               + count(*) FILTER (WHERE drop_utc IS NOT NULL AND tips > 0) AS events,
               count(*) FILTER (WHERE req_assumed OR scene_assumed OR pd_assumed) AS dst_assumed,
               count(*) FILTER (WHERE NOT (req_utc <= coalesce(scene_utc, pick_utc)
                                           AND coalesce(scene_utc, pick_utc) <= pick_utc
                                           AND pick_utc <= drop_utc)) AS out_of_order
        FROM trips
    """).fetchone()
    trips, expected_events, dst_assumed, out_of_order = expected

    # Sort and write in weekly event_time slices: sorting a whole month at once
    # ran out of memory on the largest month (2025-12, ~93M events). Slices are
    # consecutive, so the output is still in event_time order, and the first
    # and last slices are open-ended so outlier timestamps are never dropped.
    edges = [f"TIMESTAMPTZ '{y}-{m}-01' + INTERVAL {d} DAY" for d in (7, 14, 21, 28)]
    slices = [f"t::TIMESTAMPTZ < {edges[0]}"]
    slices += [f"t::TIMESTAMPTZ >= {lo} AND t::TIMESTAMPTZ < {hi}" for lo, hi in zip(edges, edges[1:])]
    slices += [f"t::TIMESTAMPTZ >= {edges[-1]}"]

    written = 0
    props = {"source_month": month, "load_id": load_id, "ingest_path": "backfill"}
    with table.transaction() as tx:
        for where in slices:
            sql = EVENTS_SQL.format(
                cbd="cbd_congestion_fee" if "cbd_congestion_fee" in cols else "NULL",
                ingest_time=ingest_time.isoformat(), where=where)
            for batch in con.execute(sql).to_arrow_reader(BATCH_ROWS):
                chunk = pa.Table.from_batches([batch]).cast(ARROW_SCHEMA)
                tx.append(chunk, snapshot_properties=props)
                written += chunk.num_rows

    table.refresh()
    added = sum(int(s.summary.additional_properties.get("added-records", 0))
                for s in table.metadata.snapshots
                if s.summary and s.summary.additional_properties.get("source_month") == month
                and s.summary.additional_properties.get("load_id") == load_id)

    checks = {
        "events_written_equal_expected": written == expected_events,
        "iceberg_added_records_equal_written": added == written,
    }
    return {
        "month": month, "source": src, "trips": trips, "events": written,
        "dst_assumed_trips": dst_assumed, "out_of_order_trips_info": out_of_order,
        "seconds": round(time.time() - t0, 1), "checks": checks,
        "status": "ok" if all(checks.values()) else "failed",
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--months", required=True)
    a = p.parse_args()
    months = BACKFILL_MONTHS if a.months == "all" else a.months.split(",")
    bad = [m for m in months if m not in BACKFILL_MONTHS]
    if bad:
        raise SystemExit(f"not backfill months (live replay covers 2026-08 and later): {bad}")

    table = open_table()
    now = datetime.now(timezone.utc)
    load_id = now.strftime("%Y%m%dT%H%M%SZ")

    results = []
    try:
        done = loaded_months(table)
        for month in months:
            if month in done:
                r = {"month": month, "status": "skipped_exists"}
            else:
                # Fresh DuckDB per month: one connection reused for 36 months
                # ran out of memory, so nothing carries over between months.
                con = connect()
                try:
                    r = backfill_month(con, table, month, load_id, now)
                finally:
                    con.close()
            results.append(r)
            print(json.dumps(r), flush=True)
            if r["status"] == "failed":
                raise SystemExit(f"check failed for {month}; stopping")
    finally:
        manifest = f"{BUCKET}/ops/manifest/backfill_events_{load_id}.json"
        con = connect()
        con.execute("CREATE OR REPLACE TEMP TABLE m AS SELECT unnest(?::JSON[]) AS r", [[json.dumps(r) for r in results]])
        con.execute(f"COPY (SELECT r FROM m) TO '{manifest}' (FORMAT json)")
        print(f"manifest: {manifest}")


if __name__ == "__main__":
    main()
