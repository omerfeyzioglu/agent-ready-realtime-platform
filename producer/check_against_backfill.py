"""Checks the Go producer against the history backfill on the same trips.

Both turn source trips into events, so for one input file they must agree:
same events, same event times, same payloads. The producer must also emit in
event time order with unique, well-formed event ids.

Usage: python producer/check_against_backfill.py <source.parquet> <producer_output.jsonl>
"""

import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from backfill_events import EVENTS_SQL, TRIPS_UTC_SQL, amb_cols  # noqa: E402


def main(src, produced):
    con = duckdb.connect()
    con.execute("INSTALL icu; LOAD icu; INSTALL json; LOAD json; SET TimeZone = 'UTC';")

    amb = ",".join(amb_cols(n, c) for n, c in (
        ("req", "request_datetime"), ("scene", "on_scene_datetime"),
        ("pick", "pickup_datetime"), ("drop", "dropoff_datetime")))
    con.execute(TRIPS_UTC_SQL.format(src=src, amb_cols=amb))
    con.execute("CREATE TABLE batch AS " + EVENTS_SQL.format(
        cbd="cbd_congestion_fee", ingest_time="2000-01-01T00:00:00+00:00", where="true"))
    con.execute(f"""
        CREATE TABLE live AS
        SELECT row_number() OVER () AS line, *
        FROM read_json('{produced}', format = 'newline_delimited', columns = {{
            event_id: 'VARCHAR', trip_id: 'VARCHAR', event_type: 'VARCHAR',
            event_time: 'TIMESTAMPTZ', payload: 'JSON'}})
    """)

    def one(sql):
        return con.execute(sql).fetchone()[0]

    key = "USING (trip_id, event_type, event_time)"
    checks = {
        "batch_events": one("SELECT count(*) FROM batch"),
        "live_events": one("SELECT count(*) FROM live"),
        "only_in_batch": one(f"SELECT count(*) FROM batch ANTI JOIN live {key}"),
        "only_in_live": one(f"SELECT count(*) FROM live ANTI JOIN batch {key}"),
        "out_of_order_lines": one(
            "SELECT count(*) FROM (SELECT event_time < lag(event_time) OVER (ORDER BY line) AS back FROM live) WHERE back"),
        "duplicate_event_ids": one("SELECT count(*) - count(DISTINCT event_id) FROM live"),
        "event_id_time_mismatch": one("""
            SELECT count(*) FROM live
            WHERE replace(substr(event_id, 1, 13), '-', '') <> printf('%012x', epoch_ms(event_time))
               OR substr(event_id, 15, 1) <> '7'"""),
    }

    # Payloads are compared as parsed JSON: 12.0 and 12 are the same number.
    different = 0
    examples = []
    rows = con.execute(f"""
        SELECT event_type, b.payload, l.payload::VARCHAR
        FROM batch b JOIN live l {key}
        WHERE b.payload <> l.payload::VARCHAR
    """)
    while chunk := rows.fetchmany(100_000):
        for event_type, b, l in chunk:
            if json.loads(b) != json.loads(l):
                different += 1
                if len(examples) < 3:
                    examples.append({"event_type": event_type, "batch": b, "live": l})
    checks["payload_mismatch"] = different

    by_type = con.execute("SELECT event_type, count(*) FROM live GROUP BY 1 ORDER BY 1").fetchall()
    ok = (checks["batch_events"] == checks["live_events"]
          and all(v == 0 for k, v in checks.items() if k not in ("batch_events", "live_events")))
    print(json.dumps({"status": "ok" if ok else "failed", "checks": checks,
                      "live_by_type": dict(by_type), "payload_examples": examples}, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
