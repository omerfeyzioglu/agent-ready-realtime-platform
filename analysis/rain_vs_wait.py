"""Does rain make riders wait longer, and how much?

For every hour of 2023-02 .. 2026-07 (Uber): the city-wide median of the
zones' wait_ratio (actual median wait / the zone's normal for that weekday
and hour, from feat_zone_hour_vs_baseline), next to Central Park
precipitation for that hour (Open-Meteo, hourly, UTC). Because the ratio is
already against each zone's own normal, rush hour and weekday effects are
taken out; what remains is how unusual the hour was.

    serving/.venv/bin/python analysis/rain_vs_wait.py

One weather point (Central Park) stands for the whole city, so local
showers are missed; read the result as a city-level effect.
"""

import json
import tempfile

import boto3
import duckdb

BUCKET = "nyc-taxi-raw-969788248726"
WEATHER = "landing/weather/open_meteo/central_park/"


def weather_rows() -> list[tuple[str, float]]:
    """(hour UTC, precipitation mm) for every hour in the landing files;
    later files win when months overlap."""
    s3 = boto3.client("s3")
    keys = sorted(
        o["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=WEATHER)
        for o in page.get("Contents", [])
        if o["Key"].endswith(".json")
    )
    hours: dict[str, float] = {}
    for k in keys:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
        assert d["utc_offset_seconds"] == 0, f"{k} is not in UTC"  # the trap we fell into once
        h = d["hourly"]
        for t, p in zip(h["time"], h["precipitation"], strict=True):
            if p is not None:
                hours[t] = p
    return sorted(hours.items())


def main() -> None:
    c = duckdb.connect()
    c.sql("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws; SET TimeZone = 'UTC'")
    c.sql("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')")

    w = weather_rows()
    with tempfile.NamedTemporaryFile("w", suffix=".csv") as f:
        f.write("hour_utc,precip_mm\n" + "".join(f"{t}:00+00,{p}\n" for t, p in w))
        f.flush()
        c.sql(f"CREATE TABLE weather AS SELECT * FROM read_csv('{f.name}', header=true, "
              "columns={'hour_utc': 'TIMESTAMPTZ', 'precip_mm': 'DOUBLE'})")
    print(f"weather hours: {len(w)} ({w[0][0]} .. {w[-1][0]} UTC)")

    # City-wide median ratio per hour: how unusual the hour was across zones.
    c.sql(f"""
        CREATE TABLE city_hour AS
        SELECT hour_utc,
               any_value(hour_of_day_local) AS hour_local,
               any_value(dayofweek(date_local)) AS dow,       -- 0 = Sunday
               median(wait_ratio) AS city_ratio,
               count(*) AS zones_n
        FROM read_parquet('s3://{BUCKET}/lake/features/zone_hour_vs_baseline/*/data.parquet',
                          hive_partitioning = true)
        WHERE provider = 'uber' AND wait_ratio IS NOT NULL
        GROUP BY hour_utc
        HAVING count(*) >= 50""")

    sql = """
        WITH j AS (
            SELECT h.*, w.precip_mm,
                   CASE WHEN w.precip_mm = 0   THEN '0 dry'
                        WHEN w.precip_mm < 0.5 THEN '1 trace (<0.5)'
                        WHEN w.precip_mm < 2   THEN '2 light (0.5-2)'
                        WHEN w.precip_mm < 5   THEN '3 moderate (2-5)'
                        ELSE                        '4 heavy (5+)' END AS rain,
                   CASE WHEN dow BETWEEN 1 AND 5 AND (hour_local BETWEEN 7 AND 9 OR hour_local BETWEEN 16 AND 19)
                        THEN 'weekday rush' ELSE 'other hours' END AS period
            FROM city_hour h JOIN weather w USING (hour_utc)
        )
        SELECT period, rain, count(*) AS hours_n,
               round(median(city_ratio), 2) AS median_city_ratio,
               round(quantile_cont(city_ratio, 0.9), 2) AS p90_city_ratio,
               round(100.0 * avg((city_ratio >= 1.3)::INT), 1) AS pct_hours_ratio_1_3_plus
        FROM j GROUP BY ALL ORDER BY period DESC, rain"""
    print("\ncity median wait_ratio by rain (1.00 = normal for that zone, weekday and hour)")
    print(f"{'period':13} {'rain':18} {'hours':>6} {'median':>7} {'p90':>6} {'% >=1.3':>8}")
    for r in c.sql(sql).fetchall():
        print(f"{r[0]:13} {r[1]:18} {r[2]:6} {r[3]:7} {r[4]:6} {r[5]:8}")

    print("\nfor comparison, Monday 2026-08-03 08:30 (live, 30-min window): city median ratio")
    print("is in evals/key/answer_key.json (mon_morning); hourly batch stops at 2026-07.")


if __name__ == "__main__":
    main()
