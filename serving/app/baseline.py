"""The "normal" for a zone, provider and local hour, computed exactly like
transform/models/features/feat_zone_hour_baseline.sql: same zone, provider,
weekday and NYC hour in the previous 6 weeks; holidays left out; hours with
too few trips do not shape the wait baseline; weak below 3 usable weeks.
Only weeks before the asked moment are used, so the baseline is what was
knowable then (no future leakage).

The batch hourly table (feat_zone_hour) ends where the batch build ends
(2026-07); weeks after that are simply missing, and the week count says so.
"""

import threading
from datetime import date, datetime, timedelta

from . import config, sources

_loaded: set[str] = set()
_lock = threading.Lock()


def _months_for(day: date) -> list[str]:
    first = day.replace(day=1)
    months = []
    for back in range(0, 3):  # 6 weeks back touches at most two earlier months
        y, m = first.year, first.month - back
        while m < 1:
            y, m = y - 1, m + 12
        months.append(f"{y:04d}-{m:02d}")
    return months


def _ensure_loaded(day: date) -> None:
    """Loads the needed months of feat_zone_hour into DuckDB memory once
    (a few MB per month); later questions are answered from memory."""
    with _lock:
        c = sources.duck()
        if not _loaded:
            c.sql(
                "CREATE TABLE zone_hour (date_local DATE, hour_of_day_local INTEGER, zone_id INTEGER, "
                "provider VARCHAR, requests BIGINT, dropoffs_in BIGINT, wait_p50_s DOUBLE, is_low_sample BOOLEAN)"
            )
            c.sql(
                f"CREATE TABLE holidays AS SELECT holiday_date FROM "
                f"read_parquet('{config.STAGING_ROOT}/holidays/data.parquet')"
            )
            _loaded.add("init")
        for m in _months_for(day):
            if m in _loaded:
                continue
            path = f"{config.FEATURES_ROOT}/zone_hour/trip_month={m}/data.parquet"
            try:
                c.sql(
                    f"INSERT INTO zone_hour SELECT date_local, hour_of_day_local, zone_id, provider, requests, "
                    f"dropoffs_in, wait_p50_s, is_low_sample FROM read_parquet('{path}')"
                )
            except Exception:
                pass  # month not built by batch (yet): its weeks are simply missing
            _loaded.add(m)


def for_hours(local_hour: datetime, zone_ids: list[int], provider: str) -> dict[int, dict]:
    """Baseline for the given local hour (NYC wall clock) for several zones."""
    day, hod = local_hour.date(), local_hour.hour
    _ensure_loaded(day)
    weeks = [day - timedelta(days=7 * k) for k in range(1, config.BASELINE_WEEKS + 1)]
    ids = ",".join(str(int(z)) for z in zone_ids) or "NULL"
    dates = ",".join(f"DATE '{d.isoformat()}'" for d in weeks)
    rows = (
        sources.duck()
        .sql(f"""
        SELECT zone_id,
               count(DISTINCT date_local) AS weeks_n,
               count(CASE WHEN NOT is_low_sample THEN wait_p50_s END) AS wait_weeks_n,
               quantile_cont(CASE WHEN NOT is_low_sample THEN wait_p50_s END, 0.25) AS p25,
               quantile_cont(CASE WHEN NOT is_low_sample THEN wait_p50_s END, 0.50) AS p50,
               quantile_cont(CASE WHEN NOT is_low_sample THEN wait_p50_s END, 0.75) AS p75,
               median(requests) AS requests_per_hour,
               median(dropoffs_in) AS dropoffs_in_per_hour
        FROM zone_hour
        WHERE zone_id IN ({ids}) AND provider = '{provider}' AND hour_of_day_local = {hod}
          AND date_local IN ({dates})
          AND date_local NOT IN (SELECT holiday_date FROM holidays)
        GROUP BY zone_id""")
        .fetchall()
    )
    out = {}
    for z, wn, wwn, p25, p50, p75, req, drop in rows:
        out[z] = {
            "wait_p25_s": _r(p25),
            "wait_p50_s": _r(p50),
            "wait_p75_s": _r(p75),
            "requests_per_hour": _r(req),
            "dropoffs_in_per_hour": _r(drop),
            "weeks_n": wn,
            "wait_weeks_n": wwn,
            "is_weak": wwn < config.BASELINE_MIN_WEEKS,
            "rule": f"same zone, provider, weekday and NYC hour ({hod}:00) in the previous "
            f"{config.BASELINE_WEEKS} weeks; holidays and low-sample hours left out",
        }
    for z in zone_ids:
        out.setdefault(
            z,
            {
                "weeks_n": 0,
                "wait_weeks_n": 0,
                "is_weak": True,
                "rule": "no earlier weeks found for this hour",
            },
        )
    return out


def _r(x):
    return None if x is None else round(float(x), 1)
