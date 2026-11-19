"""How unusual a live wait is for its own zone: the live wait_ratio ranked
against the zone and provider's hourly wait_ratio over the previous 8 weeks.
Same definition as wait_ratio_pctile in the batch table
feat_zone_hour_vs_baseline: share of earlier hours with a lower ratio,
0-100, none below 200 earlier hours. A fixed threshold would not work:
zones differ in how much they swing (a zone's own 95th percentile of
wait_ratio ranges from 1.27 to 2.03).

The batch hourly ratios are loaded into DuckDB memory once per month. Only
hours before the asked moment are used.
"""

import threading
from datetime import datetime, timedelta

from . import config, sources

HISTORY_WEEKS = 8
MIN_HISTORY_HOURS = 200
UNUSUAL_PCTILE = 95

_loaded: set[str] = set()
_lock = threading.Lock()


def _months_between(start: datetime, end: datetime) -> list[str]:
    months, y, m = [], start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def _ensure_loaded(now_utc: datetime) -> None:
    with _lock:
        c = sources.duck()
        if not _loaded:
            c.sql(
                "CREATE TABLE zone_ratio (hour_utc TIMESTAMPTZ, zone_id INTEGER, provider VARCHAR, wait_ratio DOUBLE)"
            )
            _loaded.add("init")
        for m in _months_between(now_utc - timedelta(weeks=HISTORY_WEEKS), now_utc):
            if m in _loaded:
                continue
            path = f"{config.FEATURES_ROOT}/zone_hour_vs_baseline/trip_month={m}/data.parquet"
            try:
                c.sql(
                    f"INSERT INTO zone_ratio SELECT hour_utc, zone_id, provider, wait_ratio "
                    f"FROM read_parquet('{path}') WHERE wait_ratio IS NOT NULL"
                )
            except Exception:
                pass  # month not built by batch (yet)
            _loaded.add(m)


def pctiles(ratios: dict[int, float], provider: str, now_utc: datetime) -> dict[int, dict]:
    """{zone_id: live wait_ratio} -> {zone_id: {wait_ratio_pctile, history_hours_n, is_unusual}}."""
    if not ratios:
        return {}
    _ensure_loaded(now_utc)
    c = sources.duck()
    c.sql("CREATE TEMP TABLE live (zone_id INTEGER, ratio DOUBLE)")
    c.executemany("INSERT INTO live VALUES (?, ?)", list(ratios.items()))
    since = (now_utc - timedelta(weeks=HISTORY_WEEKS)).isoformat()
    rows = c.execute(
        """
        SELECT l.zone_id,
               count(h.wait_ratio) AS n,
               100.0 * count(*) FILTER (WHERE h.wait_ratio < l.ratio) / nullif(count(h.wait_ratio), 0) AS pct
        FROM live l
        LEFT JOIN zone_ratio h
          ON h.zone_id = l.zone_id AND h.provider = ?
         AND h.hour_utc >= ?::TIMESTAMPTZ AND h.hour_utc < ?::TIMESTAMPTZ
        GROUP BY l.zone_id""",
        [provider, since, now_utc.isoformat()],
    ).fetchall()
    c.close()
    out = {}
    for zone_id, n, pct in rows:
        ok = n >= MIN_HISTORY_HOURS and pct is not None
        shown = round(pct, 1) if ok else None
        out[zone_id] = {
            "wait_ratio_pctile": shown,
            "history_hours_n": n,
            # Decided on the shown value, so "95.0" and is_unusual never disagree.
            "is_unusual": bool(ok and shown >= UNUSUAL_PCTILE),
        }
    return out
