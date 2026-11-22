"""One zone: its live state now and its minute-by-minute history."""

from datetime import timedelta
from typing import Any

from .. import baseline, config, live, scores, sources
from ..audit import audited
from .common import (
    NY,
    VERSION,
    Provider,
    Window,
    check,
    clock,
    freshness,
    iso,
    likely_driver,
    local,
    num,
    provider_caveats,
    utc,
    window_minutes,
    zone,
)


@audited
def get_zone_now(zone_id: int, provider: Provider = "uber", window: Window = "30m") -> dict[str, Any]:
    """Live state of one pickup zone at the current moment: median and 90th
    percentile wait (request -> driver arrival) over the last 15 or 30
    minutes, requests, open requests and the oldest open request, next to the
    normal for this weekday and hour. provider defaults to uber; use 30m
    unless the question is about the last few minutes."""
    check(provider, window)
    z = zone(zone_id)
    c = clock()
    w, o = live.read([zone_id], provider, window)[zone_id]
    minutes = window_minutes(window)
    caveats = provider_caveats(provider)

    if not w:
        hb_ok = c["features_lag_s"] is not None and c["features_lag_s"] <= config.LIVE_SLA_S
        return {
            "zone": z,
            "provider": provider,
            "window": window,
            "status": "no_activity" if hb_ok else "stale",
            "freshness": freshness(c["features_as_of"], c["now"], config.LIVE_SLA_S),
            "note": (
                "No trips in this zone in the last 30 minutes."
                if hb_ok
                else "The live pipeline is behind; no current value."
            ),
        }

    as_of = utc(w["window_end_utc"])
    fresh = freshness(as_of, c["now"], config.LIVE_SLA_S)
    mid = as_of - timedelta(minutes=minutes / 2)
    b = baseline.for_hours(
        mid.astimezone(NY).replace(tzinfo=None, minute=0, second=0, microsecond=0),
        [zone_id],
        provider,
    )[zone_id]
    wait_n = int(w["wait_n"])
    p50 = num(w.get("wait_p50_s"))
    expected_requests = (
        None if b.get("requests_per_hour") is None else round(b["requests_per_hour"] * minutes / 60, 1)
    )

    if fresh["status"] != "fresh":
        metrics = None
    else:
        metrics = {
            "wait_p50_s": {"value": p50, "unit": "seconds"},
            "wait_p90_s": {"value": num(w.get("wait_p90_s")), "unit": "seconds"},
            "requests": {
                "value": int(w["requests"]),
                "unit": "count",
                "expected_from_baseline": expected_requests,
            },
            "open_requests": {"value": int(o.get("open_requests", 0)), "unit": "count"},
            "open_request_max_age_s": {
                "value": num(o.get("open_request_max_age_s")),
                "unit": "seconds",
            },
        }

    comparison = {}
    if metrics and p50 is not None and b.get("wait_p50_s"):
        ratio = round(p50 / b["wait_p50_s"], 2)
        comparison = {
            "wait_ratio": ratio,
            "above_normal_range": b.get("wait_p75_s") is not None and p50 > b["wait_p75_s"],
            "below_normal_range": b.get("wait_p25_s") is not None and p50 < b["wait_p25_s"],
            # How unusual for THIS zone: rank against its own last 8 weeks.
            **scores.pctiles({zone_id: ratio}, provider, c["now"])[zone_id],
            "likely_driver": likely_driver(int(w["requests"]), expected_requests),
        }
    if wait_n < config.LOW_SAMPLE_WAITS:
        caveats.append(f"Only {wait_n} waits in this window: too few to rank or alert on.")
    if b["is_weak"]:
        caveats.append(f"Weak baseline: only {b['wait_weeks_n']} usable earlier weeks (need 3).")
    caveats.append(
        f"Baseline is per hour ({mid.astimezone(NY):%H}:00 local); the live value covers {window}."
    )
    caveats.append(
        "wait_ratio_pctile ranks this live window against the zone's hourly ratios of the last 8 "
        "weeks; short windows swing more than hours, so read 95+ as unusual, not as certain."
    )
    caveats.append("Only completed trips are in the data; unfulfilled requests are invisible.")

    return {
        "zone": z,
        "provider": provider,
        "window": window,
        "status": "ok" if metrics else "stale",
        "as_of_utc": iso(as_of),
        "as_of_local": local(as_of),
        "freshness": fresh,
        "metrics": metrics,
        "sample_size": {"wait_n": wait_n},
        "baseline": b,
        "comparison": comparison,
        "caveats": caveats,
        "definition_version": VERSION,
        "source": (
            f"{live.source()}: zone {zone_id}, {provider}, {window} window and open requests; "
            f"baseline from batch table feat_zone_hour"
        ),
    }


@audited
def get_zone_history(
    zone_id: int,
    provider: Provider = "uber",
    window: Window = "30m",
    minutes: int = 120,
) -> dict[str, Any]:
    """Minute-by-minute live values of one zone over the last `minutes`
    (max 360), up to the current moment: how the wait, requests and open
    requests developed. Use it for "why did it rise" and "since when"."""
    check(provider, window)
    z = zone(zone_id)
    minutes = max(1, min(int(minutes), config.MAX_HISTORY_MINUTES))
    c = clock()
    now_ = c["now"]
    since = now_ - timedelta(minutes=minutes)
    meta = sources.iceberg_metadata(*config.HISTORY_TABLE)
    s = window  # validated above, safe to use in a column name
    rows = (
        sources.duck()
        .sql(f"""
        SELECT as_of_utc, requests_{s}, wait_n_{s}, wait_p50_s_{s}, wait_p90_s_{s},
               open_requests, open_request_max_age_s
        FROM iceberg_scan('{meta}')
        WHERE zone_id = {int(zone_id)} AND provider = '{provider}'
          AND as_of_utc > TIMESTAMPTZ '{iso(since)}'
          AND as_of_utc <= TIMESTAMPTZ '{iso(now_)}'
        ORDER BY as_of_utc""")
        .fetchall()
    )
    # Each point is a rolling window ending at that minute, so the counts
    # carry the window in their names: an agent read a bare "requests" as
    # requests per minute.
    points = [
        {
            "as_of_local": local(r[0]),
            f"requests_last_{s}": r[1],
            f"wait_n_last_{s}": r[2],
            "wait_p50_s": None if r[3] is None else round(r[3], 1),
            "wait_p90_s": None if r[4] is None else round(r[4], 1),
            "open_requests": r[5],
            "open_request_max_age_s": r[6],
        }
        for r in rows
    ]
    last = rows[-1][0] if rows else None
    return {
        "zone": z,
        "provider": provider,
        "window": window,
        "from_local": local(since),
        "to_local": local(now_),
        "points_n": len(points),
        "values_are": f"rolling {window} windows ending at each minute (not per-minute counts)",
        "points": points,
        "freshness": freshness(last, now_, int(config.HISTORY_COMMIT_WALL_S * max(c["speed"], 1))),
        "caveats": provider_caveats(provider)
        + [
            "History is committed every 5 minutes of real time (at replay speed 10 that is 50 "
            "replay minutes), so its most recent part may be missing; get_zone_now has the latest minute.",
            "A minute with no row had no activity in the zone's last 30 minutes.",
        ],
        "definition_version": VERSION,
        "source": "Iceberg table features.feat_zone_minute_live (same records as Redis)",
    }
