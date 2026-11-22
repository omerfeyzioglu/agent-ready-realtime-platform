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
    above_normal_range,
    check,
    classify,
    clock,
    expected_requests,
    freshness,
    iso,
    likely_driver,
    local,
    num,
    provider_caveats,
    public,
    state_note,
    wait_ratio,
    window_minutes,
    zone,
)


@audited
def get_zone_now(zone_id: int, provider: Provider = "uber", window: Window = "30m") -> dict[str, Any]:
    """Live state of one pickup zone at the current moment: median and 90th
    percentile wait (request -> driver arrival) over the last 15 or 30
    minutes, requests, open requests and the oldest open request, next to the
    normal for this weekday and hour. `state` says whether the zone can be
    compared with normal (same rule as find_shortages). provider defaults to
    uber; use 30m unless the question is about the last few minutes."""
    check(provider, window)
    z = zone(zone_id)
    c = clock()
    w, o = live.read([zone_id], provider, window)[zone_id]
    minutes = window_minutes(window)
    as_of = c["features_as_of"]
    fresh = freshness(as_of, c["now"], config.LIVE_SLA_S, c)
    caveats = provider_caveats(provider)
    if fresh["status"] in ("stale", "unavailable"):
        return {
            "zone": z,
            "provider": provider,
            "window": window,
            "status": "stale",
            "freshness": fresh,
            "note": "The live pipeline is behind; no current value.",
        }

    mid = (as_of - timedelta(minutes=minutes / 2)).astimezone(NY)
    b = baseline.for_hours(mid.replace(tzinfo=None, minute=0, second=0, microsecond=0), [zone_id], provider)[
        zone_id
    ]
    state = classify(w, o, b)
    requests = int(w["requests"]) if w else 0
    p50 = num(w.get("wait_p50_s")) if w else None
    expected = expected_requests(b, minutes)
    metrics = {
        "wait_p50_s": {"value": p50, "unit": "seconds"},
        "wait_p90_s": {"value": num(w.get("wait_p90_s")) if w else None, "unit": "seconds"},
        "requests": {
            "value": requests,
            "unit": "count",
            "expected_from_baseline": None if expected is None else round(expected, 1),
        },
        "open_requests": {"value": int(o.get("open_requests", 0)) if o else 0, "unit": "count"},
        "open_request_max_age_s": {
            "value": num(o.get("open_request_max_age_s")) if o else None,
            "unit": "seconds",
        },
    }

    # Compared with normal only when the zone is comparable, exactly as in
    # find_shortages, so the two tools never disagree about a zone.
    comparison = {"likely_driver": likely_driver(requests, expected)}
    if state == "scored":
        raw = wait_ratio(p50, b)
        comparison |= {
            "wait_ratio": round(raw, 2),
            "above_normal_range": above_normal_range(p50, b),
            "below_normal_range": b["_raw"].get("wait_p25_s") is not None
            and p50 < float(b["_raw"]["wait_p25_s"]),
            # How unusual for THIS zone: rank against its own last 8 weeks.
            **scores.pctiles({zone_id: raw}, provider, c["now"])[zone_id],
        }
    if note := state_note(state, window):
        caveats.append(note)
    caveats += baseline.holiday_caveat(mid.date())
    caveats.append(f"Baseline is per hour ({mid:%H}:00 local); the live value covers {window}.")
    caveats.append(
        "wait_ratio_pctile ranks this live window against the zone's hourly ratios of the last 8 "
        "weeks; short windows swing more than hours, so read 95+ as unusual, not as certain."
    )
    caveats.append("Only completed trips are in the data; unfulfilled requests are invisible.")

    return {
        "zone": z,
        "provider": provider,
        "window": window,
        "status": "ok",
        "state": state,
        "as_of_utc": iso(as_of),
        "as_of_local": local(as_of),
        "freshness": fresh,
        "metrics": metrics,
        "sample_size": {"wait_n": int(w["wait_n"]) if w else 0},
        "baseline": public(b),
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
        .execute(
            f"""
        SELECT as_of_utc, requests_{s}, wait_n_{s}, wait_p50_s_{s}, wait_p90_s_{s},
               open_requests, open_request_max_age_s
        FROM iceberg_scan('{meta}')
        WHERE zone_id = ? AND provider = ? AND as_of_utc > ? AND as_of_utc <= ?
        ORDER BY as_of_utc""",  # only the column suffix (validated window) and the metadata path are formatted
            [int(zone_id), provider, since, now_],
        )
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
