"""Every zone's live wait against its normal, read three ways: a ranked list
(find_shortages, with a choice of what to rank by) and a city summary per
borough (get_city_summary). Both use one scoring pass, so their numbers agree."""

import statistics
from datetime import timedelta
from typing import Any, Literal, get_args

from .. import baseline, config, live, places, scores
from ..audit import audited
from .common import (
    NY,
    STATES,
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
)

SortBy = Literal["wait_pctile", "oldest_open_request", "requests_vs_expected"]
SORTS = get_args(SortBy)


def _score_all(provider: str, window: str) -> dict[str, Any]:
    """One pass over all zones: live values, normal, ratio and percentile.
    Zones that cannot be ranked by wait keep their state and open requests,
    because an old open request matters even where waits are few."""
    c = clock()
    if c["features_lag_s"] is None or c["features_lag_s"] > config.LIVE_SLA_S:
        return {"stale": True, "clock": c}
    zone_ids = list(places.zones())
    values = live.read(zone_ids, provider, window)
    minutes = window_minutes(window)
    as_of = c["features_as_of"]
    mid_local = (as_of - timedelta(minutes=minutes / 2)).astimezone(NY)
    base = baseline.for_hours(
        mid_local.replace(tzinfo=None, minute=0, second=0, microsecond=0), zone_ids, provider
    )

    rows = []
    for z, (w, o) in values.items():
        b = base[z]
        requests = int(w["requests"]) if w else 0
        expected = expected_requests(b, minutes)
        p50 = num(w.get("wait_p50_s")) if w else None
        wait_n = int(w["wait_n"]) if w else 0
        state = classify(w, o, b)
        row = {
            "zone": places.zones()[z],
            "state": state,
            "wait_p50_s": p50,
            "wait_n": wait_n,
            "requests": requests,
            "expected_requests": None if expected is None else round(expected, 1),
            "_expected": expected,
            "likely_driver": likely_driver(requests, expected),
            "open_requests": int(o.get("open_requests", 0)) if o else 0,
            "open_request_max_age_s": num(o.get("open_request_max_age_s")) if o else None,
        }
        if state == "scored":
            row["_raw_ratio"] = wait_ratio(p50, b)  # ranked and compared unrounded, shown rounded
            row |= {
                "wait_ratio": round(row["_raw_ratio"], 2),
                "normal_wait_p50_s": b["wait_p50_s"],
                "normal_range_s": [b.get("wait_p25_s"), b.get("wait_p75_s")],
                "above_normal_range": above_normal_range(p50, b),
            }
        rows.append(row)

    scored = [r for r in rows if r["state"] == "scored"]
    pct = scores.pctiles({r["zone"]["zone_id"]: r["_raw_ratio"] for r in scored}, provider, c["now"])
    for r in scored:
        r.update(pct.get(r["zone"]["zone_id"], {}))
    skipped = {s: sum(1 for r in rows if r["state"] == s) for s in STATES if s != "scored"}
    return {
        "stale": False,
        "clock": c,
        "holiday": baseline.holiday_caveat(mid_local.date()),
        "as_of": as_of,
        "mid_local": mid_local,
        "rows": rows,
        "skipped": skipped,
    }


def _stale(provider: str, window: str, c: dict) -> dict[str, Any]:
    return {
        "status": "stale",
        "provider": provider,
        "window": window,
        "freshness": freshness(c["features_as_of"], c["now"], config.LIVE_SLA_S, c),
        "note": "Live features are behind the replay clock; no values are given.",
    }


def _header(provider: str, window: str, s: dict) -> dict[str, Any]:
    return {
        "status": "ok",
        "provider": provider,
        "window": window,
        "as_of_utc": iso(s["as_of"]),
        "as_of_local": local(s["as_of"]),
        "freshness": freshness(s["as_of"], s["clock"]["now"], config.LIVE_SLA_S, s["clock"]),
    }


@audited
def find_shortages(
    provider: Provider = "uber", window: Window = "30m", top: int = 10, sort_by: SortBy = "wait_pctile"
) -> dict[str, Any]:
    """Zones ranked right now. sort_by:
    - wait_pctile (default): how unusual the live wait is for the zone itself
      (wait_ratio = live median wait / normal for this weekday and hour,
      ranked against the zone's own last 8 weeks; 95+ = is_unusual). Zones
      with too few trips or a weak baseline are left out and counted.
    - oldest_open_request: zones whose oldest open request (requested, no
      driver arrived yet) has waited longest; every zone with open requests,
      ranked or not.
    - requests_vs_expected: zones with the most requests against the normal
      count for this weekday and hour (only zones expecting at least 10).
    Answers "where is there a driver shortage right now" and "where are
    requests sitting longest / where is demand up"."""
    check(provider, window)
    if sort_by not in SORTS:
        raise ValueError(f"sort_by must be one of {', '.join(SORTS)}")
    top = max(1, min(int(top), 30))
    s = _score_all(provider, window)
    if s["stale"]:
        return _stale(provider, window, s["clock"])
    rows = s["rows"]
    scored = [r for r in rows if r["state"] == "scored"]

    if sort_by == "wait_pctile":
        # Percentile first, then ratio: a plain ratio favours zones that always swing.
        ranked = sorted(
            scored,
            key=lambda r: (
                r.get("wait_ratio_pctile") is not None,
                r.get("wait_ratio_pctile") or 0,
                r["_raw_ratio"],
            ),
            reverse=True,
        )
        rule = "Ranked by wait_ratio_pctile, then wait_ratio; only zones in state scored."
    elif sort_by == "oldest_open_request":
        ranked = sorted(
            (r for r in rows if r["open_requests"] > 0 and r["open_request_max_age_s"] is not None),
            key=lambda r: r["open_request_max_age_s"],
            reverse=True,
        )
        rule = (
            "Ranked by open_request_max_age_s over every zone with open requests; zones not in state "
            "scored are included and carry their state (their wait cannot be compared with normal)."
        )
    else:
        ranked = sorted(
            (r for r in rows if r["_expected"] and r["_expected"] >= config.MIN_EXPECTED_REQUESTS),
            key=lambda r: r["requests"] / r["_expected"],
            reverse=True,
        )
        rule = (
            f"Ranked by requests / expected requests; only zones expecting at least "
            f"{config.MIN_EXPECTED_REQUESTS} requests in the window (smaller counts swing by chance)."
        )

    return {
        **_header(provider, window, s),
        "sort_by": sort_by,
        # Explicit names: an agent read a bare "count" as "zones above normal".
        "zones_scored": len(scored),
        "zones_above_normal_range": sum(1 for r in scored if r["above_normal_range"]),
        "zones_unusual": sum(1 for r in scored if r.get("is_unusual")),
        "zones_ranked": len(ranked),
        "zones_returned": min(top, len(ranked)),
        "zones": [public(r) for r in ranked[:top]],
        "left_out": s["skipped"],
        "caveats": provider_caveats(provider)
        + s["holiday"]
        + [
            rule,
            "left_out counts zones not compared with normal: "
            + " ".join(f"{k}: {state_note(k, window)}" for k, n in s["skipped"].items() if n),
            (
                "wait_ratio = live median wait / normal median wait for this weekday and hour. "
                "wait_ratio_pctile = share of the zone's own hours in the last 8 weeks with a lower ratio "
                "(95+ = is_unusual)."
            ),
            "The percentile compares a short live window with hourly history; short windows swing more.",
            f"Baseline hour: {s['mid_local']:%H}:00 local; the live value covers {window}.",
            "Only completed trips are in the data; unfulfilled requests are invisible.",
            (
                "likely_driver is a fixed rule: demand when requests >= 1.2x expected, else unclear. "
                "It never says supply: missing drivers cannot be seen in the data. Do not name another cause."
            ),
            "For the whole city or a borough, use get_city_summary instead of adding up this list.",
        ],
        "definition_version": VERSION,
        "source": f"{live.source()} for all zones + batch baseline (feat_zone_hour)",
    }


def _summarize(rows: list[dict]) -> dict[str, Any]:
    scored = [r for r in rows if r["state"] == "scored"]
    with_expected = [r for r in rows if r["_expected"]]
    requests = sum(r["requests"] for r in with_expected)
    expected = sum(r["_expected"] for r in with_expected)
    return {
        "zones": len(rows),
        "zones_scored": len(scored),
        "zones_above_normal_range": sum(1 for r in scored if r["above_normal_range"]),
        "zones_unusual": sum(1 for r in scored if r.get("is_unusual")),
        "share_scored_above_normal_range": round(
            sum(1 for r in scored if r["above_normal_range"]) / len(scored), 2
        )
        if scored
        else None,
        "median_wait_ratio": round(statistics.median(r["_raw_ratio"] for r in scored), 2) if scored else None,
        "requests": requests,
        "expected_requests": round(expected, 1),
        "likely_driver": likely_driver(requests, expected),
        "open_requests": sum(r["open_requests"] for r in rows),
        "oldest_open_request_s": max(
            (r["open_request_max_age_s"] for r in rows if r["open_request_max_age_s"] is not None),
            default=None,
        ),
    }


@audited
def get_city_summary(provider: Provider = "uber", window: Window = "30m") -> dict[str, Any]:
    """The whole city right now, per borough and in total: how many zones are
    above their normal range or unusual, the median wait_ratio, requests
    against the expected count for this weekday and hour (with
    likely_driver), and open requests. Use it for "is it everywhere or just
    a few zones", "how is the city doing", "is demand up overall"."""
    check(provider, window)
    s = _score_all(provider, window)
    if s["stale"]:
        return _stale(provider, window, s["clock"])
    rows = s["rows"]
    boroughs = sorted({r["zone"]["borough"] for r in rows})
    return {
        **_header(provider, window, s),
        "city": _summarize(rows),
        "boroughs": {b: _summarize([r for r in rows if r["zone"]["borough"] == b]) for b in boroughs},
        "left_out": s["skipped"],
        "caveats": provider_caveats(provider)
        + s["holiday"]
        + [
            (
                "median_wait_ratio and the zone counts use only zones in state scored; requests and "
                "expected_requests add up every zone that has a normal request count."
            ),
            "Totals are sums of zone windows; a borough with few scored zones is a weak read.",
            (
                "likely_driver per borough uses the same rule as for zones (demand when requests >= 1.2x "
                "expected, else unclear); it never says supply."
            ),
            f"Baseline hour: {s['mid_local']:%H}:00 local; the live value covers {window}.",
            "Only completed trips are in the data; unfulfilled requests are invisible.",
        ],
        "definition_version": VERSION,
        "source": f"{live.source()} for all zones + batch baseline (feat_zone_hour)",
    }
