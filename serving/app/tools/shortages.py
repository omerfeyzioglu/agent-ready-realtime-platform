"""All zones ranked by how far their wait is above normal right now."""

from datetime import timedelta
from typing import Any

from .. import baseline, config, live, places, scores
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
    local,
    num,
    provider_caveats,
    window_minutes,
)


@audited
def find_shortages(provider: Provider = "uber", window: Window = "30m", top: int = 10) -> dict[str, Any]:
    """Zones where riders wait much longer than normal right now: every zone's
    live median wait divided by its normal for this weekday and hour
    (wait_ratio), ranked by how unusual that ratio is for the zone itself
    (wait_ratio_pctile against its own last 8 weeks; 95+ = is_unusual). Zones with too few trips or a weak baseline are left out
    (and counted). This answers "where is there a driver shortage right now"."""
    check(provider, window)
    top = max(1, min(int(top), 30))
    c = clock()
    if c["features_lag_s"] is None or c["features_lag_s"] > config.LIVE_SLA_S:
        return {
            "status": "stale",
            "provider": provider,
            "window": window,
            "freshness": freshness(c["features_as_of"], c["now"], config.LIVE_SLA_S),
            "note": "Live features are behind the replay clock; no ranking is given.",
        }
    zone_ids = list(places.zones())
    values = live.read(zone_ids, provider, window)

    minutes = window_minutes(window)
    as_of = c["features_as_of"]
    mid_local = (as_of - timedelta(minutes=minutes / 2)).astimezone(NY)
    base = baseline.for_hours(
        mid_local.replace(tzinfo=None, minute=0, second=0, microsecond=0),
        zone_ids,
        provider,
    )

    scored, skipped = [], {"no_activity": 0, "low_sample": 0, "weak_baseline": 0}
    for z, (w, o) in values.items():
        if not w or num(w.get("wait_p50_s")) is None:
            skipped["no_activity"] += 1
            continue
        if int(w["wait_n"]) < config.LOW_SAMPLE_WAITS:
            skipped["low_sample"] += 1
            continue
        b = base[z]
        if b["is_weak"] or not b.get("wait_p50_s"):
            skipped["weak_baseline"] += 1
            continue
        p50 = num(w["wait_p50_s"])
        scored.append(
            {
                "zone": places.zones()[z],
                "wait_p50_s": p50,
                "wait_ratio": round(p50 / b["wait_p50_s"], 2),
                "normal_wait_p50_s": b["wait_p50_s"],
                "normal_range_s": [b.get("wait_p25_s"), b.get("wait_p75_s")],
                "above_normal_range": b.get("wait_p75_s") is not None and p50 > b["wait_p75_s"],
                "wait_n": int(w["wait_n"]),
                "requests": int(w["requests"]),
                "expected_requests": None
                if b.get("requests_per_hour") is None
                else round(b["requests_per_hour"] * minutes / 60, 1),
                "open_requests": int(o.get("open_requests", 0)) if o else 0,
                "open_request_max_age_s": num(o.get("open_request_max_age_s")) if o else None,
            }
        )
    # Rank by how unusual the wait is for each zone itself (percentile against
    # its own last 8 weeks), then by the ratio. A plain ratio would favour
    # zones that always swing a lot.
    pct = scores.pctiles({x["zone"]["zone_id"]: x["wait_ratio"] for x in scored}, provider, c["now"])
    for x in scored:
        x.update(pct.get(x["zone"]["zone_id"], {}))
    scored.sort(
        key=lambda x: (
            x.get("wait_ratio_pctile") is not None,
            x.get("wait_ratio_pctile") or 0,
            x["wait_ratio"],
        ),
        reverse=True,
    )
    return {
        "status": "ok",
        "provider": provider,
        "window": window,
        "as_of_utc": iso(as_of),
        "as_of_local": local(as_of),
        "freshness": freshness(as_of, c["now"], config.LIVE_SLA_S),
        # Explicit names: an agent read a bare "count" as "zones above normal".
        "zones_scored": len(scored),
        "zones_above_normal_range": sum(1 for x in scored if x["above_normal_range"]),
        "zones_unusual": sum(1 for x in scored if x.get("is_unusual")),
        "zones_returned": min(top, len(scored)),
        "zones": scored[:top],
        "left_out": skipped,
        "caveats": provider_caveats(provider)
        + [
            "wait_ratio = live median wait / normal median wait for this weekday and hour. "
            "wait_ratio_pctile = share of the zone's own hours in the last 8 weeks with a lower ratio "
            "(95+ = is_unusual). Ranked by percentile, then ratio.",
            "The percentile compares a short live window with hourly history; short windows swing more.",
            f"Baseline hour: {mid_local:%H}:00 local; the live value covers {window}.",
            "Only completed trips are in the data; unfulfilled requests are invisible.",
        ],
        "definition_version": VERSION,
        "source": f"{live.source()} for all zones + batch baseline (feat_zone_hour)",
    }
