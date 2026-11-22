"""What the platform can and cannot answer, and metric definitions."""

from typing import Any

from ..audit import audited
from .common import live_definitions


@audited
def get_catalog() -> dict[str, Any]:
    """What this platform can and cannot answer. Describe your capabilities
    only from this list (questions_answerable / not_built_yet), never from
    general knowledge. If a topic is not available or not built yet, say so
    and do not estimate it from other data."""
    d = live_definitions()
    return {
        "questions_answerable": [
            "What is 'now' in the replay, and is the live data current? (now)",
            "Where is there a driver shortage right now, ranked? (find_shortages, sort_by=wait_pctile)",
            "Which zones have open requests waiting longest? (find_shortages, sort_by=oldest_open_request)",
            "Where are requests most above normal? (find_shortages, sort_by=requests_vs_expected)",
            "How is the whole city or a borough doing; is it everywhere or a few zones? (get_city_summary)",
            "How is a place or zone doing right now vs its normal, over 15 or 30 minutes? (get_zone_now)",
            "How unusual is a zone's wait vs its own last 8 weeks (percentile)? (get_zone_now, find_shortages)",
            "How did a zone develop minute by minute over the last hours? (get_zone_history)",
            "Is extra demand behind a long wait (likely_driver: demand if requests >= 1.2x expected, else unclear)? (get_zone_now, find_shortages)",
            "Which zones does a place name cover? (resolve_place)",
            "What does a metric mean? (describe_metric)",
        ],
        "not_built_yet": {
            "recurring weekday-hour shortage patterns": "planned (batch); cannot be answered yet",
            "incentive plans (where and when to pay extra)": "planned; only ad-hoc reasoning on live data now",
            "revenue, fares or driver pay totals per zone or period": "planned as a governed batch metric",
            "data pipeline health (lag, failures, lineage)": "planned for the reliability agent",
            "weather or events per zone": "raw data exists, not modelled yet",
        },
        "live_metrics_per_zone": {
            name: {
                "unit": f.get("unit"),
                "definition": " ".join(str(f.get("definition", "")).split()),
            }
            for section in ("windowed", "open")
            for name, f in d[section]["fields"].items()
            if isinstance(f, dict) and "unit" in f
        },
        "windows": d["windowed"]["windows"],
        "providers": ["uber (default)", "lyft", "all (both, only when asked)"],
        "history": "minute by minute per zone since the live replay started (get_zone_history)",
        "normal": "same zone, provider, weekday and NYC hour over the previous 6 weeks (batch, backtested)",
        "not_available": {
            "number of drivers online or available": "not in the data; wait time is the proxy for supply",
            "cancellations and unfulfilled requests": "only completed trips exist; open_requests undercounts real demand",
            "safety reports, incidents, complaints, ratings": "no such data in any source",
            "driver or rider identities": "not in the data",
            "prices shown to riders / surge multipliers": "not in the data (only fares actually paid, batch)",
        },
    }


@audited
def describe_metric(name: str) -> dict[str, Any]:
    """Definition, unit and version of a metric name used in the answers."""
    d = live_definitions()
    for section in ("windowed", "open"):
        f = d[section]["fields"].get(name)
        if isinstance(f, dict):
            return {
                "name": name,
                "unit": f.get("unit"),
                "version": d["version"],
                "definition": " ".join(str(f.get("definition", "")).split()),
                "computed_by": "Flink live-features, from features/live.yaml",
            }
    return {
        "name": name,
        "status": "not_available",
        "note": "Not a metric of this platform. See get_catalog for what exists.",
    }
