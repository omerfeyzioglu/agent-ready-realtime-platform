"""What the platform can and cannot answer, and metric definitions."""

from typing import Any

from ..audit import audited
from .common import live_definitions


@audited
def get_catalog() -> dict[str, Any]:
    """What this platform can and cannot answer. Check it before answering a
    question about a topic you have not seen here; if the topic is listed as
    not available, say so and do not estimate it from other data."""
    d = live_definitions()
    return {
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
            "weather or events per zone": "not built yet",
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
