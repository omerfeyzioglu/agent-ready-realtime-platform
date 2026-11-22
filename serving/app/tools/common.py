"""Rules every tool shares: the platform clock, freshness against an SLA,
input checks and provider caveats. Kept in one place so all tools enforce
them the same way."""

import functools
from datetime import datetime, timezone
from typing import Literal
from zoneinfo import ZoneInfo

import yaml

from .. import config, live, places, sources

NY = ZoneInfo(config.TIMEZONE)
Provider = Literal["uber", "lyft", "all"]
Window = Literal["15m", "30m"]
VERSION = config.FEATURE_VERSION


utc = live.utc
iso = live.iso


def local(dt: datetime) -> str:
    return dt.astimezone(NY).strftime("%a %Y-%m-%d %H:%M %Z")


def num(v: str | None) -> float | None:
    return None if v in (None, "") else float(v)


def zone(zone_id: int) -> dict:
    z = places.zones().get(int(zone_id))
    if z is None:
        raise ValueError(f"zone_id {zone_id} is not a NYC taxi zone (1-263). Use resolve_place.")
    return z


def check(provider: str, window: str | None = None) -> None:
    if provider not in ("uber", "lyft", "all"):
        raise ValueError("provider must be uber, lyft or all")
    if window is not None and window not in ("15m", "30m"):
        raise ValueError("window must be 15m or 30m")


def provider_caveats(provider: str) -> list[str]:
    if provider == "uber":
        return []
    return [
        "Lyft data differs from Uber's: about a third of the volume per zone, so many "
        "zone-windows are low-sample" + ("; 'all' mixes both providers." if provider == "all" else ".")
    ]


@functools.cache
def live_definitions() -> dict:
    return yaml.safe_load(config.LIVE_DEFINITIONS.read_text())


def clock() -> dict:
    """The platform's current moment. Live: the replay clock in Redis. Frozen
    (evaluation): the fixed moment, with live values from the minute log."""
    at = live.frozen_at()
    if at is not None:
        live.check_in_log(at)
        return {
            "now": at,
            "running": True,
            "frozen": True,
            "wall_age_s": 0.0,
            "speed": 0.0,
            "features_as_of": at,
            "features_lag_s": 0.0,
        }
    r = sources.redis_client()
    c = r.hgetall("replay:v1:clock")
    hb = r.hgetall(f"feat:live:{VERSION}:heartbeat")
    if not c:
        raise RuntimeError("replay clock not available (producer has not published one)")
    now = utc(c["replay_time_utc"])
    wall_age = (datetime.now(timezone.utc) - utc(c["wall_time_utc"])).total_seconds()
    feats = utc(hb["as_of_utc"]) if hb else None
    return {
        "now": now,
        "running": wall_age <= config.CLOCK_STOPPED_AFTER_WALL_S,
        "frozen": False,
        "wall_age_s": round(wall_age, 1),
        "speed": float(c.get("speed", 0)),
        "features_as_of": feats,
        "features_lag_s": None if feats is None else (now - feats).total_seconds(),
    }


def freshness(as_of: datetime | None, now: datetime, sla_s: int, c: dict | None = None) -> dict:
    """fresh | stale (behind the replay clock: withheld) | paused (the replay
    clock itself stopped, e.g. the producer is down: the clock and the
    features freeze together, so the lag alone would still read 0)."""
    if as_of is None:
        return {"status": "unavailable", "sla_s": sla_s}
    lag = (now - as_of).total_seconds()
    status = "fresh" if lag <= sla_s else "stale"
    out = {"status": status, "age_s": round(lag), "sla_s": sla_s}
    if status == "stale":
        out["reason"] = f"value is {round(lag)} s behind the replay clock; the SLA is {sla_s} s. Withheld."
    elif c is not None and not c["running"]:
        out["status"] = "paused"
        out["reason"] = (
            f"the replay clock stopped {round(c['wall_age_s'])} s ago (wall time): these are the values "
            "at that moment, not live. Say so before using them."
        )
    return out


# Why a zone is or is not ranked, in this order. The eval key uses the same order.
STATES = ("no_activity", "no_completed_waits", "low_sample", "weak_baseline", "scored")


def classify(w: dict | None, o: dict | None, b: dict) -> str:
    """One rule for every tool: no_activity = nothing in the window and no
    open request; no_completed_waits = requests or open requests but no
    driver has arrived yet (can be the worst shortage, not a quiet zone);
    low_sample = too few waits to compare; weak_baseline = too few earlier
    weeks; scored = comparable with normal."""
    requests = int(w["requests"]) if w else 0
    open_n = int(o.get("open_requests", 0)) if o else 0
    if not w or num(w.get("wait_p50_s")) is None:
        return "no_completed_waits" if requests > 0 or open_n > 0 else "no_activity"
    if int(w["wait_n"]) < config.LOW_SAMPLE_WAITS:
        return "low_sample"
    if b["is_weak"] or not b.get("wait_p50_s"):
        return "weak_baseline"
    return "scored"


def state_note(state: str, window: str) -> str | None:
    return {
        "no_activity": f"No requests and no open requests in the last {window}.",
        "no_completed_waits": (
            f"Requests or open requests in the last {window}, but no driver arrived yet, so there is "
            "no wait to compare. Check open_requests and open_request_max_age_s: this can be a shortage."
        ),
        "low_sample": f"Too few completed waits in the last {window} (< {config.LOW_SAMPLE_WAITS}) to compare with normal.",
        "weak_baseline": f"Fewer than {config.BASELINE_MIN_WEEKS} usable earlier weeks for this hour: no reliable normal.",
    }.get(state)


def likely_driver(requests: int, expected: float | None) -> dict:
    """Why a wait may be long, decided by a fixed rule instead of by the
    model. The data can show extra demand (more requests than expected); it
    cannot show missing supply, because unfulfilled requests never appear.
    So the answer is "demand" or "unclear", never "supply"."""
    if not expected:
        return {
            "value": "unknown",
            "requests_vs_expected": None,
            "reason": "no expected request count for this weekday and hour",
        }
    raw = requests / expected  # decide on the raw ratio, show it rounded
    ratio = round(raw, 2)
    if raw >= config.DEMAND_RATIO:
        return {
            "value": "demand",
            "requests_vs_expected": ratio,
            "reason": f"requests are {ratio}x the expected count (rule: >= {config.DEMAND_RATIO}x)",
        }
    return {
        "value": "unclear",
        "requests_vs_expected": ratio,
        "reason": f"requests are {raw:.3f}x the expected count, below {config.DEMAND_RATIO}x; "
        "fewer drivers is possible but the data cannot show it (no driver count, "
        "unfulfilled requests are invisible)",
    }


def expected_requests(b: dict, minutes: int) -> float | None:
    """Normal request count for a window of this length (unrounded)."""
    rph = b["_raw"].get("requests_per_hour")
    return None if rph is None else float(rph) * minutes / 60


def wait_ratio(p50: float, b: dict) -> float:
    """Live median wait / normal median wait, unrounded."""
    return p50 / float(b["_raw"]["wait_p50_s"])


def above_normal_range(p50: float, b: dict) -> bool:
    p75 = b["_raw"].get("wait_p75_s")
    return p75 is not None and p50 > float(p75)


def public(d: dict) -> dict:
    """Drop internal (underscore) fields before a value leaves a tool."""
    return {k: v for k, v in d.items() if not k.startswith("_")}


def window_minutes(window: str) -> int:
    return int(window.rstrip("m"))
