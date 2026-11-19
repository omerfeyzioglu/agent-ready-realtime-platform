"""Rules every tool shares: the platform clock, freshness against an SLA,
input checks and provider caveats. Kept in one place so all tools enforce
them the same way."""

import functools
from datetime import datetime, timezone
from typing import Literal
from zoneinfo import ZoneInfo

import yaml

from .. import config, places, sources

NY = ZoneInfo(config.TIMEZONE)
Provider = Literal["uber", "lyft", "all"]
Window = Literal["15m", "30m"]
VERSION = "v1"


def utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        "wall_age_s": round(wall_age, 1),
        "speed": float(c.get("speed", 0)),
        "features_as_of": feats,
        "features_lag_s": None if feats is None else (now - feats).total_seconds(),
    }


def freshness(as_of: datetime | None, now: datetime, sla_s: int) -> dict:
    if as_of is None:
        return {"status": "unavailable", "sla_s": sla_s}
    lag = (now - as_of).total_seconds()
    status = "fresh" if lag <= sla_s else "stale"
    out = {"status": status, "age_s": round(lag), "sla_s": sla_s}
    if status == "stale":
        out["reason"] = f"value is {round(lag)} s behind the replay clock; the SLA is {sla_s} s. Withheld."
    return out


def window_minutes(window: str) -> int:
    return int(window.rstrip("m"))
