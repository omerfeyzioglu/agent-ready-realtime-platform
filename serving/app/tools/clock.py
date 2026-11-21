"""now: the platform's current moment (replay clock) and pipeline status."""

from typing import Any

from .. import config
from ..audit import audited
from .common import clock, iso, local


@audited
def now() -> dict[str, Any]:
    """The platform's current moment (the replay clock), in UTC and New York
    time, and whether the live pipeline is keeping up. Call this first: every
    "now", "today" or "right now" in a question means this moment, not the
    real-world date."""
    c = clock()
    lag = c["features_lag_s"]
    if c["frozen"]:
        status, note = "ok", "Time is fixed at this moment (frozen replay); values are as of this minute."
    elif not c["running"]:
        status, note = "paused", "Replay is paused: time is frozen at this moment."
    elif lag is None or lag > config.LIVE_SLA_S:
        status, note = (
            "degraded",
            "Live features are behind the replay clock; live answers will be withheld.",
        )
    else:
        status, note = "ok", "Live features are current."
    return {
        "status": status,
        "now_utc": iso(c["now"]),
        "now_local": local(c["now"]),
        "replay_speed": c["speed"],
        "live_features_as_of_utc": None if c["features_as_of"] is None else iso(c["features_as_of"]),
        "live_features_lag_s": None if lag is None else round(lag),
        "live_sla_s": config.LIVE_SLA_S,
        "note": note + " Data is a replay of real NYC trips; 'now' is replay time.",
    }
