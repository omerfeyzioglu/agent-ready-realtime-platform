"""Where the tools' live values come from.

Normally from Redis: the latest minute Flink wrote. With FROZEN_NOW set, the
server stands still at that moment (an evaluation run): the same values are
read from the Iceberg minute log, which holds every minute Flink also wrote
to Redis (one computation, two sinks). Both paths return the same shape, the
Redis hashes, so the tools never know which one answered.
"""

import functools
from datetime import datetime, timedelta, timezone

from . import config, sources

VERSION = config.FEATURE_VERSION

Values = dict[int, tuple[dict, dict]]  # zone_id -> (window hash, open-requests hash)


def frozen_at() -> datetime | None:
    """The fixed moment, whole minutes like the log, or None when live."""
    if not config.FROZEN_NOW:
        return None
    return utc(config.FROZEN_NOW).replace(second=0, microsecond=0)


def source() -> str:
    at = frozen_at()
    if at is None:
        return "Redis live features (Flink live-features)"
    return f"Iceberg features.feat_zone_minute_live at {iso(at)} (frozen moment, same records as Redis)"


def read(zone_ids: list[int], provider: str, window: str) -> Values:
    at = frozen_at()
    return (
        _from_redis(zone_ids, provider, window) if at is None else _from_log(zone_ids, provider, window, at)
    )


def _from_redis(zone_ids: list[int], provider: str, window: str) -> Values:
    pipe = sources.redis_client().pipeline(transaction=False)
    for z in zone_ids:
        pipe.hgetall(f"feat:zone_window:{VERSION}:{z}:{provider}:{window}")
        pipe.hgetall(f"feat:zone_open:{VERSION}:{z}:{provider}")
    pipe.hget(f"feat:live:{VERSION}:heartbeat", "as_of_utc")
    res = pipe.execute()
    hb = res[-1]
    # Flink stops writing a zone once its window is empty, but the last
    # record stays until its TTL (10 min). The minute log has no row for that
    # zone, so treat a record older than the heartbeat minute as gone: both
    # paths then agree that the zone is quiet.
    oldest = None if hb is None else utc(hb) - timedelta(seconds=60)

    def current(h: dict, field: str) -> dict:
        return h if h and (oldest is None or utc(h[field]) >= oldest) else {}

    return {
        z: (current(res[2 * i], "window_end_utc"), current(res[2 * i + 1], "as_of_utc"))
        for i, z in enumerate(zone_ids)
    }


def _from_log(zone_ids: list[int], provider: str, window: str, at: datetime) -> Values:
    """One row per active zone at that minute. A zone with no row had no
    activity in its last 30 minutes, which Redis shows as a missing key."""
    check_in_log(at)
    w = window  # validated by the tools ("15m" or "30m"), safe in a column name
    ids = ",".join(str(int(z)) for z in zone_ids) or "NULL"
    rows = (
        sources.duck()
        .execute(
            f"""SELECT zone_id, requests_{w}, wait_n_{w}, wait_p50_s_{w}, wait_p90_s_{w},
                       open_requests, open_request_max_age_s
                FROM iceberg_scan('{_meta()}')
                WHERE provider = ? AND as_of_utc = ? AND zone_id IN ({ids})""",
            [provider, at],
        )
        .fetchall()
    )
    minutes = int(w.rstrip("m"))
    start = iso(at - timedelta(minutes=minutes))
    out: Values = {z: ({}, {}) for z in zone_ids}
    for zid, req, wait_n, p50, p90, open_n, open_age in rows:
        win = {
            "window": w,
            "window_start_utc": start,
            "window_end_utc": iso(at),
            "requests": str(req),
            "wait_n": str(wait_n),
            "is_low_sample": str(wait_n < config.LOW_SAMPLE_WAITS).lower(),
        }
        if p50 is not None:  # same rounding as the Redis writer
            win["wait_p50_s"] = str(round(p50, 1))
            win["wait_p90_s"] = str(round(p90, 1))
        opn = {"as_of_utc": iso(at), "open_requests": str(open_n)}
        if open_age is not None:
            opn["open_request_max_age_s"] = str(open_age)
        out[zid] = (win, opn)
    return out


def utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _meta() -> str:
    return sources.iceberg_metadata(*config.HISTORY_TABLE)


@functools.cache
def check_in_log(at: datetime) -> None:
    """A frozen moment must be a minute the log actually holds; otherwise
    every zone would look idle and the answers would be silently wrong."""
    n, first, last = (
        sources.duck()
        .execute(
            f"SELECT count(*) FILTER (WHERE as_of_utc = ?), min(as_of_utc), max(as_of_utc) "
            f"FROM iceberg_scan('{_meta()}')",
            [at],
        )
        .fetchone()
    )
    if n == 0:
        raise RuntimeError(
            f"FROZEN_NOW {iso(at)} is not in the minute log (it covers {iso(first)} to {iso(last)})"
        )
