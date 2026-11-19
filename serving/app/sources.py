"""Connections to the data, in one place. Tools never open their own: every
read goes through the clients here, which carry the API's own (read-only)
identities: the Redis user `api` and, in the cluster, the API's IAM role."""

import threading
import time

import boto3
import duckdb
import redis

from . import config

_lock = threading.Lock()
_redis: redis.Redis | None = None
_duck: duckdb.DuckDBPyConnection | None = None
_meta_cache: dict[tuple[str, str], tuple[float, str]] = {}


def redis_client() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            username=config.REDIS_USER,
            password=config.REDIS_PASSWORD,
            decode_responses=True,
            socket_timeout=2,
            # The read-only user may not run CLIENT SETINFO.
            lib_name=None,
            lib_version=None,
        )
    return _redis


def duck() -> duckdb.DuckDBPyConnection:
    """One DuckDB connection for S3 reads (Parquet, Iceberg). DuckDB
    connections are not safe across threads, so callers take a cursor."""
    global _duck
    with _lock:
        if _duck is None:
            c = duckdb.connect()
            c.sql(
                "INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws; "
                "INSTALL iceberg; LOAD iceberg; SET enable_progress_bar = false; "
                "SET TimeZone = 'UTC'"
            )
            c.sql(f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION '{config.AWS_REGION}')")
            _duck = c
        return _duck.cursor()


def iceberg_metadata(database: str, table: str, max_age_s: float = 60) -> str:
    """Current metadata file of an Iceberg table, from Glue (cached for a
    minute: the table changes every 5 minutes)."""
    key = (database, table)
    hit = _meta_cache.get(key)
    if hit and time.monotonic() - hit[0] < max_age_s:
        return hit[1]
    glue = boto3.client("glue", region_name=config.AWS_REGION)
    loc = glue.get_table(DatabaseName=database, Name=table)["Table"]["Parameters"]["metadata_location"]
    _meta_cache[key] = (time.monotonic(), loc)
    return loc
