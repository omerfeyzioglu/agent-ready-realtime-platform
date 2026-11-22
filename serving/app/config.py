"""Settings, all from the environment so the same code runs on a laptop and
in the cluster (only the values differ)."""

import os
from pathlib import Path

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "16379"))
REDIS_USER = os.getenv("REDIS_USER", "api")  # read-only Redis user
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("BUCKET", "nyc-taxi-raw-969788248726")
FEATURES_ROOT = f"s3://{BUCKET}/lake/features"
STAGING_ROOT = f"s3://{BUCKET}/lake/staging"
HISTORY_TABLE = ("features", "feat_zone_minute_live")  # Glue database, table

REPO_ROOT = Path(os.getenv("REPO_ROOT", Path(__file__).resolve().parents[2]))
LIVE_DEFINITIONS = REPO_ROOT / "features" / "live.yaml"
GAZETTEER = Path(__file__).with_name("gazetteer.yaml")
AUDIT_LOG = Path(os.getenv("AUDIT_LOG", REPO_ROOT / "serving" / "audit" / "tool_calls.jsonl"))

TIMEZONE = "America/New_York"
FEATURE_VERSION = "v1"  # Redis key and response version of the live features

# Evaluation mode: the server stands still at this moment (ISO UTC, e.g.
# 2026-08-03T22:00Z) and reads live values from the Iceberg minute log
# instead of Redis. Unset = live. Answers are then reproducible and need no
# cluster.
FROZEN_NOW = os.getenv("FROZEN_NOW", "")

# Freshness rules, in replay time (the platform's clock).
LIVE_SLA_S = 120  # live features may lag the replay clock by 2 minutes
CLOCK_STOPPED_AFTER_WALL_S = 30  # producer silent this long (wall clock) = replay paused
BASELINE_WEEKS = 6  # same rule as dbt (backtested 2026-11-16)
BASELINE_MIN_WEEKS = 3
LOW_SAMPLE_WAITS = 10  # live windows; batch hours use 30
# likely_driver: requests at least this many times the expected count = demand.
DEMAND_RATIO = 1.2
# Ranking by requests vs expected: smaller expected counts swing by chance.
MIN_EXPECTED_REQUESTS = 10
MAX_HISTORY_MINUTES = 360
# The history table is committed every 5 minutes of WALL time (Flink
# checkpoints), which is 5 x speed minutes of replay time. Its SLA is one
# commit interval plus a minute, scaled by the replay speed.
HISTORY_COMMIT_WALL_S = 360
