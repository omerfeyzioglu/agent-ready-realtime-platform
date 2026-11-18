-- Raw writer: trip-events (Kafka) -> raw.trip_events (Iceberg, Glue catalog).
--
-- Every event is stored as delivered: envelope columns plus the payload as
-- a JSON string. Events that cannot be read go to trip-events-dlq with the
-- reason, instead of stopping the job. The Iceberg sink commits on each
-- checkpoint (every 5 minutes), together with the Kafka offsets, so a
-- restart neither loses nor duplicates rows.

CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  'warehouse' = 's3://nyc-taxi-raw-969788248726/lake'
);

-- The whole Kafka value as text ('raw' format), plus Kafka metadata.
CREATE TEMPORARY TABLE kafka_trip_events (
  `value`     STRING,
  `headers`   MAP<STRING, BYTES> METADATA FROM 'headers' VIRTUAL,
  `partition` INT METADATA FROM 'partition' VIRTUAL,
  `offset`    BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'trip-events',
  'properties.bootstrap.servers' = 'events-kafka-bootstrap.kafka:9092',
  'properties.group.id' = 'raw-writer',
  -- First start: read from the beginning; later: continue from the
  -- offsets committed with the last checkpoint.
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'raw'
);

CREATE TEMPORARY TABLE kafka_dlq (
  reason       STRING,
  source_topic STRING,
  `partition`  INT,
  `offset`     BIGINT,
  raw_value    STRING,
  detected_at  TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'trip-events-dlq',
  'properties.bootstrap.servers' = 'events-kafka-bootstrap.kafka:9092',
  'format' = 'json'
);

-- Parse once. JSON_VALUE and TO_TIMESTAMP return NULL on bad input instead
-- of failing, so a broken event becomes a row with NULLs and a reason.
-- Times are UTC strings; the session time zone is UTC.
CREATE TEMPORARY VIEW parsed AS
SELECT
  event_id, trip_id, event_type, payload, event_ts, ingest_ts,
  `partition`, `offset`, `value`,
  CASE
    WHEN event_id IS NULL OR trip_id IS NULL THEN 'missing id'
    WHEN event_type IS NULL
      OR event_type NOT IN ('requested', 'arrived', 'picked_up', 'dropped_off', 'tip_added')
      THEN 'unknown event_type'
    WHEN event_ts IS NULL THEN 'bad event_time'
    WHEN payload IS NULL THEN 'missing payload'
    WHEN ingest_ts IS NULL THEN 'missing replay_time header'
  END AS reason
FROM (
  SELECT
    JSON_VALUE(`value`, '$.event_id')   AS event_id,
    JSON_VALUE(`value`, '$.trip_id')    AS trip_id,
    JSON_VALUE(`value`, '$.event_type') AS event_type,
    JSON_QUERY(`value`, '$.payload')    AS payload,
    CAST(TO_TIMESTAMP(SUBSTRING(JSON_VALUE(`value`, '$.event_time') FROM 1 FOR 19),
                      'yyyy-MM-dd''T''HH:mm:ss') AS TIMESTAMP_LTZ(6)) AS event_ts,
    -- ingest_time = when the event was sent on the replay clock (header set
    -- by the producer), so as-of views are right at any replay speed.
    CAST(TO_TIMESTAMP(SUBSTRING(CAST(`headers`['replay_time'] AS STRING) FROM 1 FOR 19),
                      'yyyy-MM-dd''T''HH:mm:ss') AS TIMESTAMP_LTZ(6)) AS ingest_ts,
    `partition`, `offset`, `value`
  FROM kafka_trip_events
);

-- `raw` is a reserved word in Flink SQL (the RAW type), so it is quoted.
INSERT INTO lake.`raw`.trip_events
SELECT event_id, trip_id, event_type, event_ts, payload, ingest_ts,
       'stream', `partition`, `offset`
FROM parsed
WHERE reason IS NULL;

INSERT INTO kafka_dlq
SELECT reason, 'trip-events', `partition`, `offset`, `value`, CURRENT_TIMESTAMP
FROM parsed
WHERE reason IS NOT NULL;
