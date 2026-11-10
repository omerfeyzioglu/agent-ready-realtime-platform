{{ config(location=partition_location('trip_events', 'event_month', var('month'))) }}

-- One row per event with the JSON payload parsed into typed columns.
-- Built per event_time month. Duplicates (same event_id delivered twice,
-- e.g. a producer retry) are removed here: a duplicate carries the same
-- event_time, so it always lands in the same month and is caught.

with src as (
    select *
    from {{ source('raw', 'trip_events') }}
    where {{ in_month('event_time', var('month')) }}
    {{ slice_filter('event_time') }}
),

-- Find duplicate ids first (cheap: one column), then only rank those rows.
-- A window over every row of the month would need far more memory.
dup_ids as (
    select event_id
    from src
    group by event_id
    having count(*) > 1
),

deduped as (
    select * from src anti join dup_ids using (event_id)
    union all
    select * from src semi join dup_ids using (event_id)
    qualify row_number() over (
        partition by event_id order by ingest_time, kafka_offset nulls last
    ) = 1
),

parsed as (
    select
        *,
        json_valid(payload) as payload_ok,
        case when json_valid(payload) then from_json(payload, '{
            "provider": "VARCHAR", "originating_base": "VARCHAR", "pickup_zone": "INTEGER",
            "shared_request": "VARCHAR", "wav_request": "VARCHAR", "access_a_ride": "VARCHAR",
            "dispatching_base": "VARCHAR", "wav_match": "VARCHAR",
            "dropoff_zone": "INTEGER", "trip_miles": "DOUBLE", "trip_time": "BIGINT",
            "base_passenger_fare": "DOUBLE", "tolls": "DOUBLE", "bcf": "DOUBLE",
            "sales_tax": "DOUBLE", "congestion_surcharge": "DOUBLE", "airport_fee": "DOUBLE",
            "cbd_congestion_fee": "DOUBLE", "driver_pay": "DOUBLE", "shared_match": "VARCHAR",
            "tips": "DOUBLE", "_dst_assumed": "BOOLEAN"
        }') end as p
    from deduped
)

select
    event_id,
    trip_id,
    -- trip_id is a UUID v7 whose time part is the request time, so this is
    -- the month the trip started in (used to assemble whole trips later)
    strftime(uuid_extract_timestamp(trip_id::uuid), '%Y-%m') as trip_month,
    event_type,
    event_time,
    ingest_time,
    ingest_path,
    kafka_partition,
    kafka_offset,

    -- requested
    p.provider as provider_license,
    case p.provider when 'HV0003' then 'uber' when 'HV0005' then 'lyft' end as provider,
    p.originating_base,
    p.pickup_zone,
    {{ yn('p.shared_request') }} as shared_requested,
    {{ yn('p.wav_request') }} as wav_requested,
    {{ yn('p.access_a_ride') }} as access_a_ride,

    -- arrived
    p.dispatching_base,
    {{ yn('p.wav_match') }} as wav_matched,

    -- dropped_off
    p.dropoff_zone,
    p.trip_miles,
    p.trip_time as trip_time_s,
    p.base_passenger_fare,
    p.tolls,
    p.bcf,
    p.sales_tax,
    p.congestion_surcharge,
    p.airport_fee,
    p.cbd_congestion_fee,
    p.driver_pay,
    {{ yn('p.shared_match') }} as shared_matched,

    -- tip_added
    p.tips,

    -- flags (never drop: downstream decides)
    coalesce(p._dst_assumed, false) as is_dst_assumed,
    not payload_ok as is_payload_invalid
from parsed
