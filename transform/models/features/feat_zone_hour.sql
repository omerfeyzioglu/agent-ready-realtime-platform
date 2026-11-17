{{ config(location=feature_location('zone_hour', 'trip_month', var('month'))) }}
-- depends_on: {{ ref('stg_trips') }}

-- Grain: one row per zone x hour x provider (uber, lyft, all).
-- Pickup-side metrics (requests, wait, speed, prices) are counted in the
-- pickup zone and the hour of the request. dropoffs_in is counted in the
-- dropoff zone and the hour of the dropoff: drivers becoming free there.
-- Built per month. NYC is a whole number of hours from UTC, so a local hour
-- is never split between two monthly partitions.

{% set min_mph = 1 %}
{% set max_mph = 80 %}
{% set low_sample = 30 %}

{%- set files = [] -%}
{%- for m in var('trip_months') -%}
    {%- do files.append("'" ~ partition_location('trips', 'trip_month', m) ~ "'") -%}
{%- endfor %}

-- A trip requested late in one month can end in the next, so dropoffs of
-- this month are read from this and the previous trip partition.
with source as (
    select *
    from read_parquet([{{ files | join(', ') }}])
    -- Excluded: trips we cannot place or time correctly. Counted in staging.
    where not is_incomplete
      and not is_out_of_order
      and not is_content_duplicate
),

pickups as (
    select
        date_trunc('hour', requested_at) as hour_utc,
        pickup_zone as zone_id,
        provider,
        request_to_arrival_s,
        request_to_pickup_s,
        trip_miles,
        pickup_to_dropoff_s,
        base_passenger_fare,
        driver_pay,
        -- Speed is an average, so physically impossible trips are left out
        -- of it. Medians need no such guard.
        coalesce(trip_miles / nullif(pickup_to_dropoff_s, 0) * 3600
                 between {{ min_mph }} and {{ max_mph }}, false) as speed_ok,
        -- Money ratios skip refunds/adjustments and zero-distance trips.
        trip_miles > 0 and not has_negative_amount as money_ok
    from source
    where trip_month = '{{ var("month") }}'
      and not is_pickup_zone_unknown
      and not is_pickup_outside_nyc
),

pickup_agg as (
    select
        hour_utc,
        zone_id,
        coalesce(provider, 'all') as provider,

        count(*) as requests,

        -- wait time = request -> driver arrival (seconds)
        count(request_to_arrival_s) as wait_n,
        quantile_cont(request_to_arrival_s, 0.25) as wait_p25_s,
        quantile_cont(request_to_arrival_s, 0.50) as wait_p50_s,
        quantile_cont(request_to_arrival_s, 0.75) as wait_p75_s,
        quantile_cont(request_to_arrival_s, 0.90) as wait_p90_s,

        -- request -> pickup also contains the rider walking to the car
        quantile_cont(request_to_pickup_s, 0.50) as request_to_pickup_p50_s,

        -- correct ratios: total / total, never the mean of per-trip ratios
        sum(trip_miles) filter (where speed_ok)
            / (sum(pickup_to_dropoff_s) filter (where speed_ok) / 3600.0) as avg_speed_mph,
        count(*) filter (where speed_ok) as speed_n,
        sum(base_passenger_fare) filter (where money_ok)
            / sum(trip_miles) filter (where money_ok) as fare_per_mile,
        sum(driver_pay) filter (where money_ok)
            / sum(trip_miles) filter (where money_ok) as driver_pay_per_mile
    from pickups
    group by grouping sets ((hour_utc, zone_id, provider), (hour_utc, zone_id))
),

dropoff_agg as (
    select
        date_trunc('hour', dropped_off_at) as hour_utc,
        dropoff_zone as zone_id,
        coalesce(provider, 'all') as provider,
        count(*) as dropoffs_in
    from source
    where {{ in_month('dropped_off_at', var('month')) }}
      and not is_dropoff_zone_unknown
      and not is_dropoff_outside_nyc
    group by grouping sets ((1, 2, source.provider), (1, 2))
),

-- A zone-hour with dropoffs but no requests still gets a row (requests = 0).
joined as (
    select *
    from pickup_agg
    full outer join dropoff_agg using (hour_utc, zone_id, provider)
)

select
    '{{ var("month") }}' as trip_month,
    hour_utc,
    -- NYC wall-clock columns, used to compare an hour with its own kind
    (hour_utc at time zone 'America/New_York') as hour_local,
    (hour_utc at time zone 'America/New_York')::date as date_local,
    isodow(hour_utc at time zone 'America/New_York') as weekday_local,
    hour(hour_utc at time zone 'America/New_York') as hour_of_day_local,
    zone_id,
    provider,
    coalesce(requests, 0) as requests,
    coalesce(dropoffs_in, 0) as dropoffs_in,
    coalesce(wait_n, 0) as wait_n,
    wait_p25_s,
    wait_p50_s,
    wait_p75_s,
    wait_p90_s,
    request_to_pickup_p50_s,
    avg_speed_mph,
    coalesce(speed_n, 0) as speed_n,
    fare_per_mile,
    driver_pay_per_mile,
    -- kept, not dropped: a median over a handful of trips is noise
    coalesce(wait_n, 0) < {{ low_sample }} as is_low_sample
from joined
