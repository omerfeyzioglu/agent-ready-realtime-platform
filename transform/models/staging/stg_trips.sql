{{ config(location=partition_location('trips', 'trip_month', var('month'))) }}
-- depends_on: {{ ref('stg_trip_events') }}

-- One row per FHVHV trip, assembled from its events. Built per trip_month
-- (the month of the request). A trip's later events can fall into the next
-- month (and, with bad source clocks, the previous one), so the runner passes
-- the staged event partitions month-1..month+1.

{%- set files = [] -%}
{%- for m in var('event_months') -%}
    {%- do files.append("'" ~ partition_location('trip_events', 'event_month', m) ~ "'") -%}
{%- endfor %}

with events as (
    select *
    from read_parquet([{{ files | join(', ') }}])
    where trip_month = '{{ var("month") }}'
),

trips as (
    select
        trip_id,
        any_value(trip_month) as trip_month,

        min(event_time) filter (where event_type = 'requested')   as requested_at,
        min(event_time) filter (where event_type = 'arrived')     as arrived_at,
        min(event_time) filter (where event_type = 'picked_up')   as picked_up_at,
        min(event_time) filter (where event_type = 'dropped_off') as dropped_off_at,
        min(event_time) filter (where event_type = 'tip_added')   as tip_added_at,

        any_value(provider)          filter (where event_type = 'requested') as provider,
        any_value(provider_license)  filter (where event_type = 'requested') as provider_license,
        any_value(originating_base)  filter (where event_type = 'requested') as originating_base,
        any_value(pickup_zone)       filter (where event_type = 'requested') as pickup_zone,
        any_value(shared_requested)  filter (where event_type = 'requested') as shared_requested,
        any_value(wav_requested)     filter (where event_type = 'requested') as wav_requested,
        any_value(access_a_ride)     filter (where event_type = 'requested') as access_a_ride,

        any_value(dispatching_base)  filter (where event_type = 'arrived') as dispatching_base,
        any_value(wav_matched)       filter (where event_type = 'arrived') as wav_matched,

        any_value(dropoff_zone)          filter (where event_type = 'dropped_off') as dropoff_zone,
        any_value(trip_miles)            filter (where event_type = 'dropped_off') as trip_miles,
        any_value(trip_time_s)           filter (where event_type = 'dropped_off') as trip_time_s,
        any_value(base_passenger_fare)   filter (where event_type = 'dropped_off') as base_passenger_fare,
        any_value(tolls)                 filter (where event_type = 'dropped_off') as tolls,
        any_value(bcf)                   filter (where event_type = 'dropped_off') as bcf,
        any_value(sales_tax)             filter (where event_type = 'dropped_off') as sales_tax,
        any_value(congestion_surcharge)  filter (where event_type = 'dropped_off') as congestion_surcharge,
        any_value(airport_fee)           filter (where event_type = 'dropped_off') as airport_fee,
        any_value(cbd_congestion_fee)    filter (where event_type = 'dropped_off') as cbd_congestion_fee,
        any_value(driver_pay)            filter (where event_type = 'dropped_off') as driver_pay,
        any_value(shared_matched)        filter (where event_type = 'dropped_off') as shared_matched,

        coalesce(any_value(tips) filter (where event_type = 'tip_added'), 0) as tips,

        count(*) as event_count,
        bool_or(is_dst_assumed) as is_dst_assumed
    from events
    group by trip_id
)

select
    *,

    -- durations in seconds; which one is "wait time" is a metric decision
    -- (Uber: request -> arrival, Lyft: request -> pickup), made downstream
    epoch(arrived_at - requested_at)       as request_to_arrival_s,
    epoch(picked_up_at - requested_at)     as request_to_pickup_s,
    epoch(picked_up_at - arrived_at)       as arrival_to_pickup_s,
    epoch(dropped_off_at - picked_up_at)   as pickup_to_dropoff_s,

    -- flags: rows are kept, each metric decides what to exclude
    requested_at is null or picked_up_at is null or dropped_off_at is null
        as is_incomplete,
    coalesce(not (
        requested_at <= coalesce(arrived_at, picked_up_at)
        and coalesce(arrived_at, picked_up_at) <= picked_up_at
        and picked_up_at <= dropped_off_at
    ), false) as is_out_of_order,
    {{ zone_unknown('pickup_zone') }}      as is_pickup_zone_unknown,
    {{ zone_outside_nyc('pickup_zone') }}  as is_pickup_outside_nyc,
    {{ zone_unknown('dropoff_zone') }}     as is_dropoff_zone_unknown,
    {{ zone_outside_nyc('dropoff_zone') }} as is_dropoff_outside_nyc,
    coalesce(least(base_passenger_fare, tolls, bcf, sales_tax, congestion_surcharge,
                   airport_fee, cbd_congestion_fee, driver_pay, tips) < 0, false)
        as has_negative_amount,
    -- trip_id is random, so the same ride delivered twice gets two ids;
    -- duplicates are found by content (provider, base, times, zones).
    -- Incomplete trips are left out: their missing times are NULL, and a
    -- window groups NULLs together, which would pair unrelated trips.
    case when requested_at is not null and picked_up_at is not null and dropped_off_at is not null
         then row_number() over (
                  partition by provider_license, dispatching_base, requested_at, picked_up_at,
                               dropped_off_at, pickup_zone, dropoff_zone,
                               requested_at is null or picked_up_at is null or dropped_off_at is null
                  order by trip_id
              ) > 1
         else false
    end as is_content_duplicate
from trips
