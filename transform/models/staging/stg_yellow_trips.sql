{{ config(location=partition_location('yellow_trips', 'file_month', var('month'))) }}

-- Yellow taxi trips, one row per source row, built per TLC file month.
-- Yellow is batch-only (no request or arrival time, so no events): this model
-- reads the source file directly. Column names are normalized to snake_case,
-- ids cast to INTEGER, and columns a month does not have are NULL (never 0).

{%- set optional = {
    'cbd_congestion_fee': 'double',
    'request_source': 'varchar',
} -%}
{%- set present = [] -%}
{%- if execute -%}
    {%- set described = run_query("describe select * from " ~ source('source_files', 'yellow_trips')) -%}
    {%- for name in described.columns[0].values() -%}
        {%- do present.append(name | lower) -%}
    {%- endfor -%}
{%- endif %}

with src as (
    select * from {{ source('source_files', 'yellow_trips') }}
),

typed as (
    select
        trip_id::varchar                         as trip_id,
        VendorID::integer                        as vendor_id,
        -- TLC writes NYC local time; stored here as UTC like every other table.
        -- In the fall-back hour (01:00-01:59 happens twice) the conversion takes
        -- one occurrence and the row is flagged.
        timezone('America/New_York', tpep_pickup_datetime)  as pickup_at,
        timezone('America/New_York', tpep_dropoff_datetime) as dropoff_at,
        tpep_pickup_datetime                     as pickup_local,
        tpep_dropoff_datetime                    as dropoff_local,
        passenger_count::integer                 as passenger_count,
        trip_distance,
        RatecodeID::integer                      as ratecode_id,
        {{ yn('store_and_fwd_flag') }}           as store_and_forward,
        PULocationID::integer                    as pickup_zone,
        DOLocationID::integer                    as dropoff_zone,
        payment_type::integer                    as payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,                                         -- "Airport_fee" in some months
        {%- for col, typ in optional.items() %}
        {% if col in present %}{{ col }}::{{ typ }}{% else %}null::{{ typ }}{% endif %} as {{ col }},
        {%- endfor %}
        -- for duplicate detection: every source column except the generated trip_id
        hash(* columns(* exclude (trip_id)))     as content_hash
    from src
)

select
    * exclude (content_hash),
    epoch(dropoff_at - pickup_at) as pickup_to_dropoff_s,

    -- TLC files contain stray rows from other months/years
    strftime(pickup_local, '%Y-%m') <> '{{ var("month") }}' as is_outside_file_month,
    coalesce(dropoff_at < pickup_at, false) as is_out_of_order,
    (pickup_local::date in ({{ fall_back_dates() }}) and hour(pickup_local) = 1)
        or (dropoff_local::date in ({{ fall_back_dates() }}) and hour(dropoff_local) = 1)
        as is_dst_ambiguous,
    {{ zone_unknown('pickup_zone') }}      as is_pickup_zone_unknown,
    {{ zone_outside_nyc('pickup_zone') }}  as is_pickup_outside_nyc,
    {{ zone_unknown('dropoff_zone') }}     as is_dropoff_zone_unknown,
    {{ zone_outside_nyc('dropoff_zone') }} as is_dropoff_outside_nyc,
    -- refunds and adjustments appear as rows with negative amounts
    coalesce(total_amount < 0, false) as has_negative_amount,
    -- trip_id is random, so an exact repeat of a source row is found by content
    row_number() over (partition by content_hash order by trip_id) > 1 as is_content_duplicate
from typed
