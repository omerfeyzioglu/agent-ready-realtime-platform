{{ config(location=feature_location('zone_hour_baseline', 'trip_month', var('month'))) }}
-- depends_on: {{ ref('feat_zone_hour') }}
-- depends_on: {{ ref('stg_holidays') }}

-- Grain: one row per zone x hour x provider, the same rows as feat_zone_hour.
-- "Normal" for an hour = the same zone, provider, weekday and local hour in
-- the previous {{ var('baseline_weeks') }} weeks. Only earlier hours are used, never the hour
-- itself, so the baseline is what was knowable before the hour happened.

{% set weeks = var('baseline_weeks') %}
{% set min_weeks = 3 %}

{%- set files = [] -%}
{%- for m in var('feature_months') -%}
    {%- do files.append("'" ~ feature_location('zone_hour', 'trip_month', m) ~ "'") -%}
{%- endfor %}

with history as (
    select * from read_parquet([{{ files | join(', ') }}])
),

holidays as (
    select holiday_date from read_parquet('{{ staging_root() }}/holidays/data.parquet')
),

current_rows as (
    select hour_utc, date_local, hour_of_day_local, zone_id, provider
    from history
    where trip_month = '{{ var("month") }}'
),

-- One row per (current hour, earlier week). Joining on date + hour of day
-- keeps the comparison on the NYC wall clock across DST changes.
matched as (
    select
        c.hour_utc, c.zone_id, c.provider,
        h.date_local as past_date,
        h.requests, h.dropoffs_in, h.avg_speed_mph, h.fare_per_mile, h.driver_pay_per_mile,
        -- a noisy median must not shape the baseline
        case when not h.is_low_sample then h.wait_p50_s end as wait_p50_s
    from current_rows c
    join history h
      on  h.zone_id = c.zone_id
      and h.provider = c.provider
      and h.hour_of_day_local = c.hour_of_day_local
      and h.date_local between c.date_local - {{ weeks * 7 }} and c.date_local - 7
      and (c.date_local - h.date_local) % 7 = 0
    -- holidays are not normal days: leave them out of the comparison set
    where h.date_local not in (select holiday_date from holidays)
),

agg as (
    select
        hour_utc, zone_id, provider,
        count(distinct past_date) as baseline_weeks_n,
        count(wait_p50_s) as baseline_wait_weeks_n,
        quantile_cont(wait_p50_s, 0.25) as baseline_wait_p25_s,
        quantile_cont(wait_p50_s, 0.50) as baseline_wait_p50_s,
        quantile_cont(wait_p50_s, 0.75) as baseline_wait_p75_s,
        median(requests) as baseline_requests,
        median(dropoffs_in) as baseline_dropoffs_in,
        median(avg_speed_mph) as baseline_avg_speed_mph,
        median(fare_per_mile) as baseline_fare_per_mile,
        median(driver_pay_per_mile) as baseline_driver_pay_per_mile
    from matched
    group by hour_utc, zone_id, provider
)

select
    '{{ var("month") }}' as trip_month,
    c.hour_utc,
    c.zone_id,
    c.provider,
    {{ weeks }} as baseline_window_weeks,
    coalesce(a.baseline_weeks_n, 0) as baseline_weeks_n,
    coalesce(a.baseline_wait_weeks_n, 0) as baseline_wait_weeks_n,
    a.baseline_wait_p25_s,
    a.baseline_wait_p50_s,
    a.baseline_wait_p75_s,
    a.baseline_requests,
    a.baseline_dropoffs_in,
    a.baseline_avg_speed_mph,
    a.baseline_fare_per_mile,
    a.baseline_driver_pay_per_mile,
    c.date_local in (select holiday_date from holidays) as is_holiday,
    -- too little history to call anything "normal"
    coalesce(a.baseline_wait_weeks_n, 0) < {{ min_weeks }} as is_baseline_weak
from current_rows c
left join agg a using (hour_utc, zone_id, provider)
