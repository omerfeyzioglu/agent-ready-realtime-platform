{{ config(location=feature_location('zone_hour_vs_baseline', 'trip_month', var('month'))) }}
-- depends_on: {{ ref('feat_zone_hour') }}
-- depends_on: {{ ref('feat_zone_hour_baseline') }}

-- Grain: one row per zone x hour x provider, the same rows as feat_zone_hour.
-- Puts the actual value next to its baseline and scores the wait:
--   wait_ratio        = actual wait / normal wait (1.0 = as usual)
--   wait_ratio_pctile = share of this zone's own hours in the previous
--                       {{ var('score_weeks') }} weeks that had a lower ratio (0-100)
-- The score replaces a fixed threshold: zones differ in how much they swing,
-- so "unusual" is judged against each zone's own recent history.

{% set score_days = var('score_weeks') * 7 %}
{% set min_history = 200 %}

{%- set actual = [] -%}
{%- set baseline = [] -%}
{%- for m in var('feature_months') -%}
    {%- do actual.append("'" ~ feature_location('zone_hour', 'trip_month', m) ~ "'") -%}
    {%- do baseline.append("'" ~ feature_location('zone_hour_baseline', 'trip_month', m) ~ "'") -%}
{%- endfor %}

with joined as (
    select
        f.*,
        b.* exclude (trip_month, hour_utc, zone_id, provider),
        -- a ratio needs a trustworthy value on both sides
        case when not f.is_low_sample and not b.is_baseline_weak and b.baseline_wait_p50_s > 0
             then f.wait_p50_s / b.baseline_wait_p50_s end as wait_ratio,
        f.requests / nullif(b.baseline_requests, 0)                     as requests_ratio,
        f.dropoffs_in / nullif(b.baseline_dropoffs_in, 0)               as dropoffs_in_ratio,
        f.avg_speed_mph / nullif(b.baseline_avg_speed_mph, 0)           as speed_ratio,
        f.fare_per_mile / nullif(b.baseline_fare_per_mile, 0)           as fare_per_mile_ratio
    from read_parquet([{{ actual | join(', ') }}]) f
    join read_parquet([{{ baseline | join(', ') }}]) b using (hour_utc, zone_id, provider)
),

current_rows as (
    select * from joined where trip_month = '{{ var("month") }}'
),

history as (
    select zone_id, provider, hour_utc, wait_ratio
    from joined
    where wait_ratio is not null
),

-- For every current hour: how many of the zone's earlier hours exist in the
-- window, and how many of them had a lower ratio. Earlier hours only.
scored as (
    select
        c.hour_utc, c.zone_id, c.provider,
        count(*) as score_history_n,
        count(*) filter (where h.wait_ratio < c.wait_ratio) as lower_n
    from current_rows c
    join history h
      on  h.zone_id = c.zone_id
      and h.provider = c.provider
      and h.hour_utc >= c.hour_utc - interval {{ score_days }} day
      and h.hour_utc <  c.hour_utc
    where c.wait_ratio is not null
    group by c.hour_utc, c.zone_id, c.provider
)

select
    c.*,
    coalesce(s.score_history_n, 0) as score_history_n,
    case when s.score_history_n >= {{ min_history }}
         then round(100.0 * s.lower_n / s.score_history_n, 1) end as wait_ratio_pctile
from current_rows c
left join scored s using (hour_utc, zone_id, provider)
