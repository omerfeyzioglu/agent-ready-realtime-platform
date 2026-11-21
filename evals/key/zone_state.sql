-- Answer key: every pickup zone's state at one frozen moment, computed from
-- the tables directly, not through the API. The rules follow the dbt models:
--   normal     = feat_zone_hour_baseline: same zone, provider, weekday and NYC
--                hour in the previous 6 weeks; holidays left out; low-sample
--                hours do not shape the wait; weak below 3 usable weeks
--   percentile = feat_zone_hour_vs_baseline: share of the zone's own hourly
--                ratios in the previous 8 weeks that are lower; none under
--                200 hours
-- The live side is the 30-minute window Flink wrote for that minute (the
-- minute log holds the same records as Redis).
--
-- Names in curly braces are file paths filled in by build_key.py; $params are values.
--   $moment_utc  the frozen minute
--   $local_date  NYC date of the window's middle (the baseline's weekday)
--   $hour        NYC hour of the window's middle (the baseline's hour)
--   $provider    uber | lyft

with live as (
    select
        zone_id,
        requests_30m                as requests,
        wait_n_30m                  as wait_n,
        round(wait_p50_s_30m, 1)    as wait_p50_s,
        round(wait_p90_s_30m, 1)    as wait_p90_s,
        open_requests,
        open_request_max_age_s
    from iceberg_scan('{minute_log}')
    where provider = $provider and as_of_utc = $moment_utc
),

zones as (
    select zone_id, zone_name, borough
    from read_parquet('{zones}')
    where not is_unknown and not is_outside_nyc
),

holidays as (
    select holiday_date from read_parquet('{holidays}')
),

-- The same weekday and hour, 1 to 6 weeks back, holidays left out.
past as (
    select h.*
    from read_parquet({zone_hour_files}) h
    where h.provider = $provider
      and h.hour_of_day_local = $hour
      and h.date_local in (select $local_date - (7 * k)::INTEGER from range(1, 7) t(k))
      and h.date_local not in (select holiday_date from holidays)
),

normal as (
    select
        zone_id,
        count(distinct date_local)                                              as weeks_n,
        count(wait_p50_s) filter (where not is_low_sample)                      as wait_weeks_n,
        round(quantile_cont(wait_p50_s, 0.25) filter (where not is_low_sample), 1) as normal_p25_s,
        round(quantile_cont(wait_p50_s, 0.50) filter (where not is_low_sample), 1) as normal_p50_s,
        round(quantile_cont(wait_p50_s, 0.75) filter (where not is_low_sample), 1) as normal_p75_s,
        round(median(requests), 1)                                              as normal_requests_per_hour
    from past
    group by zone_id
),

joined as (
    select
        z.zone_id, z.zone_name, z.borough,
        l.requests, l.wait_n, l.wait_p50_s, l.wait_p90_s,
        l.open_requests, l.open_request_max_age_s,
        coalesce(n.weeks_n, 0)       as weeks_n,
        coalesce(n.wait_weeks_n, 0)  as wait_weeks_n,
        n.normal_p25_s, n.normal_p50_s, n.normal_p75_s,
        round(n.normal_requests_per_hour / 2, 1) as expected_requests_30m,
        coalesce(n.wait_weeks_n, 0) < 3          as is_baseline_weak,
        case when l.wait_p50_s is not null and n.normal_p50_s > 0
             then round(l.wait_p50_s / n.normal_p50_s, 2) end as wait_ratio
    from zones z
    left join live l using (zone_id)
    left join normal n using (zone_id)
),

-- The zone's own hourly ratios in the 8 weeks before the moment.
history as (
    select zone_id, wait_ratio
    from read_parquet({ratio_files})
    where provider = $provider
      and wait_ratio is not null
      and hour_utc >= $moment_utc - interval 56 day
      and hour_utc <  $moment_utc
),

ranked as (
    select
        j.zone_id,
        count(h.wait_ratio) as history_hours_n,
        100.0 * count(*) filter (where h.wait_ratio < j.wait_ratio)
              / nullif(count(h.wait_ratio), 0) as pctile
    from joined j
    left join history h using (zone_id)
    where j.wait_ratio is not null
    group by j.zone_id
)

select
    j.*,
    r.history_hours_n,
    case when r.history_hours_n >= 200 then round(r.pctile, 1) end as wait_ratio_pctile,
    -- why a zone is or is not ranked, in the API's order of checks
    case
        when j.wait_p50_s is null       then 'no_activity'
        when j.wait_n < 10              then 'low_sample'
        when j.is_baseline_weak or j.normal_p50_s is null then 'weak_baseline'
        else 'scored'
    end as state
from joined j
left join ranked r using (zone_id)
order by j.zone_id
