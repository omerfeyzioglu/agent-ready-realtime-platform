-- The baseline must have exactly the rows of feat_zone_hour for the month:
-- no row lost in the join, none multiplied.
with b as (select hour_utc, zone_id, provider, count(*) as n from {{ ref('feat_zone_hour_baseline') }} group by all),
f as (
    select hour_utc, zone_id, provider
    from read_parquet('{{ feature_location("zone_hour", "trip_month", var("month")) }}')
)
select * from f full outer join b using (hour_utc, zone_id, provider)
where b.n is distinct from 1 or f.hour_utc is null
