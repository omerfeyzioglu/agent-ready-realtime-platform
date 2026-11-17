-- Same rows as feat_zone_hour for the month: none lost, none multiplied.
with v as (select hour_utc, zone_id, provider, count(*) as n from {{ ref('feat_zone_hour_vs_baseline') }} group by all),
f as (
    select hour_utc, zone_id, provider
    from read_parquet('{{ feature_location("zone_hour", "trip_month", var("month")) }}')
)
select * from f full outer join v using (hour_utc, zone_id, provider)
where v.n is distinct from 1 or f.hour_utc is null
