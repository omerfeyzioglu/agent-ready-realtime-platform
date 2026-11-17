-- The grain must hold: one row per zone, hour and provider.
select hour_utc, zone_id, provider, count(*) as n
from {{ ref('feat_zone_hour') }}
group by all
having count(*) > 1
