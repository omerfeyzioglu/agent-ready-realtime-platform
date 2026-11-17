-- 'all' must equal uber + lyft for every zone and hour.
select hour_utc, zone_id
from {{ ref('feat_zone_hour') }}
group by all
having sum(requests) filter (where provider = 'all')
        is distinct from sum(requests) filter (where provider <> 'all')
    or sum(dropoffs_in) filter (where provider = 'all')
        is distinct from sum(dropoffs_in) filter (where provider <> 'all')
