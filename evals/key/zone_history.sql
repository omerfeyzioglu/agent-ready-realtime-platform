-- Answer key: how a set of zones developed before a frozen moment, every 15
-- minutes, from the minute log (rolling 30-minute windows ending at each
-- point). Used for "why did it jump", "since when" and premise checks.
--   $provider, $zone_ids (list), $from_utc, $to_utc

select
    as_of_utc,
    zone_id,
    requests_30m              as requests_last_30m,
    wait_n_30m                as wait_n_last_30m,
    round(wait_p50_s_30m, 1)  as wait_p50_s,
    open_requests,
    open_request_max_age_s
from iceberg_scan('{minute_log}')
where provider = $provider
  and list_contains($zone_ids, zone_id)
  and as_of_utc between $from_utc and $to_utc
  and minute(as_of_utc) % 15 = 0
order by zone_id, as_of_utc
