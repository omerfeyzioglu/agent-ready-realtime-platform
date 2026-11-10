{{ config(location=staging_root() ~ '/zones/data.parquet') }}

-- TLC taxi zones. 264 and 265 are placeholders, not places.

select
    LocationID::integer as zone_id,
    Borough             as borough,
    Zone                as zone_name,
    service_zone,
    {{ zone_unknown('LocationID') }}     as is_unknown,
    {{ zone_outside_nyc('LocationID') }} as is_outside_nyc
from {{ source('source_files', 'taxi_zone_lookup') }}
