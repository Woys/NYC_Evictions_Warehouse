with evictions_locations as (
    SELECT DISTINCT 

    incident_address,
    incident_zip,
    location_type,
    street_name,
    address_type,
    city,
    landmark,
    borough, 

    longitude,
    latitude,

    from {{ref ('homeless_encampment_data') }}
),
evictions_dim as (
    SELECT DISTINCT *
    FROM evictions_locations)

SELECT row_number() over () as location_dim_id, *
from evictions_dim