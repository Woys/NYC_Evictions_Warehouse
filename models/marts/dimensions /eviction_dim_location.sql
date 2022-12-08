with evictions_locations as (
    SELECT DISTINCT eviction_address,
    borough,
    eviction_location_type,
    eviction_post_code,
    apartment_number,
    latitude,
    longitude

    from {{ref ('evictions_data') }}
),
evictions_dim as (
    SELECT 
    DISTINCT COALESCE(eviction_address, 'Not Available') as eviction_address,
    borough,
    eviction_location_type,
    eviction_post_code,
    apartment_number,
    latitude,
    longitude
    FROM evictions_locations)

SELECT row_number() over () as location_dim_id, *
from evictions_dim