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
    FROM evictions_locations
),

homeless_encampment_locations as (
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
homeless_encampment_dim as (
    SELECT DISTINCT *
    FROM homeless_encampment_locations
),


union_dims as(

SELECT
    longitude,
    latitude,

    apartment_number,
    null as street_name,

    eviction_post_code,
    null as incident_zip,
    null as incident_address,
    eviction_address,
    null as location_type,
    eviction_location_type,
    null as address_type,
    null as landmark,
    borough,
    null as city
from evictions_dim

union all

SELECT
    longitude,
    latitude,

    null as apartment_number,
    street_name,

    null as eviction_post_code,
    incident_zip,
    incident_address,
    null as eviction_address,
    location_type,
    null as eviction_location_type,
    address_type,
    landmark,
    borough,
    city

from homeless_encampment_dim

),

final as (
    select DISTINCT 
    longitude,
    latitude,

    COALESCE(apartment_number, 'Not Available') as apartment_number,
    COALESCE(street_name, 'Not Available') as street_name,

    COALESCE(eviction_post_code, 0) AS eviction_post_code,
    COALESCE(incident_zip, 0) AS incident_zip,
    COALESCE(incident_address, 'Not Available') as incident_address,
    COALESCE(eviction_address, 'Not Available') as eviction_address,
    COALESCE(location_type, 'Not Available') as location_type,
    COALESCE(eviction_location_type, 'Not Available') as eviction_location_type,
    COALESCE(address_type, 'Not Available') as address_type,
    COALESCE(landmark, 'Not Available') as landmark,
    COALESCE(borough, 'Not Available') as borough,
    COALESCE(city, 'Not Available') as city,
from union_dims)


SELECT row_number() over () as location_dim_id, *
from final