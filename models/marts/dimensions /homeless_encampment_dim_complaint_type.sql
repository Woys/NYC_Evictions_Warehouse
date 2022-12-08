with evictions_locations as (
    SELECT DISTINCT 

    complaint_type

    from {{ref ('homeless_encampment_data') }}
),
evictions_dim as (
    SELECT DISTINCT *
    FROM evictions_locations)

SELECT row_number() over () as complaint_type_id, *
from evictions_dim