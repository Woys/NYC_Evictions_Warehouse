with evictions_locations as (
    SELECT DISTINCT
    ejectment,
    eviction_type,
    docket_index_number,
    docket_number

    from {{ref ('evictions_data') }}
),
evictions_dim as (
    select *
    FROM evictions_locations)

SELECT row_number() over () as evictions_type_id, *
from evictions_dim