with eviction_type as (
    select * from {{ ref('eviction_dim_eviction_type') }}
),


location as (
    select * from {{ ref('homeless_encampment_eviction_dim_location') }}
),

date_dim as (
    select * from {{ ref('dim_dates') }}
),

evictions as (
    select *
    from {{ref ('evictions_data') }}
),


join_tbl as (
    select location.location_dim_id, eviction_type.evictions_type_id,date_dim.date_dim_id 
    from evictions
    LEFT JOIN location on 
        # eviction_address
        ((evictions.eviction_address = location.eviction_address)
        or (evictions.eviction_address is null and location.eviction_address is null))
        # borough
        AND ((evictions.borough = location.borough)
        or (evictions.borough is null and location.borough is null))
        # eviction_location_type,
        AND ((evictions.eviction_location_type = location.eviction_location_type)
        or (evictions.eviction_location_type is null and location.eviction_location_type is null))
        # eviction_post_code,
        AND ((evictions.eviction_post_code = location.eviction_post_code)
        or (evictions.eviction_post_code is null and location.eviction_post_code is null))
        # apartment_number,
        AND ((evictions.apartment_number = location.apartment_number)
        or (evictions.apartment_number is null and location.apartment_number is null))
        # latitude,
        AND ((evictions.latitude = location.latitude)
        or (evictions.latitude is null and location.latitude is null))
        # longitude
        AND ((evictions.longitude = location.longitude)
        or (evictions.longitude is null and location.longitude is null))

    

    LEFT JOIN eviction_type on 
        # ejectment,
        ((evictions.ejectment = eviction_type.ejectment)
        or (evictions.ejectment is null and eviction_type.ejectment is null))
        # eviction_type,
        AND ((evictions.eviction_type = eviction_type.eviction_type)
        or (evictions.eviction_type is null and eviction_type.eviction_type is null))
        # docket_index_number,
        AND ((evictions.docket_index_number = eviction_type.docket_index_number)
        or (evictions.docket_index_number is null and eviction_type.docket_index_number is null))
        # docket_number
        AND ((evictions.docket_number = eviction_type.docket_number)
        or (evictions.docket_number is null and eviction_type.docket_number is null))

    LEFT JOIN date_dim on 
        # ejectment,
        ((evictions.executed_date = date_dim.date_value)
        or (evictions.executed_date is null and date_dim.date_value is null))
)

SELECT row_number() over () as unique_key_DD, *
from join_tbl

