WITH evictions AS (
    SELECT 
    int64_field_0 as eviction_id,

    cast(executed_date as date) as executed_date,


    eviction_address,
    borough,
    residential_commercial_ind as eviction_location_type,
    eviction_zip as eviction_post_code,
    #eviction_apt_num as apartment_number, # Many nuuls!!
    COALESCE(eviction_apt_num, 'Not Available') as apartment_number,
    latitude,
    longitude,

    ejectment,
    eviction_possession as eviction_type,
    court_index_number as docket_index_number,
    docket_number
    from {{ source('NYC_complaints', 'evictions') }}

    where
        int64_field_0 IS NOT NULL
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
    limit 10 
)

SELECT * FROM evictions