WITH homeless_encampment AS (
    SELECT 

    int64_field_0 AS homeless_encampment_id,

    cast(created_date as date) as created_date, 

    incident_address,
    incident_zip,
    #COALESCE(CAST(incident_zip AS STRING), 'N/A') as incident_zip,
    location_type,
    street_name,
    COALESCE(address_type, 'Not Available') as address_type,
    city,
    COALESCE(landmark, 'Not Available') as landmark,
    borough, 

    longitude,
    latitude,
    complaint_type

    
    from {{ source('NYC_complaints', 'homeless_encampment') }}
    where 
         
        created_date BETWEEN '2017-01-03' AND '2021-07-10'

        and location_type is not null 
        and incident_zip is not null
        and incident_address is not null 
        and street_name is not null 
        and city is not null 
        and longitude is not null 
        and latitude is not null
        and borough !='Unspecified' and  borough is not NULL

    limit 20

)

SELECT * FROM homeless_encampment

