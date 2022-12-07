WITH homeless_person_pessistancet AS (
    SELECT 

    int64_field_0 AS homeless_person_pessistancet_id,

    cast(created_date as date) as created_date,

    incident_address,
    incident_zip
    location_type,
    street_name,
    address_type,
    city,
    landmark, # a lot of null!!
    borough, # avoid unsepcified type!!!

    longitude,
    latitude,

    descriptor, # null!!
    complaint_type
    
    from {{ source('NYC_complaints', 'homeless_person_pessistancet') }}
    where 

        location_type is not null 
        and incident_address is not null 
        and street_name is not null 
        and city is not null 
        and longitude is not null 
        and latitude is not null

    limit 10 

)

SELECT * FROM homeless_person_pessistancet