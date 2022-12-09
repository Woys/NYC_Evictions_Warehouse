with complaint_type as (
    select * from {{ ref('homeless_encampment_dim_complaint_type') }}
),


location as (
    select * from {{ ref('homeless_encampment_dim_location') }}
),

date_dim as (
    select * from {{ ref('dim_dates') }}
),

encampment as (
    select *
    from {{ref ('homeless_encampment_data') }}
),


join_tbl as (

    select date_dim.date_dim_id, complaint_type.complaint_type_id, location.location_dim_id #, encampment.* 
    from encampment

    LEFT JOIN date_dim on 
        # ejectment,
        ((encampment.created_date = date_dim.date_value)
        or (encampment.created_date is null and date_dim.date_value is null))

    LEFT JOIN complaint_type on 
        # complaint_type,
        ((encampment.complaint_type = complaint_type.complaint_type)
        or (encampment.complaint_type is null and complaint_type.complaint_type is null))

    LEFT JOIN location on 
        #incident_address,
        ((encampment.incident_address = location.incident_address)
        or (encampment.incident_address is null and location.incident_address is null))
        #incident_zip,
        AND ((encampment.incident_zip = location.incident_zip)
        or (encampment.incident_zip is null and location.incident_zip is null))
        #location_type,
        AND ((encampment.location_type = location.location_type)
        or (encampment.location_type is null and location.location_type is null))
        #street_name,
        AND ((encampment.street_name = location.street_name)
        or (encampment.street_name is null and location.street_name is null))
        #address_type,
        AND ((encampment.address_type = location.address_type)
        or (encampment.address_type is null and location.address_type is null))
        #city,
        AND ((encampment.city = location.city)
        or (encampment.city is null and location.city is null))
        #landmark,
        AND ((encampment.landmark = location.landmark)
        or (encampment.landmark is null and location.landmark is null))
        #borough, 
        AND ((encampment.borough = location.borough)
        or (encampment.borough is null and location.borough is null))

        #longitude,
        AND ((encampment.longitude = location.longitude)
        or (encampment.longitude is null and location.longitude is null))
        #latitude,
        AND ((encampment.latitude = location.latitude)
        or (encampment.latitude is null and location.latitude is null))
)



SELECT row_number() over () as unique_key, *
from join_tbl


