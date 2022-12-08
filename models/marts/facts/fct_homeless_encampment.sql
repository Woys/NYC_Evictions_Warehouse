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

    select date_dim.date_dim_id, complaint_type.complaint_type_id, encampment.* 
    from encampment

    LEFT JOIN date_dim on 
        # ejectment,
        ((encampment.created_date = date_dim.date_value)
        or (encampment.created_date is null and date_dim.date_value is null))

    LEFT JOIN complaint_type on 
        # complaint_type,
        ((encampment.complaint_type = complaint_type.complaint_type)
        or (encampment.complaint_type is null and complaint_type.complaint_type is null))
    
    )



SELECT row_number() over () as unique_key, *
from join_tbl