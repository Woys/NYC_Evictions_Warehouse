/*
orders as (

    select * from {{ ref('stg_orders') }}

),

with evictions as (
    select *
    from {{ref ('evictions_data') }}
),

join_tbl as (
    select evictions.*
    from evictions
    LEFT JOIN eviction_dim_location on evictions.eviction_address = eviction_dim_location.eviction_address
)
select *
from evictions
*/



    select * from {{ ref('evictions_data') }}

