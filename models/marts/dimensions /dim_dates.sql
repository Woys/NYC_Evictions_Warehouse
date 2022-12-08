{{ config (
    materialized="table"
)}}

with dates_dim as (
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2017-01-03' as date)",
    end_date="cast('2022-12-06' as date)"
   )
}})

select row_number() OVER () AS date_dim_id,
    date_day as date_value,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(YEAR FROM date_day) AS year
from dates_dim
order by 1

