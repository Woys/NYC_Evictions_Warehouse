{{ config (
    materialized="table"
)}}

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2017-01-03' as date)",
    end_date="cast('2017-01-03' as date)"
   )
}}