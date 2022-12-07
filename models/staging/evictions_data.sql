with stg_evictions as (
select *
from {{ ref('stg_evictions') }})

select *
from stg_evictions

