with homeless_encampment as (
select *
from {{ ref('stg_homeless_encampment') }})

select *
from homeless_encampment

