{{
  config(
    materialized='table',
    schema='silver',
    comment='Silver layer titles table'
  )
}}


with src as (
  select * from {{ source('hr_staging', 'titles') }}
)
select
  title_id,
  title
from src
