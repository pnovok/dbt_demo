{{
  config(
    materialized='table',
    schema='silver',
    comment='Silver layer departments table'
  )
}}

-- models/your_model.sql


with src as (
  select * from {{ source('hr_staging', 'departments') }}
)
select
  dept_no,
  dept_name
from src