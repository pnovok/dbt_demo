{{
  config(
    materialized='table',
    schema='silver',
    comment='Silver layer dept_manager table'
  )
}}


with src as (
  select * from {{ source('hr_staging', 'dept_manager') }}
)
select
  dept_no,
  emp_no
from src