{{
  config(
    materialized='table',
    schema='silver',
    comment='Silver layer salaries table'
  )
}}

with src as (
  select * from {{ source('hr_staging', 'salaries') }}
)
select
  emp_no,
  salary
from src
where emp_no is not NULL