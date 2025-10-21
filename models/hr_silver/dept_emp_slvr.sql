{{
  config(
    materialized='table',
    schema='silver',
    comment='Silver layer dept_emp table'
  )
}}


with src as (
  select * from {{ source('hr_staging', 'dept_emp') }}
)
select
  emp_no,
  dept_no
from src