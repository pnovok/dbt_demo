{{
  config(
    materialized='table',
    schema='silver',
    comment='Silver layer departures table'
  )
}}


with src as (
  select * from {{ source('hr_staging', 'departures') }}
)
select
  emp_no,
   cast(
    CASE
        WHEN to_date(
            exit_date,
            'M/d/yy'
        ) > current_date()
        THEN date_add(
            YEAR,
            -100,
            to_date(
                exit_date,
                'M/d/yy'
            )
        )
        ELSE to_date(
            exit_date,
            'M/d/yy'
        )
    END  as date) as exit_date,
  exit_reason
from src
WHERE
    emp_no IS NOT NULL