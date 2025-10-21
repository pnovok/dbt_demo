{{
  config(
    materialized='table',
    schema='silver',
    comment='Silver layer employee table'
  )
}}


with src as (
    select * from {{ source('hr_staging', 'employees') }}
)
select
    emp_no,
    cast(
    CASE
        WHEN to_date(
            hire_date,
            'M/d/yy'
        ) > current_date()
        THEN date_add(
            YEAR,
            -100,
            to_date(
                hire_date,
                'M/d/yy'
            )
        )
        ELSE to_date(
            hire_date,
            'M/d/yy'
        )
    END  as date) as hire_date,

    cast (
     CASE
        WHEN to_date(
            birth_date,
            'M/d/yy'
        ) > current_date()
        THEN date_add(
            YEAR,
            -100,
            to_date(
                birth_date,
                'M/d/yy'
            )
        )
        ELSE to_date(
            birth_date,
            'M/d/yy'
        )
    END as date) as birth_date,

    first_name,
    last_name,
    sex,
    emp_title_id
from src
    where emp_no is not NULL