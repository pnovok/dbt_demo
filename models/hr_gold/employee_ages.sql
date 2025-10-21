-- ============================================================================
--  employee_ages.sql
-- ============================================================================

{{
  config(
    materialized='table',
    schema='gold',
    comment='Employee Ages Distribution'
  )
}}

  SELECT
    emp_no,
    YEAR(TO_DATE(birth_date)) AS birth_year,
    YEAR(CURRENT_DATE()) - YEAR(TO_DATE(birth_date)) AS age_at_departure,
    CASE
      WHEN YEAR(CURRENT_DATE()) - YEAR(TO_DATE(birth_date)) < 25 THEN 'Gen Z (< 25)'
      WHEN YEAR(CURRENT_DATE()) - YEAR(TO_DATE(birth_date)) BETWEEN 25 AND 40 THEN 'Millennials (25-40)'
      WHEN YEAR(CURRENT_DATE()) - YEAR(TO_DATE(birth_date)) BETWEEN 41 AND 56 THEN 'Gen X (41-56)'
      ELSE 'Baby Boomers (> 56)'
    END AS generation
  FROM  {{ ref('employee_slvr') }} 
