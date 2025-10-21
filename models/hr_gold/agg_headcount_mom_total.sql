
-- ============================================================================
-- agg_headcount_mom_total.sql
-- Company-wide Month-over-Month Headcount Comparison
-- Uses fct_employee_snapshot and fct_employee_turnover
-- ============================================================================
{{
  config(
    materialized='view',
    schema='gold',
    file_format='delta',
    comment='Company-wide monthly headcount comparison'
  )
}}

WITH current_headcount AS (
  SELECT
    COUNT(DISTINCT emp_no) AS total_headcount_current_month
  FROM {{ ref('fct_employee_snapshot') }}
  WHERE employment_status = 'Active'
),

previous_month_departures AS (
  SELECT
    COUNT(DISTINCT emp_no) AS employees_departed_last_month
  FROM {{ ref('fct_employee_turnover') }} 
  WHERE exit_year = YEAR(DATE_ADD(MONTH, -1, CURRENT_DATE()))
    AND exit_month = MONTH(DATE_ADD(MONTH, -1, CURRENT_DATE()))
),

previous_month_hires AS (
  SELECT
    COUNT(DISTINCT emp_no) AS employees_hired_last_month
  FROM {{ ref('fct_employee_snapshot') }}
  WHERE YEAR(hire_date) = YEAR(DATE_ADD(MONTH, -1, CURRENT_DATE()))
    AND MONTH(hire_date) = MONTH(DATE_ADD(MONTH, -1, CURRENT_DATE()))
)

SELECT
  MONTH(DATE_ADD(MONTH, -1, CURRENT_DATE())) AS previous_month,
  YEAR(DATE_ADD(MONTH, -1, CURRENT_DATE())) AS previous_year,
  MONTH(CURRENT_DATE()) AS current_month,
  YEAR(CURRENT_DATE()) AS current_year,
  ch.total_headcount_current_month 
    + pmd.employees_departed_last_month
    - pmh.employees_hired_last_month AS total_headcount_previous_month,
  ch.total_headcount_current_month,
  ch.total_headcount_current_month 
    - (ch.total_headcount_current_month + pmd.employees_departed_last_month - pmh.employees_hired_last_month) AS headcount_change,
  ROUND(
    100.0 * (ch.total_headcount_current_month - (ch.total_headcount_current_month + pmd.employees_departed_last_month - pmh.employees_hired_last_month)) / 
    NULLIF(ch.total_headcount_current_month + pmd.employees_departed_last_month - pmh.employees_hired_last_month, 0),
    2
  ) AS headcount_change_pct,
  CASE
    WHEN ch.total_headcount_current_month > (ch.total_headcount_current_month + pmd.employees_departed_last_month - pmh.employees_hired_last_month) THEN 'Growth'
    WHEN ch.total_headcount_current_month < (ch.total_headcount_current_month + pmd.employees_departed_last_month - pmh.employees_hired_last_month) THEN 'Decline'
    ELSE 'Stable'
  END AS headcount_trend,
  pmh.employees_hired_last_month,
  pmd.employees_departed_last_month,
  CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM current_headcount ch, previous_month_departures pmd, previous_month_hires pmh