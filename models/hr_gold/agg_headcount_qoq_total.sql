-- ============================================================================
-- agg_headcount_qoq_total.sql
-- Company-wide Quarter-over-Quarter Headcount Comparison
-- Uses fct_employee_snapshot and fct_employee_turnover
-- ============================================================================
{{
  config(
    materialized='view',
    schema='gold',
    comment='Company-wide quarterly headcount comparison'
  )
}}

WITH current_quarter_dates AS (
  SELECT
    YEAR(CURRENT_DATE()) AS current_year,
    QUARTER(CURRENT_DATE()) AS current_quarter,
    YEAR(DATE_ADD(QUARTER, -1, CURRENT_DATE())) AS previous_year,
    QUARTER(DATE_ADD(QUARTER, -1, CURRENT_DATE())) AS previous_quarter
),

current_headcount AS (
  SELECT
    COUNT(DISTINCT emp_no) AS total_headcount_current_quarter
  FROM {{ ref('fct_employee_snapshot') }}
  WHERE employment_status = 'Active'
),

previous_quarter_departures AS (
  SELECT
    COUNT(DISTINCT emp_no) AS employees_departed_last_quarter
  FROM {{ ref('fct_employee_turnover') }}  t
  CROSS JOIN current_quarter_dates cqd
  WHERE t.exit_year = cqd.previous_year
    AND QUARTER(TO_DATE(CONCAT(t.exit_year, '-', LPAD(t.exit_month, 2, '0'), '-01'))) = cqd.previous_quarter
),

previous_quarter_hires AS (
  SELECT
    COUNT(DISTINCT emp_no) AS employees_hired_last_quarter
  FROM {{ ref('fct_employee_snapshot') }} e
  CROSS JOIN current_quarter_dates cqd
  WHERE YEAR(e.hire_date) = cqd.previous_year
    AND QUARTER(e.hire_date) = cqd.previous_quarter
)

SELECT
  cqd.previous_year AS previous_year,
  cqd.previous_quarter AS previous_quarter,
  cqd.current_year AS current_year,
  cqd.current_quarter AS current_quarter,
  CONCAT('Q', cqd.previous_quarter, ' ', cqd.previous_year) AS previous_quarter_label,
  CONCAT('Q', cqd.current_quarter, ' ', cqd.current_year) AS current_quarter_label,
  ch.total_headcount_current_quarter 
    + pqd.employees_departed_last_quarter
    - pqh.employees_hired_last_quarter AS total_headcount_previous_quarter,
  ch.total_headcount_current_quarter,
  ch.total_headcount_current_quarter 
    - (ch.total_headcount_current_quarter + pqd.employees_departed_last_quarter - pqh.employees_hired_last_quarter) AS headcount_change,
  ROUND(
    100.0 * (ch.total_headcount_current_quarter - (ch.total_headcount_current_quarter + pqd.employees_departed_last_quarter - pqh.employees_hired_last_quarter)) / 
    NULLIF(ch.total_headcount_current_quarter + pqd.employees_departed_last_quarter - pqh.employees_hired_last_quarter, 0),
    2
  ) AS headcount_change_pct,
  CASE
    WHEN ch.total_headcount_current_quarter > (ch.total_headcount_current_quarter + pqd.employees_departed_last_quarter - pqh.employees_hired_last_quarter) THEN 'Growth'
    WHEN ch.total_headcount_current_quarter < (ch.total_headcount_current_quarter + pqd.employees_departed_last_quarter - pqh.employees_hired_last_quarter) THEN 'Decline'
    ELSE 'Stable'
  END AS headcount_trend,
  pqh.employees_hired_last_quarter,
  pqd.employees_departed_last_quarter,
  CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM current_quarter_dates cqd, current_headcount ch, previous_quarter_departures pqd, previous_quarter_hires pqh