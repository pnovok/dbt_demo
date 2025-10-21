-- ============================================================================
-- agg_ytd_leavers_summary.sql
-- Year-to-Date Number of Leavers Summary
-- Uses fct_employee_turnover
-- ============================================================================

{{
  config(
    materialized='view',
    schema='gold',
    comment='Year-to-date leavers total count'
  )
}}

WITH ytd_leavers AS (
  SELECT
    COUNT(DISTINCT emp_no) AS total_leavers_ytd,
    ROUND(AVG(tenure_years), 2) AS avg_tenure_years_leavers,
    ROUND(AVG(age_at_departure), 1) AS avg_age_at_departure
  FROM {{ ref('fct_employee_turnover') }}
  WHERE exit_year = YEAR(CURRENT_DATE())
    AND exit_date IS NOT NULL
),

current_employees AS (
  SELECT
    COUNT(DISTINCT emp_no) AS total_active_employees
  FROM {{ ref('fct_employee_snapshot') }}
  WHERE employment_status = 'Active'
)

SELECT
  YEAR(CURRENT_DATE()) AS year,
  ytd.total_leavers_ytd,
  ce.total_active_employees,
  ytd.avg_tenure_years_leavers,
  ytd.avg_age_at_departure,
  ROUND(
    100.0 * ytd.total_leavers_ytd / (ytd.total_leavers_ytd + ce.total_active_employees),
    2
  ) AS annual_fluctuation_rate_pct,
  CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM ytd_leavers ytd, current_employees ce