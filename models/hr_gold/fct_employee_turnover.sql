-- ============================================================================
--  fct_employee_turnover.sql
-- ============================================================================

{{
  config(
    materialized='table',
    schema='gold',
    partition_by=['exit_year'],
    comment='Fact table tracking employee departures with demographics, tenure, and exit reasons'
  )
}}
 

SELECT
      d.emp_no,
      e.first_name,
      e.last_name,
      e.sex,
      ea.age_at_departure,
      ea.generation,
      TO_DATE(d.exit_date) AS exit_date,
      YEAR(d.exit_date) AS exit_year,
      MONTH(d.exit_date) AS exit_month,
      d.exit_reason,
      de.dept_no,
      dept.dept_name,
      ROUND(
          CAST(
              DATEDIFF(
                  TO_DATE(d.exit_date),
                  TO_DATE(e.hire_date)
              ) AS DECIMAL(10, 2)
          ) / 365.25, 0
      ) AS tenure_years
    FROM {{ ref('departures_slvr') }} d
    LEFT JOIN  {{ ref('employee_slvr') }} e ON d.emp_no = e.emp_no
    LEFT JOIN {{ ref('employee_ages') }} ea ON e.emp_no = ea.emp_no
    LEFT JOIN {{ ref('dept_emp_slvr') }} de ON d.emp_no = de.emp_no
    LEFT JOIN {{ ref('departments_slvr') }}  dept ON de.dept_no = dept.dept_no