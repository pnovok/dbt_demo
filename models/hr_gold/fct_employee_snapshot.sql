-- ============================================================================
-- fct_employee_snapshot.sql
-- ============================================================================
{{
  config(
    materialized='table',
    schema='gold',
    comment='Current employee snapshot with demographics, department, salary, and employment status'
  )
}}

SELECT
    e.emp_no,
    e.first_name,
    e.last_name,
    e.sex,
    ea.generation,
    TO_DATE(e.hire_date) AS hire_date,
    YEAR(TO_DATE(e.hire_date)) AS hire_year,
    ROUND(CAST(
        DATEDIFF(CURRENT_DATE(), TO_DATE(e.hire_date))
        AS DECIMAL(10, 2)
    ) / 365.25, 0) AS tenure_years,
    de.dept_no,
    dept.dept_name,
    sal.salary,
    CASE WHEN dc.emp_no IS NOT NULL THEN 'Departed' ELSE 'Active' END AS employment_status
FROM  {{ ref('employee_slvr') }} e
LEFT JOIN{{ ref('dept_emp_slvr') }} de ON e.emp_no = de.emp_no
LEFT JOIN {{ ref('departments_slvr') }} dept ON de.dept_no = dept.dept_no
LEFT JOIN {{ ref('employee_ages') }} ea ON e.emp_no = ea.emp_no
LEFT JOIN {{ ref('salaries_slvr') }} sal ON e.emp_no = sal.emp_no
LEFT JOIN {{ ref('departures_slvr') }} dc ON e.emp_no = dc.emp_no
