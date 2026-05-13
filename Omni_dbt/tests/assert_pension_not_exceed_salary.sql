-- assert_pension_not_exceed_salary.sql
-- Pipeline halts and alerts if pension ever exceeds salary

SELECT
    employee_id,
    employee_pension,
    gross_salary
FROM {{ ref('stg_hr_payroll_runs') }}
WHERE employee_pension > gross_salary