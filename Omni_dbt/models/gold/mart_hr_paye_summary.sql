-- mart_hr_paye_summary.sql
-- PAYE, pension, NHF totals by department
-- Power BI dashboard source


{{ config(materialized='table', file_format='delta', schema='gold') }}

SELECT
    department,
    payroll_run_month,
    COUNT(DISTINCT employee_id)             AS employee_count,
    ROUND(SUM(paye_monthly), 2)             AS total_paye_collected,
    ROUND(SUM(employee_pension), 2)         AS total_employee_pension,
    ROUND(SUM(employer_pension), 2)         AS total_employer_pension,
    ROUND(SUM(nhf_deduction), 2)            AS total_nhf,
    ROUND(SUM(nhis_deduction), 2)           AS total_nhis,
    ROUND(SUM(total_deductions), 2)         AS total_deductions,
    ROUND(SUM(gross_salary), 2)             AS total_gross,
    ROUND(SUM(net_salary_payable), 2)       AS total_net_payable
FROM {{ ref('stg_hr_payroll_runs') }}
WHERE payout_blocked = false
GROUP BY department, payroll_run_month