-- stg_hr_payroll_runs.sql
-- Validated payroll rows with block reason carried through

{{ config(materialized='view', schema='silver') }}

SELECT
    EmployeeNumber                          AS employee_id,
    CONCAT(JobRole, ' #', EmployeeNumber)   AS full_name,
    Department                              AS department,
    JobLevel                                AS job_grade,
    gross_monthly_ngn,
    allowances,
    gross_salary,
    employee_pension,
    employer_pension,
    nhf_deduction,
    nhis_deduction,
    paye_monthly,
    total_deductions,
    net_salary,
    net_salary_payable,
    payout_blocked,
    block_reason,
    budget_cap,
    fx_rate_used,
    fx_is_fallback,
    payroll_run_month,
    calculated_at
FROM {{ source('bronze', 'hr_payroll_runs') }}
WHERE gross_salary  > 0
  AND paye_monthly >= 0