-- mart_hr_paye_detail.sql
-- One row per employee — maps directly to Excel template
-- Power Query connects here for live refresh

{{ config(materialized='table', file_format='delta', schema='gold') }}

SELECT
    employee_id,
    full_name,
    department,
    job_grade,
    gross_monthly_ngn                       AS basic_salary,
    allowances,
    gross_salary,
    employee_pension,
    employer_pension,
    nhf_deduction                           AS nhf,
    nhis_deduction                          AS nhis,
    paye_monthly                            AS paye_tax,
    total_deductions,
    net_salary_payable                      AS net_salary,
    payout_blocked,
    block_reason,
    fx_rate_used,
    fx_is_fallback,
    payroll_run_month,
    calculated_at
FROM {{ ref('stg_hr_payroll_runs') }}
ORDER BY department, employee_id