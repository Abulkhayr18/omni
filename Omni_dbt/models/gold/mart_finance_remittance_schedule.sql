-- mart_finance_remittance_schedule.sql
-- What is owed to each statutory body and by when
-- FIRS, Pension Fund Administrators, NHF, NHIS


{{ config(materialized='table', file_format='delta', schema='gold') }}

SELECT
    payroll_run_month,

    -- PAYE remittance to FIRS (due by 10th of following month)
    ROUND(SUM(paye_monthly), 2)                         AS paye_to_remit,
    DATE_ADD(
        LAST_DAY(TO_DATE(payroll_run_month, 'yyyy_MM')), 10
    )                                                   AS paye_due_date,
    'Federal Inland Revenue Service (FIRS)'             AS paye_remit_to,

    -- Employee pension remittance to PFA (due within 7 days of payday)
    ROUND(SUM(employee_pension), 2)                     AS employee_pension_to_remit,
    ROUND(SUM(employer_pension), 2)                     AS employer_pension_to_remit,
    ROUND(SUM(employee_pension) + SUM(employer_pension), 2) AS total_pension_to_remit,
    DATE_ADD(
        LAST_DAY(TO_DATE(payroll_run_month, 'yyyy_MM')), 7
    )                                                   AS pension_due_date,
    'Pension Fund Administrator (PFA)'                  AS pension_remit_to,

    -- NHF remittance to FMBN (due by end of following month)
    ROUND(SUM(nhf_deduction), 2)                        AS nhf_to_remit,
    LAST_DAY(
        ADD_MONTHS(TO_DATE(payroll_run_month, 'yyyy_MM'), 1)
    )                                                   AS nhf_due_date,
    'Federal Mortgage Bank of Nigeria (FMBN)'           AS nhf_remit_to,

    -- NHIS remittance
    ROUND(SUM(nhis_deduction), 2)                       AS nhis_to_remit,
    DATE_ADD(
        LAST_DAY(TO_DATE(payroll_run_month, 'yyyy_MM')), 10
    )                                                   AS nhis_due_date,
    'National Health Insurance Scheme (NHIS)'           AS nhis_remit_to

FROM {{ ref('stg_hr_payroll_runs') }}
WHERE payout_blocked = false
GROUP BY payroll_run_month