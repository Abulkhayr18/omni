-- mart_finance_journal_entries.sql
-- Auto-generated double-entry bookkeeping from payroll run
-- Debit salary expense, credit bank and liability accounts


{{ config(materialized='table', file_format='delta', schema='gold') }}

-- Gross salary expense (debit)
SELECT
    payroll_run_month,
    department,
    'Salary Expense'                        AS account_name,
    'debit'                                 AS entry_type,
    ROUND(SUM(gross_salary), 2)             AS amount
FROM {{ ref('stg_hr_payroll_runs') }}
WHERE payout_blocked = false
GROUP BY payroll_run_month, department

UNION ALL

-- Net salary payable to employees (credit bank)
SELECT
    payroll_run_month,
    department,
    'Bank Account'                          AS account_name,
    'credit'                                AS entry_type,
    ROUND(SUM(net_salary_payable), 2)       AS amount
FROM {{ ref('stg_hr_payroll_runs') }}
WHERE payout_blocked = false
GROUP BY payroll_run_month, department

UNION ALL

-- PAYE liability (credit)
SELECT
    payroll_run_month,
    department,
    'PAYE Tax Payable'                      AS account_name,
    'credit'                                AS entry_type,
    ROUND(SUM(paye_monthly), 2)             AS amount
FROM {{ ref('stg_hr_payroll_runs') }}
WHERE payout_blocked = false
GROUP BY payroll_run_month, department

UNION ALL

-- Employee pension liability (credit)
SELECT
    payroll_run_month,
    department,
    'Employee Pension Payable'              AS account_name,
    'credit'                                AS entry_type,
    ROUND(SUM(employee_pension), 2)         AS amount
FROM {{ ref('stg_hr_payroll_runs') }}
WHERE payout_blocked = false
GROUP BY payroll_run_month, department

UNION ALL

-- Employer pension expense (debit)
SELECT
    payroll_run_month,
    department,
    'Employer Pension Expense'              AS account_name,
    'debit'                                 AS entry_type,
    ROUND(SUM(employer_pension), 2)         AS amount
FROM {{ ref('stg_hr_payroll_runs') }}
WHERE payout_blocked = false
GROUP BY payroll_run_month, department

UNION ALL

-- NHF liability (credit)
SELECT
    payroll_run_month,
    department,
    'NHF Payable'                           AS account_name,
    'credit'                                AS entry_type,
    ROUND(SUM(nhf_deduction), 2)            AS amount
FROM {{ ref('stg_hr_payroll_runs') }}
WHERE payout_blocked = false
GROUP BY payroll_run_month, department

UNION ALL

-- NHIS liability (credit)
SELECT
    payroll_run_month,
    department,
    'NHIS Payable'                          AS account_name,
    'credit'                                AS entry_type,
    ROUND(SUM(nhis_deduction), 2)           AS amount
FROM {{ ref('stg_hr_payroll_runs') }}
WHERE payout_blocked = false
GROUP BY payroll_run_month, department