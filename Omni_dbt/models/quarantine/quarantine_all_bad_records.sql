-- quarantine_all_bad_records.sql
-- All rejected and blocked records with reason and timestamp

{{ config(materialized='table', file_format='delta', schema='quarantine') }}

-- Missing employee data
SELECT
    'hr_employees'                          AS domain,
    CAST(EmployeeNumber AS STRING)          AS record_id,
    'null_employee_id_or_income'            AS rejection_reason,
    ingested_at                             AS rejected_at
FROM {{ source('bronze', 'hr_employees_raw') }}
WHERE EmployeeNumber IS NULL
   OR MonthlyIncome  IS NULL

UNION ALL

-- Invalid salary or tax calculation
SELECT
    'hr_payroll'                            AS domain,
    CAST(EmployeeNumber AS STRING)          AS record_id,
    'invalid_salary_or_tax'                 AS rejection_reason,
    calculated_at                           AS rejected_at
FROM {{ source('bronze', 'hr_payroll_runs') }}
WHERE gross_salary <= 0
   OR paye_monthly  < 0

UNION ALL

-- HR-driven payout blocks
SELECT
    'hr_payroll_blocked'                    AS domain,
    CAST(EmployeeNumber AS STRING)          AS record_id,
    block_reason                            AS rejection_reason,
    calculated_at                           AS rejected_at
FROM {{ source('bronze', 'hr_payroll_runs') }}
WHERE payout_blocked = true

UNION ALL

-- Bad FX rates
SELECT
    'market'                                        AS domain,
    CONCAT(from_currency, '_', to_currency)         AS record_id,
    'zero_or_null_rate'                             AS rejection_reason,
    ingested_at                                     AS rejected_at
FROM {{ source('bronze', 'market_exchange_rates_raw') }}
WHERE rate IS NULL
   OR rate <= 0