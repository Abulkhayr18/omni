-- stg_hr_employees.sql
-- Cleaned employee records

{{ config(materialized='view', schema='silver') }}

SELECT
    EmployeeNumber                          AS employee_id,
    CONCAT(JobRole, ' #', EmployeeNumber)   AS full_name,
    Department                              AS department,
    JobLevel                                AS job_grade,
    MonthlyIncome                           AS monthly_income_usd,
    Attrition                               AS attrition_status,
    YearsAtCompany                          AS tenure_years,
    ingested_at
FROM {{ source('bronze', 'hr_employees_raw') }}
WHERE EmployeeNumber IS NOT NULL
  AND MonthlyIncome  IS NOT NULL