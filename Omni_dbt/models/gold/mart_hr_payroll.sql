-- mart_hr_payroll.sql
-- Headcount, payroll cost and attrition by department

{{ config(materialized='table', file_format='delta', schema='gold') }}

SELECT
    p.department,
    COUNT(DISTINCT p.employee_id)               AS headcount,
    ROUND(SUM(p.gross_salary), 2)               AS total_gross_payroll,
    ROUND(SUM(p.net_salary_payable), 2)         AS total_net_payable,
    ROUND(AVG(p.net_salary_payable), 2)         AS avg_net_salary,
    ROUND(SUM(p.paye_monthly), 2)               AS total_paye,
    SUM(CASE WHEN e.attrition_status = 'Yes'
        THEN 1 ELSE 0 END)                      AS attrition_count,
    SUM(CASE WHEN p.payout_blocked = true
        THEN 1 ELSE 0 END)                      AS blocked_payouts
FROM {{ ref('stg_hr_payroll_runs') }} p
LEFT JOIN {{ ref('stg_hr_employees') }} e
    ON p.employee_id = e.employee_id
GROUP BY p.department