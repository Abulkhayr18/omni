# Omni — HR and Finance Data Pipeline

An end-to-end data engineering platform that automates payroll processing, PAYE tax calculation, and financial reporting for a multi-department organisation. Built on a Medallion Architecture (Bronze → Silver → Gold) using real datasets and live market data.

---

## What This Project Does

The pipeline ingests IBM HR employee records, converts salaries from USD to NGN using live exchange rates, calculates Nigerian PAYE tax per employee, applies statutory deductions (Pension, NHF, NHIS), flags blocked payouts, and delivers audit-ready payroll data directly into an Excel template and Power BI dashboard.

---

## Stack

| Layer | Tool |
|---|---|
| Compute | PySpark on Databricks |
| Storage | Delta Lake (Unity Catalog) |
| Transformation | dbt Core with dbt-databricks |
| Orchestration | Apache Airflow (Astronomer) |
| FX Rates | ExchangeRate-API |
| Payroll Output | Excel with Power Query |
| Version Control | GitHub |

---

## Data Sources

- IBM HR Analytics Attrition dataset (Kaggle) — 1,470 real employee records
- ExchangeRate-API — live USD/NGN, USD/GBP, USD/EUR daily rates

---

## Pipeline Architecture

```
Bronze (Delta Tables — written by PySpark)
    hr_employees_raw              raw IBM HR employee records
    market_exchange_rates_raw     live FX rates with fallback flag
    hr_payroll_runs               PAYE-calculated payroll with payout blocks

Silver (dbt Views)
    stg_hr_employees              cleaned employee records
    stg_hr_payroll_runs           validated payroll with block reason
    stg_market_exchange_rates     FX rates with daily change and direction

Quarantine (dbt Delta Table)
    quarantine_all_bad_records    all rejected records with reason and timestamp

Gold (dbt Delta Tables)
    mart_hr_paye_detail           one row per employee — Excel template source
    mart_hr_paye_summary          department totals — Power BI source
    mart_hr_payroll               headcount, cost and attrition by department
    mart_finance_journal_entries  auto-generated double-entry bookkeeping
    mart_finance_remittance_schedule  statutory payment due dates (FIRS, PFA, FMBN, NHIS)
    mart_market_rates             daily FX rates with change and fallback flag
```

---

## PAYE Engine Logic

Based on Nigerian Finance Act tax bands. IBM HR MonthlyIncome (USD) is converted to NGN using the live USD/NGN rate from ExchangeRate-API.

Columns calculated per employee per payroll run:

- Gross monthly salary (NGN) — MonthlyIncome × USD/NGN rate
- Allowances — 20% of gross
- CRA — higher of ₦200,000 or (1% + 20%) of gross salary
- Taxable income — gross salary minus CRA
- PAYE tax — banded 7% to 24% per Nigerian Finance Act
- Employee pension — 8% of gross
- Employer pension — 10% of gross
- NHF deduction — 2.5% of gross
- NHIS deduction — flat ₦5,000
- Total deductions — sum of all statutory deductions
- Net salary payable — gross minus deductions (zero if payout blocked)

---

## Payout Block Logic

Payouts are blocked automatically for:

| Reason | Source Column |
|---|---|
| Terminated employee | `Attrition = 'Yes'` |
| Net salary zero or negative | Calculated |
| Gross salary exceeds job grade budget cap | `JobLevel` vs cap table |

Blocked records are written to the quarantine table with the specific block reason and timestamp.

---

## FX Fallback

If ExchangeRate-API is unavailable, the pipeline reads the most recent rate from the Bronze Delta table and marks `is_fallback = true` on the payroll run. The Gold table exposes this flag so finance teams can see when a fallback rate was used.

---

## Notebooks (run in order)

1. `01_bronze_ingestion.py` — loads IBM HR CSV into Bronze
2. `02_market_data_ingestion.py` — fetches live FX rates, falls back to last known rate
3. `03_paye_payroll.py` — full PAYE calculation, payout blocking, CSV export

---

## dbt Project Structure

```
Omni_dbt/
    dbt_project.yml
    models/
        sources.yml
        silver/
            hr/
                stg_hr_employees.sql
                stg_hr_payroll_runs.sql
            market/
                stg_market_exchange_rates.sql
        quarantine/
            quarantine_all_bad_records.sql
            schema.yml
        gold/
            mart_hr_paye_detail.sql
            mart_hr_paye_summary.sql
            mart_hr_payroll.sql
            mart_finance_journal_entries.sql
            mart_finance_remittance_schedule.sql
            mart_market_rates.sql
            schema.yml
    tests/
        assert_pension_not_exceed_salary.sql
```

---

## Orchestration

Airflow DAG runs daily at 6:00 AM (Africa/Lagos):

```
bronze_ingestion → market_ingestion → paye_payroll → dbt run → dbt test
```

On failure: email alert sent, Gold tables retain previous day's data.

---

## Excel Integration

The `mart_hr_paye_detail` Gold table connects directly to the Excel payroll template via Power Query. Finance teams click Refresh to auto-populate all payroll columns. A hidden validation tab compares pipeline-calculated net salary against Excel-recalculated values and turns cells red if there is any discrepancy.

See `docs/excel_integration.md` for the full Power Query setup guide.

---

## Setup

1. Upload `WA_Fn-UseC_-HR-Employee-Attrition.csv` to `/Volumes/workspace/omni/raw_data/` in Databricks
2. Replace `YOUR_EXCHANGERATE_API_KEY` in `02_market_data_ingestion.py` with your key from exchangerate-api.com
3. Run notebooks 01, 02, 03 in order
4. Run `dbt run` then `dbt test` from the `Omni_dbt` folder
5. Connect Excel template via Power Query to `workspace.gold.mart_hr_paye_detail`

---

## What Makes This Project Stand Out

- Real dataset, not synthetic data
- Actual Nigerian PAYE law with correct CRA calculation that most basic calculators get wrong
- Live FX conversion with automatic fallback when API is unavailable
- Cross-domain payout blocking using HR data — terminated employees, invalid calculations, budget breaches
- Quarantine layer preserving every rejected record with reason and timestamp for audit
- Auto-generated double-entry journal entries from payroll output — no manual finance entry needed
- Statutory remittance schedule with legally correct due dates for FIRS, PFA, FMBN, and NHIS
- Excel template with live Power Query refresh and validation tab — finance team never copies and pastes
- Architecture mirrors how Rippling, Gusto, and Deel handle multi-domain HR and payroll data
