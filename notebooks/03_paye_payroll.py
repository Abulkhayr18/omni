# Databricks notebook source
# ============================================================
# 03_paye_payroll.py
# Full PAYE engine with HR-driven payout block logic
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, greatest, when, current_timestamp
from pyspark.sql.types import DoubleType
from datetime import datetime

spark = SparkSession.builder.appName("omni_paye_payroll").getOrCreate()

spark.sql("USE CATALOG workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

RUN_MONTH = datetime.today().strftime("%Y_%m")

GRADE_BUDGET_CAPS = {
    1: 500_000,
    2: 800_000,
    3: 1_200_000,
    4: 2_000_000,
    5: 4_000_000,
}


# --- Step 1: Get USD/NGN rate ---
fx_rows = (
    spark.table("bronze.market_exchange_rates_raw")
    .filter(
        (col("from_currency") == "USD") &
        (col("to_currency")   == "NGN")
    )
    .orderBy(col("ingested_at").desc())
    .limit(1)
    .collect()
)

if not fx_rows:
    raise RuntimeError("No USD/NGN rate available. Cannot run payroll.")

usd_ngn     = fx_rows[0]["rate"]
is_fallback = fx_rows[0]["is_fallback"]
print(f"USD/NGN rate: {usd_ngn} | Fallback: {is_fallback}")


# --- Step 2: PAYE tax UDF ---
def calc_paye(taxable_income):
    if taxable_income <= 0:
        return 0.0
    bands = [
        (300_000,   0.07),
        (300_000,   0.11),
        (500_000,   0.15),
        (500_000,   0.19),
        (1_600_000, 0.21),
    ]
    tax       = 0.0
    remaining = taxable_income
    for band_size, rate in bands:
        if remaining <= 0:
            break
        chunk      = min(remaining, band_size)
        tax       += chunk * rate
        remaining -= chunk
    if remaining > 0:
        tax += remaining * 0.24
    return float(tax)

paye_udf = spark.udf.register("calc_paye_udf", calc_paye, DoubleType())


# --- Step 3: Load employees ---
emp = spark.table("bronze.hr_employees_raw")


# --- Step 4: Salary calculations ---
emp = emp.withColumn("gross_monthly_ngn", col("MonthlyIncome").cast(DoubleType()) * lit(usd_ngn))
emp = emp.withColumn("allowances",        col("gross_monthly_ngn") * lit(0.20))
emp = emp.withColumn("gross_salary",      col("gross_monthly_ngn") + col("allowances"))

emp = emp.withColumn(
    "cra",
    greatest(
        lit(200_000.0),
        col("gross_salary") * lit(0.01) + col("gross_salary") * lit(0.20)
    )
)

emp = emp.withColumn("taxable_income",   col("gross_salary") - col("cra"))
emp = emp.withColumn("paye_monthly",     paye_udf(col("taxable_income")))
emp = emp.withColumn("employee_pension", col("gross_salary") * lit(0.08))
emp = emp.withColumn("employer_pension", col("gross_salary") * lit(0.10))
emp = emp.withColumn("nhf_deduction",    col("gross_salary") * lit(0.025))
emp = emp.withColumn("nhis_deduction",   lit(5_000.0))

emp = emp.withColumn(
    "total_deductions",
    col("paye_monthly") +
    col("employee_pension") +
    col("nhf_deduction") +
    col("nhis_deduction")
)
emp = emp.withColumn("net_salary", col("gross_salary") - col("total_deductions"))


# --- Step 5: Budget cap lookup ---
def get_budget_cap(job_level):
    return float(GRADE_BUDGET_CAPS.get(job_level, 999_999_999))

budget_cap_udf = spark.udf.register("get_budget_cap_udf", get_budget_cap, DoubleType())
emp = emp.withColumn("budget_cap", budget_cap_udf(col("JobLevel")))


# --- Step 6: Payout block logic ---
emp = emp.withColumn(
    "block_reason",
    when(col("Attrition") == "Yes",                lit("terminated_employee"))
    .when(col("net_salary") <= 0,                  lit("net_salary_zero_or_negative"))
    .when(col("gross_salary") > col("budget_cap"), lit("exceeds_grade_budget_cap"))
    .otherwise(lit(None))
)

emp = emp.withColumn("payout_blocked",    col("block_reason").isNotNull())
emp = emp.withColumn(
    "net_salary_payable",
    when(col("payout_blocked") == True, lit(0.0)).otherwise(col("net_salary"))
)


# --- Step 7: Add metadata and write ---
emp = emp.withColumn("payroll_run_month", lit(RUN_MONTH))
emp = emp.withColumn("fx_rate_used",      lit(usd_ngn))
emp = emp.withColumn("fx_is_fallback",    lit(is_fallback))
emp = emp.withColumn("calculated_at",     current_timestamp())

emp.write.format("delta").mode("overwrite").saveAsTable("bronze.hr_payroll_runs")

total   = emp.count()
blocked = emp.filter(col("payout_blocked") == True).count()
print(f"Payroll complete. Employees: {total} | Blocked: {blocked}")