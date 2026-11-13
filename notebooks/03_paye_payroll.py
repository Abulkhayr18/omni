# Databricks notebook source
# ============================================================
# 03_paye_payroll.py
# OMNI PAYE Engine with HR-driven payout block logic
# Integrated with OMNI Data Lineage Tracker
# ============================================================

# ------------------------------------------------------------
# 1. IMPORTS
# ------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    greatest,
    when,
    current_timestamp
)
from pyspark.sql.types import DoubleType
from datetime import datetime

from lineage_tracker import LineageTracker


# ------------------------------------------------------------
# 2. INITIALIZE SPARK SESSION
# ------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("omni_paye_payroll")
    .getOrCreate()
)


# ------------------------------------------------------------
# 3. INITIALIZE LINEAGE TRACKER
# ------------------------------------------------------------

lineage = LineageTracker(
    pipeline_name="OMNI",
    transformation_name="PAYE Payroll Calculation",
    script_name="03_paye_payroll.py",
    metadata_dir="/Volumes/workspace/omni/lineage_metadata"
)


# ------------------------------------------------------------
# 4. REGISTER UPSTREAM INPUTS
# ------------------------------------------------------------

lineage.add_input(
    "bronze.hr_employees_raw",
    "Raw employee and HR records"
)

lineage.add_input(
    "bronze.market_exchange_rates_raw",
    "USD/NGN exchange rate data"
)


# ------------------------------------------------------------
# 5. REGISTER DOWNSTREAM OUTPUTS
# ------------------------------------------------------------

lineage.add_output(
    "bronze.hr_payroll_runs",
    "Calculated payroll records"
)


# ------------------------------------------------------------
# 6. REGISTER TRANSFORMATIONS
# ------------------------------------------------------------

lineage.add_transformation(
    "FX Conversion",
    "Converts employee monthly income using the latest USD/NGN rate"
)

lineage.add_transformation(
    "PAYE Calculation",
    "Calculates taxable income and monthly PAYE liability"
)

lineage.add_transformation(
    "Payroll Deductions",
    "Calculates pension, NHF, NHIS and total deductions"
)

lineage.add_transformation(
    "Payout Validation",
    "Determines whether an employee salary payment should be blocked"
)


# ------------------------------------------------------------
# 7. SPARK CATALOG CONFIGURATION
# ------------------------------------------------------------

spark.sql("USE CATALOG workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")


# ------------------------------------------------------------
# 8. PAYROLL CONFIGURATION
# ------------------------------------------------------------

RUN_MONTH = datetime.today().strftime("%Y_%m")

GRADE_BUDGET_CAPS = {
    1: 500_000,
    2: 800_000,
    3: 1_200_000,
    4: 2_000_000,
    5: 4_000_000,
}


# ============================================================
# 9. EXECUTE PAYROLL WITH LINEAGE TRACKING
# ============================================================

with lineage.track():

    # --------------------------------------------------------
    # STEP 1: GET USD/NGN EXCHANGE RATE
    # --------------------------------------------------------

    fx_rows = (
        spark.table("bronze.market_exchange_rates_raw")
        .filter(
            (col("from_currency") == "USD") &
            (col("to_currency") == "NGN")
        )
        .orderBy(col("ingested_at").desc())
        .limit(1)
        .collect()
    )

    if not fx_rows:
        raise RuntimeError(
            "No USD/NGN rate available. Cannot run payroll."
        )

    usd_ngn = fx_rows[0]["rate"]
    is_fallback = fx_rows[0]["is_fallback"]

    print(
        f"USD/NGN rate: {usd_ngn} | "
        f"Fallback: {is_fallback}"
    )


    # --------------------------------------------------------
    # STEP 2: PAYE TAX UDF
    # --------------------------------------------------------

    def calc_paye(taxable_income):

        if taxable_income <= 0:
            return 0.0

        bands = [
            (300_000, 0.07),
            (300_000, 0.11),
            (500_000, 0.15),
            (500_000, 0.19),
            (1_600_000, 0.21),
        ]

        tax = 0.0
        remaining = taxable_income

        for band_size, rate in bands:

            if remaining <= 0:
                break

            chunk = min(remaining, band_size)

            tax += chunk * rate
            remaining -= chunk

        if remaining > 0:
            tax += remaining * 0.24

        return float(tax)


    paye_udf = spark.udf.register(
        "calc_paye_udf",
        calc_paye,
        DoubleType()
    )


    # --------------------------------------------------------
    # STEP 3: LOAD EMPLOYEE DATA
    # --------------------------------------------------------

    emp = spark.table("bronze.hr_employees_raw")


    # --------------------------------------------------------
    # STEP 4: SALARY CALCULATIONS
    # --------------------------------------------------------

    emp = emp.withColumn(
        "gross_monthly_ngn",
        col("MonthlyIncome").cast(DoubleType()) * lit(usd_ngn)
    )

    emp = emp.withColumn(
        "allowances",
        col("gross_monthly_ngn") * lit(0.20)
    )

    emp = emp.withColumn(
        "gross_salary",
        col("gross_monthly_ngn") + col("allowances")
    )

    emp = emp.withColumn(
        "cra",
        greatest(
            lit(200_000.0),
            col("gross_salary") * lit(0.01)
            + col("gross_salary") * lit(0.20)
        )
    )

    emp = emp.withColumn(
        "taxable_income",
        col("gross_salary") - col("cra")
    )

    emp = emp.withColumn(
        "paye_monthly",
        paye_udf(col("taxable_income"))
    )

    emp = emp.withColumn(
        "employee_pension",
        col("gross_salary") * lit(0.08)
    )

    emp = emp.withColumn(
        "employer_pension",
        col("gross_salary") * lit(0.10)
    )

    emp = emp.withColumn(
        "nhf_deduction",
        col("gross_salary") * lit(0.025)
    )

    emp = emp.withColumn(
        "nhis_deduction",
        lit(5_000.0)
    )

    emp = emp.withColumn(
        "total_deductions",
        col("paye_monthly")
        + col("employee_pension")
        + col("nhf_deduction")
        + col("nhis_deduction")
    )

    emp = emp.withColumn(
        "net_salary",
        col("gross_salary") - col("total_deductions")
    )


    # --------------------------------------------------------
    # STEP 5: BUDGET CAP LOOKUP
    # --------------------------------------------------------

    def get_budget_cap(job_level):

        return float(
            GRADE_BUDGET_CAPS.get(
                job_level,
                999_999_999
            )
        )


    budget_cap_udf = spark.udf.register(
        "get_budget_cap_udf",
        get_budget_cap,
        DoubleType()
    )

    emp = emp.withColumn(
        "budget_cap",
        budget_cap_udf(col("JobLevel"))
    )


    # --------------------------------------------------------
    # STEP 6: PAYOUT BLOCK LOGIC
    # --------------------------------------------------------

    emp = emp.withColumn(
        "block_reason",

        when(
            col("Attrition") == "Yes",
            lit("terminated_employee")
        )

        .when(
            col("net_salary") <= 0,
            lit("net_salary_zero_or_negative")
        )

        .when(
            col("gross_salary") > col("budget_cap"),
            lit("exceeds_grade_budget_cap")
        )

        .otherwise(lit(None))
    )

    emp = emp.withColumn(
        "payout_blocked",
        col("block_reason").isNotNull()
    )

    emp = emp.withColumn(
        "net_salary_payable",

        when(
            col("payout_blocked") == True,
            lit(0.0)
        )

        .otherwise(col("net_salary"))
    )


    # --------------------------------------------------------
    # STEP 7: ADD PAYROLL METADATA
    # --------------------------------------------------------

    emp = emp.withColumn(
        "payroll_run_month",
        lit(RUN_MONTH)
    )

    emp = emp.withColumn(
        "fx_rate_used",
        lit(usd_ngn)
    )

    emp = emp.withColumn(
        "fx_is_fallback",
        lit(is_fallback)
    )

    emp = emp.withColumn(
        "calculated_at",
        current_timestamp()
    )


    # --------------------------------------------------------
    # STEP 8: WRITE PAYROLL OUTPUT
    # --------------------------------------------------------

    emp.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("bronze.hr_payroll_runs")


    # --------------------------------------------------------
    # STEP 9: EXECUTION SUMMARY
    # --------------------------------------------------------

    total = emp.count()

    blocked = emp.filter(
        col("payout_blocked") == True
    ).count()

    print(
        f"Payroll complete. "
        f"Employees: {total} | "
        f"Blocked: {blocked}"
    )


# ============================================================
# 10. LINEAGE TRACKING COMPLETED
# ============================================================

print("OMNI payroll execution and lineage tracking completed.")
