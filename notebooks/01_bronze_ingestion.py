
spark.sql("USE CATALOG workspace")
spark.sql("DROP SCHEMA IF EXISTS omni1 CASCADE")
spark.sql("DROP SCHEMA IF EXISTS omni_bronze CASCADE")
spark.sql("DROP SCHEMA IF EXISTS omni_gold CASCADE")
spark.sql("DROP SCHEMA IF EXISTS omni_quarantine CASCADE")
spark.sql("DROP SCHEMA IF EXISTS omni_silver CASCADE")
print("All old omni schemas dropped.")


# 01_bronze_ingestion.py
# Loads IBM HR CSV into Bronze Delta table
# Using Unity Catalog — 

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("omni_bronze_ingestion").getOrCreate()

# Set workspace catalog and create bronze schema
spark.sql("USE CATALOG workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")


def load_csv(file_path, table_name):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )
    df = df.withColumn("ingested_at", current_timestamp())

    df.write.format("delta").mode("overwrite").saveAsTable(f"bronze.{table_name}")

    print(f"Loaded {df.count()} rows into bronze.{table_name}")


load_csv(
    "/Volumes/workspace/omni/raw_data/WA_Fn-UseC_-HR-Employee-Attrition.csv",
    "hr_employees_raw"
)