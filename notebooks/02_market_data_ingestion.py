# Pulls live FX rates from ExchangeRate-API into Bronze
# Falls back to last known good rate if API is unavailable

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from pyspark.sql.functions import current_timestamp, col

spark = SparkSession.builder.appName("omni_market_ingestion").getOrCreate()

spark.sql("USE CATALOG workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

API_KEY    = "EXCHANGE_API_KEY" 
BASE_URL   = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"
TABLE_NAME = "bronze.market_exchange_rates_raw"
TARGETS    = ["NGN", "GBP", "EUR"]

schema = StructType([
    StructField("from_currency",  StringType(),  False),
    StructField("to_currency",    StringType(),  False),
    StructField("rate",           DoubleType(),  False),
    StructField("last_refreshed", StringType(),  True),
    StructField("is_fallback",    BooleanType(), False),
])


def fetch_live_rates():
    resp = requests.get(BASE_URL, timeout=10)
    resp.raise_for_status()
    data      = resp.json()
    rates     = data.get("conversion_rates", {})
    refreshed = data.get("time_last_update_utc", "")
    rows = []
    for target in TARGETS:
        rows.append({
            "from_currency":  "USD",
            "to_currency":    target,
            "rate":           float(rates[target]),
            "last_refreshed": refreshed,
            "is_fallback":    False,
        })
    return rows


def fetch_fallback_rates():
    print("API unavailable. Loading last known good rates from Bronze.")
    rows = []
    for target in TARGETS:
        row = (
            spark.table(TABLE_NAME)
            .filter(
                (col("from_currency") == "USD") &
                (col("to_currency")   == target)
            )
            .orderBy(col("ingested_at").desc())
            .limit(1)
            .collect()
        )
        if row:
            rows.append({
                "from_currency":  "USD",
                "to_currency":    target,
                "rate":           row[0]["rate"],
                "last_refreshed": row[0]["last_refreshed"],
                "is_fallback":    True,
            })
        else:
            raise RuntimeError(f"No fallback rate for USD/{target}.")
    return rows

try:
    rate_rows = fetch_live_rates()
    print("Live FX rates fetched successfully.")
except Exception as e:
    print(f"Live fetch failed: {e}")
    rate_rows = fetch_fallback_rates()

df = spark.createDataFrame(rate_rows, schema=schema)
df = df.withColumn("ingested_at", current_timestamp())

df.write.format("delta").mode("append").saveAsTable(TABLE_NAME)

fallback_used = any(r["is_fallback"] for r in rate_rows)
print(f"Wrote {len(rate_rows)} FX rates. Fallback used: {fallback_used}")