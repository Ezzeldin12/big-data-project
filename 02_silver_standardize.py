from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ══════════════════════════════════════════
# SILVER 1 — Standardized
# ══════════════════════════════════════════
@dp.table(name="nyc_taxi_silver_standardized", comment="Silver: unified schema + cast types")
def silver_standardized():
    df = dp.read("nyc_taxi_bronze")

    return (
        df
        # ── Unified column names ──────────────────
        .withColumnRenamed("VendorID",             "vendor_id")
        .withColumnRenamed("RateCodeID",           "rate_code_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime","dropoff_datetime")

        # ── Map pre-2015 text codes → integers before casting ──
        .withColumn("vendor_id",
            F.when(F.upper(F.col("vendor_id")) == "CMT", F.lit("1"))
             .when(F.upper(F.col("vendor_id")) == "VTS", F.lit("2"))
             .otherwise(F.col("vendor_id"))
        )
        .withColumn("payment_type",
            F.when(F.upper(F.col("payment_type")) == "CRD", F.lit("1"))
             .when(F.upper(F.col("payment_type")) == "CSH", F.lit("2"))
             .when(F.upper(F.col("payment_type")) == "NOC", F.lit("3"))
             .when(F.upper(F.col("payment_type")) == "DIS", F.lit("4"))
             .when(F.upper(F.col("payment_type")) == "UNK", F.lit("5"))
             .otherwise(F.col("payment_type"))
        )

        # ── Cast all numerics (were strings from CSV) ──
        .withColumn("vendor_id",             F.col("vendor_id").cast("integer"))
        .withColumn("passenger_count",       F.col("passenger_count").cast("integer"))
        .withColumn("trip_distance",         F.col("trip_distance").cast("double"))
        .withColumn("fare_amount",           F.col("fare_amount").cast("double"))
        .withColumn("extra",                 F.col("extra").cast("double"))
        .withColumn("mta_tax",               F.col("mta_tax").cast("double"))
        .withColumn("tip_amount",            F.col("tip_amount").cast("double"))
        .withColumn("tolls_amount",          F.col("tolls_amount").cast("double"))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast("double"))
        .withColumn("total_amount",          F.col("total_amount").cast("double"))
        .withColumn("payment_type",          F.col("payment_type").cast("integer"))
        .withColumn("pickup_latitude",       F.col("pickup_latitude").cast("double"))
        .withColumn("pickup_longitude",      F.col("pickup_longitude").cast("double"))
        .withColumn("dropoff_latitude",      F.col("dropoff_latitude").cast("double"))
        .withColumn("dropoff_longitude",     F.col("dropoff_longitude").cast("double"))

        # ── Cast timestamps ───────────────────────
        .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))

        # ── Fill nulls for surcharge (pre-2015 had none) ──
        .fillna(0.0, subset=["extra", "improvement_surcharge"])
    )
