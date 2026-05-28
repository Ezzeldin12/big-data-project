from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

# ══════════════════════════════════════════
# SILVER 3 — Deduped
# ══════════════════════════════════════════
@dp.table(name="nyc_taxi_silver_deduped")
def nyc_taxi_silver_deduped():
    df = dp.read("nyc_taxi_silver_clean")

    NATURAL_KEY = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_latitude",
        "dropoff_latitude",
        "fare_amount",
    ]

    w = Window.partitionBy(*NATURAL_KEY).orderBy(F.monotonically_increasing_id())

    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
