from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ══════════════════════════════════════════
# SILVER 2 — Clean
# ══════════════════════════════════════════
@dp.table(name="nyc_taxi_silver_clean", comment="Silver: business rules validation")
def nyc_taxi_silver_clean():
    df = dp.read("nyc_taxi_silver_standardized")

    return (
        df
        .filter(F.col("fare_amount") > 0)
        .filter((F.col("trip_distance") > 0) & (F.col("trip_distance") < 100))
        .filter((F.col("passenger_count") > 0) & (F.col("passenger_count") <= 6))
        .filter(F.col("vendor_id").isin([1, 2]))
        .dropna(subset=["pickup_datetime", "dropoff_datetime"])
        .filter(F.col("pickup_datetime") < F.col("dropoff_datetime"))
        .filter(F.year("pickup_datetime").isin([2014, 2015]))
    )
