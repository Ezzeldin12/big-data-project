from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ══════════════════════════════════════════
# SILVER 4 — Enriched
# ══════════════════════════════════════════
@dp.table(name="nyc_taxi_silver_enriched", comment="Silver: enriched features")
def nyc_taxi_silver_enriched():
    df = dp.read("nyc_taxi_silver_deduped")

    return (
        df
        .withColumn("pickup_date",      F.to_date("pickup_datetime"))
        .withColumn("pickup_year",      F.year("pickup_datetime"))
        .withColumn("pickup_month",     F.month("pickup_datetime"))
        .withColumn("pickup_day",       F.dayofmonth("pickup_datetime"))
        .withColumn("pickup_hour",      F.hour("pickup_datetime"))
        .withColumn("pickup_dayofweek", F.dayofweek("pickup_datetime"))
        .withColumn("trip_duration_min",
            (F.unix_timestamp("dropoff_datetime") -
             F.unix_timestamp("pickup_datetime")) / 60
        )
        .withColumn("fare_per_mile",
            F.when(F.col("trip_distance") > 0,
                F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
        )
        .withColumn("fare_per_minute",
            F.when(F.col("trip_duration_min") > 0,
                F.round(F.col("fare_amount") / F.col("trip_duration_min"), 2))
        )
        .withColumn("time_of_day",
            F.when((F.col("pickup_hour") >= 6)  & (F.col("pickup_hour") < 12), "Morning")
             .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 17), "Afternoon")
             .when((F.col("pickup_hour") >= 17) & (F.col("pickup_hour") < 21), "Evening")
             .otherwise("Night")
        )
        .withColumn("day_type",
            F.when(F.col("pickup_dayofweek").isin([1, 7]), "Weekend").otherwise("Weekday")
        )
        .withColumn("vendor_name",
            F.when(F.col("vendor_id") == 1, "Creative Mobile Technologies")
             .when(F.col("vendor_id") == 2, "VeriFone")
             .otherwise("Unknown")
        )
        .withColumn("payment_method",
            F.when(F.col("payment_type") == 1, "Credit Card")
             .when(F.col("payment_type") == 2, "Cash")
             .when(F.col("payment_type") == 3, "No Charge")
             .when(F.col("payment_type") == 4, "Dispute")
             .otherwise("Unknown")
        )
        .withColumn("trip_type",
            F.when(F.col("trip_distance") <= 1,  "Very Short")
             .when(F.col("trip_distance") <= 3,  "Short")
             .when(F.col("trip_distance") <= 10, "Medium")
             .otherwise("Long")
        )
        .withColumn("has_tip", F.col("tip_amount") > 0)
        .withColumn("tip_percentage",
            F.when(F.col("fare_amount") > 0,
                F.round((F.col("tip_amount") / F.col("fare_amount")) * 100, 1)
            ).otherwise(0.0)
        )
        .withColumn("_silver_timestamp", F.current_timestamp())
    )
