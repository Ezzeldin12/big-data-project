from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ══════════════════════════════════════════
# GOLD 5 — Hourly Demand
# ══════════════════════════════════════════
@dp.table(
    name="gold_hourly_demand",
    comment="Gold: hourly trip demand by weekday/weekend"
)
def gold_hourly_demand():
    df = dp.read("nyc_taxi_silver_enriched")

    return (
        df.groupBy("pickup_hour", "day_type", "time_of_day")
        .agg(
            F.count("*")                         .alias("total_trips"),
            F.sum("total_amount")                .alias("total_revenue"),
            F.avg("fare_amount")                 .alias("avg_fare"),
            F.avg("trip_duration_min")           .alias("avg_duration_min"),
            F.avg("tip_percentage")              .alias("avg_tip_pct"),
            F.sum(F.col("has_tip").cast("int"))  .alias("trips_with_tip"),
        )
        .withColumn("tip_rate_pct",
            F.round(F.col("trips_with_tip") / F.col("total_trips") * 100, 1)
        )
        .orderBy("pickup_hour", "day_type")
    )
