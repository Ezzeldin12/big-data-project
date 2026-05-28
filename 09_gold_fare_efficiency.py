from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ══════════════════════════════════════════
# GOLD 4 — Fare Efficiency
# ══════════════════════════════════════════
@dp.table(
    name="gold_fare_efficiency",
    comment="Gold: fare per mile and per minute by time of day and trip type"
)
def gold_fare_efficiency():
    df = dp.read("nyc_taxi_silver_enriched")

    return (
        df
        .filter(F.col("fare_per_mile").isNotNull() & F.col("fare_per_minute").isNotNull())
        .groupBy("time_of_day", "trip_type")
        .agg(
            F.count("*")               .alias("total_trips"),
            F.avg("fare_per_mile")     .alias("avg_fare_per_mile"),
            F.avg("fare_per_minute")   .alias("avg_fare_per_min"),
            F.avg("fare_amount")       .alias("avg_fare"),
            F.avg("trip_distance")     .alias("avg_distance"),
            F.avg("trip_duration_min") .alias("avg_duration_min"),
            F.avg("tip_percentage")    .alias("avg_tip_pct"),
        )
        .orderBy("time_of_day", "trip_type")
    )
