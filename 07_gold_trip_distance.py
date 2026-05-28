from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ══════════════════════════════════════════
# GOLD 2 — Trip Distance Breakdown
# ══════════════════════════════════════════
@dp.table(
    name="gold_trip_distance",
    comment="Gold: trip distribution by distance bucket"
)
def gold_trip_distance():
    df = dp.read("nyc_taxi_silver_enriched")

    return (
        df.groupBy("trip_type")
        .agg(
            F.count("*")                         .alias("total_trips"),
            F.sum("total_amount")                .alias("total_revenue"),
            F.avg("fare_amount")                 .alias("avg_fare"),
            F.avg("trip_distance")               .alias("avg_distance"),
            F.avg("trip_duration_min")           .alias("avg_duration_min"),
            F.avg("tip_percentage")              .alias("avg_tip_pct"),
            F.sum(F.col("has_tip").cast("int"))  .alias("trips_with_tip"),
        )
        .withColumn("revenue_per_trip",
            F.round(F.col("total_revenue") / F.col("total_trips"), 2)
        )
        .withColumn("tip_rate_pct",
            F.round(F.col("trips_with_tip") / F.col("total_trips") * 100, 1)
        )
        .withColumn("_sort_order",
            F.when(F.col("trip_type") == "Very Short", 1)
             .when(F.col("trip_type") == "Short",      2)
             .when(F.col("trip_type") == "Medium",     3)
             .otherwise(4)
        )
        .orderBy("_sort_order")
        .drop("_sort_order")
    )
