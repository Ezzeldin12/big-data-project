from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ══════════════════════════════════════════
# GOLD 1 — Monthly Trends
# ══════════════════════════════════════════
@dp.table(
    name="gold_monthly_trends",
    comment="Gold: monthly KPIs across Nov 2014 – Feb 2015"
)
def gold_monthly_trends():
    df = dp.read("nyc_taxi_silver_enriched")

    return (
        df.groupBy("pickup_year", "pickup_month")
        .agg(
            F.count("*")                         .alias("total_trips"),
            F.sum("total_amount")                .alias("total_revenue"),
            F.avg("fare_amount")                 .alias("avg_fare"),
            F.avg("trip_distance")               .alias("avg_distance"),
            F.avg("trip_duration_min")           .alias("avg_duration_min"),
            F.avg("tip_percentage")              .alias("avg_tip_pct"),
            F.sum(F.col("has_tip").cast("int"))  .alias("trips_with_tip"),
            F.avg("passenger_count")             .alias("avg_passengers"),
        )
        .withColumn("tip_rate_pct",
            F.round(F.col("trips_with_tip") / F.col("total_trips") * 100, 1)
        )
        .orderBy("pickup_year", "pickup_month")
    )
