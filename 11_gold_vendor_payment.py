from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ══════════════════════════════════════════
# GOLD 6 — Vendor & Payment Analysis
# ══════════════════════════════════════════
@dp.table(
    name="gold_vendor_payment",
    comment="Gold: vendor performance by payment method"
)
def gold_vendor_payment():
    df = dp.read("nyc_taxi_silver_enriched")

    return (
        df.groupBy("vendor_name", "payment_method")
        .agg(
            F.count("*")                         .alias("total_trips"),
            F.sum("total_amount")                .alias("total_revenue"),
            F.avg("fare_amount")                 .alias("avg_fare"),
            F.avg("trip_distance")               .alias("avg_distance"),
            F.avg("tip_percentage")              .alias("avg_tip_pct"),
            F.sum(F.col("has_tip").cast("int"))  .alias("trips_with_tip"),
        )
        .withColumn("revenue_per_trip",
            F.round(F.col("total_revenue") / F.col("total_trips"), 2)
        )
        .withColumn("tip_rate_pct",
            F.round(F.col("trips_with_tip") / F.col("total_trips") * 100, 1)
        )
        .orderBy("vendor_name", "payment_method")
    )
