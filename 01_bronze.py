# ══════════════════════════════════════════
# BRONZE
# ══════════════════════════════════════════
from pyspark import pipelines as dp
from pyspark.sql import functions as F

PRE2015_PATH  = "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2014-*.csv.gz"
POST2015_PATH = "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2015-*.csv.gz"


def trim_column_names(df):
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip())
    return df


def read_and_normalize_pre2015(path):
    df = spark.read.option("header", "true").csv(path)
    df = trim_column_names(df)
    return (
        df
        .withColumnRenamed("vendor_id",       "VendorID")
        .withColumnRenamed("pickup_datetime",  "tpep_pickup_datetime")
        .withColumnRenamed("dropoff_datetime", "tpep_dropoff_datetime")
        .withColumnRenamed("rate_code",        "RateCodeID")
        .withColumnRenamed("surcharge",        "extra")
        .withColumn("improvement_surcharge", F.lit(None).cast("string"))
        .withColumn("_schema_version",       F.lit("pre_2015"))
    )


def read_and_normalize_2015plus(path):
    df = spark.read.option("header", "true").csv(path)
    return df.withColumn("_schema_version", F.lit("2015_plus"))


@dp.table(name="nyc_taxi_bronze", comment="Bronze layer: raw ingestion — 24 months (2014-01 to 2015-12)")
def nyc_taxi_bronze():
    pre_2015  = read_and_normalize_pre2015(PRE2015_PATH)
    post_2015 = read_and_normalize_2015plus(POST2015_PATH)
    return pre_2015.unionByName(post_2015, allowMissingColumns=True)
