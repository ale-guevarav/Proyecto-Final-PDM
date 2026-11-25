
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, TimestampType
)

PROJECT_DIR = Path(__file__).resolve().parent.parent
INPUT_PATH = str(
    PROJECT_DIR / "data" / "sample" / "transactions_sample.jsonl"
)

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("region", StringType(), True),
    StructField("payment_method", StringType(), True)
])

spark = (
    SparkSession.builder
    .appName("TestCommercialTransactions")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.schema(schema).json(INPUT_PATH)

clean_df = (
    df.dropna()
    .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
    .dropDuplicates(["transaction_id"])
)

enriched_df = (
    clean_df
    .withColumn(
        "total_amount",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    .withColumn("transaction_date", F.to_date("timestamp"))
)

summary_df = (
    enriched_df
    .groupBy("region", "category")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.round(F.avg("total_amount"), 2).alias("average_transaction")
    )
)

filtered_df = (
    summary_df
    .filter(F.col("total_sales") > 1000)
    .orderBy(F.col("total_sales").desc())
)

print("\nRESULTADOS DEL PIPELINE:")
filtered_df.show(30, truncate=False)

spark.stop()
