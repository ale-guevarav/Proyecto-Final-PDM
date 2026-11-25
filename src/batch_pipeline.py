import sys

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

# Configuración del proyecto
PROJECT_DIR = Path(__file__).resolve().parent.parent

LOCAL_INPUT = str(
    PROJECT_DIR / "data" / "sample" / "transactions_sample.jsonl"
)

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else LOCAL_INPUT

LOCAL_OUTPUT = str(
    PROJECT_DIR / "data" / "processed_local"
)

OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else LOCAL_OUTPUT

# Esquema explícito de las transacciones
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("region", StringType(), True),
    StructField("payment_method", StringType(), True),
])

# Inicializar Spark
spark = (
    SparkSession.builder
    .appName("CommercialTransactionsBatch")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Leer los datos originales
transactions_df = (
    spark.read
    .schema(transaction_schema)
    .json(INPUT_PATH)
)


# Primera transformación: limpieza de datos

clean_transactions_df = (
    transactions_df
    .dropna(subset=[
        "transaction_id",
        "customer_id",
        "product_id",
        "category",
        "quantity",
        "unit_price",
        "timestamp",
        "region",
        "payment_method"
    ])
    .filter(
        (transactions_df["quantity"] > 0) &
        (transactions_df["unit_price"] > 0)
    )
    .dropDuplicates(["transaction_id"])
)

print("Transformación de limpieza configurada correctamente")


# Segunda transformación: derivación de columnas

enriched_transactions_df = (
    clean_transactions_df
    .withColumn(
        "total_amount",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    .withColumn(
        "transaction_date",
        F.to_date(F.col("timestamp"))
    )
)

print("Transformación de derivación de columnas configurada correctamente")


# Tercera transformación: agregaciones de ventas

sales_summary_df = (
    enriched_transactions_df
    .groupBy("region", "category")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(
            F.sum("total_amount"), 2
        ).alias("total_sales"),
        F.round(
            F.avg("total_amount"), 2
        ).alias("average_transaction")
    )
)

print("Agregación de ventas configurada correctamente")


# Cuarta transformación: filtrado y ordenamiento

filtered_sales_df = (
    sales_summary_df
    .filter(F.col("total_sales") > 1000)
    .orderBy(F.col("total_sales").desc())
)


print("Filtrado y ordenamiento configurados correctamente")

# Persistencia: guardar el resumen de ventas en Parquet
print("Iniciando escritura de resultados en Parquet...")

(
    filtered_sales_df
    .write
    .mode("overwrite")
    .partitionBy("region")
    .parquet(OUTPUT_PATH)
)

print(f"Resultados guardados correctamente en: {OUTPUT_PATH}")

spark.stop()
