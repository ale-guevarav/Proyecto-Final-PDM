
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from mongodb_writer import write_to_mongodb
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "commercial-transactions"

spark = (
    SparkSession.builder
    .appName("CommercialTransactionsStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Mismo esquema de nueve campos de la Parte I
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

# Leer eventos desde Apache Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Convertir los mensajes JSON en columnas
transactions_df = (
    kafka_df
    .select(
        F.from_json(
            F.col("value").cast("string"),
            transaction_schema
        ).alias("data")
    )
    .select("data.*")
)

# Limpiar y transformar las transacciones
processed_df = (
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
        "payment_method",
    ])
    .filter(
        (F.col("quantity") > 0) &
        (F.col("unit_price") > 0)
    )
    .withColumn(
        "total_amount",
        F.round(
            F.col("quantity") * F.col("unit_price"),
            2
        )
    )
    .withColumn(
        "transaction_date",
        F.to_date("timestamp")
    )
)

# Mostrar resultados en la terminal
query = (
    processed_df.writeStream
    .foreachBatch(write_to_mongodb)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/transactions_mongodb"
    )
    .start()
)


print("Streaming iniciado. Esperando transacciones de Kafka...")
query.awaitTermination()
