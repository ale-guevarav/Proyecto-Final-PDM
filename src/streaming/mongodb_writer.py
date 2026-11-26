
from pymongo import MongoClient, UpdateOne

MONGODB_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "commercial_transactions"
COLLECTION_NAME = "processed_transactions"


def write_to_mongodb(batch_df, batch_id):
    """
    Guarda un microbatch de Spark en MongoDB.
    Utiliza transaction_id para evitar duplicados.
    """

    if batch_df.isEmpty():
        print(f"Microbatch {batch_id}: sin transacciones.")
        return

    client = MongoClient(
        MONGODB_URI,
        serverSelectionTimeoutMS=5000
    )

    try:
        collection = client[DATABASE_NAME][COLLECTION_NAME]

        total_processed = 0
        total_inserted = 0

        # Procesar registros sin cargarlos todos en memoria
        for row in batch_df.toLocalIterator():
            document = row.asDict(recursive=True)

            # Convertir la fecha a texto para MongoDB
            if document.get("transaction_date") is not None:
                document["transaction_date"] = str(
                    document["transaction_date"]
                )

            operation = UpdateOne(
                {"transaction_id": document["transaction_id"]},
                {"$setOnInsert": document},
                upsert=True
            )

            result = collection.bulk_write(
                [operation],
                ordered=False
            )

            total_processed += 1
            total_inserted += result.upserted_count

        print(
            f"Microbatch {batch_id}: "
            f"{total_processed} procesadas, "
            f"{total_inserted} insertadas en MongoDB."
        )

    finally:
        client.close()
