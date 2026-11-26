
import json
import random
import time
import uuid

from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer


# Configuración
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "commercial-transactions"

fake = Faker("es_MX")

CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home",
    "Food",
    "Books",
]

REGIONS = [
    "Jalisco",
    "CDMX",
    "Nuevo Leon",
    "Puebla",
    "Guanajuato",
]

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "Cash",
    "Transfer",
]


# Conexión con Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
    acks="all",
)


def generate_transaction():
    """Genera una transacción comercial sintética."""

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1, 100000):06d}",
        "product_id": f"PROD-{random.randint(1, 10000):05d}",
        "category": random.choice(CATEGORIES),
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(10, 5000), 2),
        "timestamp": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S"
        ),
        "region": random.choice(REGIONS),
        "payment_method": random.choice(PAYMENT_METHODS),
    }


def main():
    print("Iniciando productor de transacciones...")
    print(f"Kafka: {KAFKA_SERVER}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("Presiona Ctrl+C para detener.\n")

    count = 0

    try:
        while True:
            transaction = generate_transaction()

            producer.send(
                KAFKA_TOPIC,
                key=transaction["customer_id"],
                value=transaction,
            )

            count += 1

            print(
                f"[{count}] "
                f"Transacción: {transaction['transaction_id']} | "
                f"Región: {transaction['region']} | "
                f"Categoría: {transaction['category']}"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nDeteniendo productor...")

    finally:
        producer.flush()
        producer.close()
        print(f"Total de transacciones enviadas: {count}")


if __name__ == "__main__":
    main()
