
import json
import random
from pathlib import Path
from faker import Faker

# Configuración inicial
fake = Faker("es_MX")

NUM_TRANSACTIONS = 10

# Carpeta donde guardaremos los datos de prueba
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "transactions_sample.jsonl"

# Valores disponibles para nuestras transacciones
categories = ["Electronics", "Clothing", "Home", "Books", "Sports"]
regions = ["Jalisco", "CDMX", "Nuevo Leon", "Puebla", "Queretaro"]
payment_methods = ["Card", "Cash", "Transfer"]


def generate_transaction(index):
    """Genera una transacción comercial ficticia."""

    return {
        "transaction_id": f"TXN-{index:08d}",
        "customer_id": f"CUST-{random.randint(1, 10000):05d}",
        "product_id": f"PROD-{random.randint(1, 1000):05d}",
        "category": random.choice(categories),
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(10, 5000), 2),
        "timestamp": fake.date_time_between(
            start_date="-1y",
            end_date="now"
        ).isoformat(),
        "region": random.choice(regions),
        "payment_method": random.choice(payment_methods)
    }


# Guardar una transacción por línea
with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
    for i in range(1, NUM_TRANSACTIONS + 1):
        transaction = generate_transaction(i)
        file.write(json.dumps(transaction, ensure_ascii=False) + "\n")

print(f"Generadas {NUM_TRANSACTIONS} transacciones.")
print(f"Archivo creado en: {OUTPUT_FILE}")
print(f"Tamaño: {OUTPUT_FILE.stat().st_size:,} bytes")