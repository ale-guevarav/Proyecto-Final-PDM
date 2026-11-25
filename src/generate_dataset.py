
import argparse
import json
import random
from pathlib import Path
from faker import Faker

fake = Faker("es_MX")

CATEGORIES = ["Electronics", "Clothing", "Home", "Books", "Sports"]
REGIONS = ["Jalisco", "CDMX", "Nuevo Leon", "Puebla", "Queretaro"]
PAYMENT_METHODS = ["Card", "Cash", "Transfer"]

PROJECT_DIR = Path(__file__).resolve().parent.parent


def generate_transaction(index):
    return {
        "transaction_id": f"TXN-{index:012d}",
        "customer_id": f"CUST-{random.randint(1, 10000):05d}",
        "product_id": f"PROD-{random.randint(1, 1000):05d}",
        "category": random.choice(CATEGORIES),
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(10, 5000), 2),
        "timestamp": fake.date_time_between(
            start_date="-1y",
            end_date="now"
        ).isoformat(),
        "region": random.choice(REGIONS),
        "payment_method": random.choice(PAYMENT_METHODS)
    }


def generate_dataset(target_mb, chunk_mb, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)

    target_bytes = int(target_mb * 1024 * 1024)
    chunk_bytes = int(chunk_mb * 1024 * 1024)

    total_bytes = 0
    total_records = 0
    part_number = 0

    while total_bytes < target_bytes:
        part_number += 1
        file_path = output_dir / f"transactions_part_{part_number:05d}.jsonl"
        file_bytes = 0

        with file_path.open("w", encoding="utf-8") as file:
            while file_bytes < chunk_bytes and total_bytes < target_bytes:
                transaction = generate_transaction(total_records + 1)

                line = json.dumps(
                    transaction, ensure_ascii=False
                ) + "\n"

                encoded_bytes = len(line.encode("utf-8"))
                file.write(line)

                file_bytes += encoded_bytes
                total_bytes += encoded_bytes
                total_records += 1

        print(
            f"Archivo {part_number}: "
            f"{file_bytes / (1024 * 1024):.2f} MB | "
            f"Registros acumulados: {total_records:,}",
            flush=True
        )

    print("\nGeneración finalizada.")
    print(f"Archivos creados: {part_number}")
    print(f"Registros generados: {total_records:,}")
    print(f"Tamaño total: {total_bytes / (1024 * 1024):.2f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--target-mb", type=float, default=1)
    parser.add_argument("--chunk-mb", type=float, default=1)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_DIR / "data" / "sample"
    )

    args = parser.parse_args()

    if args.target_mb <= 0 or args.chunk_mb <= 0:
        parser.error("Los tamaños deben ser mayores que cero.")

    generate_dataset(
        target_mb=args.target_mb,
        chunk_mb=args.chunk_mb,
        output_dir=args.output_dir
    )
