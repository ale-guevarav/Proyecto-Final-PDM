# Dataset Documentation — Commercial Transactions

## 1. Dataset Description

The dataset contains synthetic commercial transactions generated with Python and Faker. It simulates purchases across different product categories, geographic regions, and payment methods.

The objective is to analyze transaction patterns, calculate sales metrics, and process large volumes of structured data using Apache Spark on AWS EMR.

The same transaction schema will be reused in the second part of the project to simulate continuous events through Apache Kafka.

## 2. Data Model

The dataset uses a relational-style, tabular data model represented as JSON Lines (JSONL).

Each line contains one independent commercial transaction with nine attributes. This structure allows Spark to load the records into DataFrames and supports serialization into JSON messages for Kafka.

## 3. Dataset Schema

| Field | Data Type | Description |
|---|---|---|
| `transaction_id` | String | Unique transaction identifier |
| `customer_id` | String | Customer identifier |
| `product_id` | String | Product identifier |
| `category` | String | Product category |
| `quantity` | Integer | Number of purchased units |
| `unit_price` | Double | Price per unit |
| `timestamp` | Timestamp | Date and time of the transaction |
| `region` | String | Geographic region |
| `payment_method` | String | Payment method |

The `total_amount` field will be calculated during Spark processing by multiplying `quantity` by `unit_price`.

## 4. Dataset Size and Generation

The dataset was generated using a Python script with the Faker library.

- **Number of records:** 133,595,850
- **Number of JSONL files:** 120
- **Total raw dataset size:** 32,345,850,790 bytes (approximately 32.35 GB)
- **File size:** approximately 256 MiB per file
- **Format:** JSON Lines (JSONL)
- **Generation method:** Synthetic data generation using Python and Faker

The generated dataset exceeds the minimum requirement of 30 GB.

## 5. Data Storage

The raw dataset is stored in Amazon S3 in the AWS `us-east-1` region.

**S3 bucket:** `pdm-transacciones-2026-alegu`

**Raw dataset path:**

`s3://pdm-transacciones-2026-alegu/raw/transactions/`

The processed data will be written in Parquet format to:

`s3://pdm-transacciones-2026-alegu/processed/transactions/`

## 6. Source Code

The dataset generation scripts are available in the project's GitHub repository:

- Repository: https://github.com/ale-guevarav/Proyecto-Final-PDM
- Main generator: https://github.com/ale-guevarav/Proyecto-Final-PDM/blob/main/src/generate_dataset.py
- Sample generator: https://github.com/ale-guevarav/Proyecto-Final-PDM/blob/main/src/generate_transactions.py