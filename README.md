
# Proyecto Final - Procesamiento de Datos Masivos

Proyecto desarrollado para implementar dos arquitecturas de procesamiento de datos masivos: procesamiento por lotes y procesamiento en tiempo real.

## Parte I - Procesamiento Batch

Tecnologias utilizadas:
- Python
- Faker
- Apache Spark
- Amazon EMR
- Amazon S3

Se genero un dataset sintetico de transacciones comerciales con mas de 133 millones de registros y un tamano aproximado de 32.35 GB.

El procesamiento se realizo con Apache Spark en Amazon EMR, aplicando limpieza, transformaciones, filtros, agrupaciones y calculos de ventas.

Los resultados se almacenaron en Amazon S3 utilizando archivos Parquet particionados por region.

## Parte II - Procesamiento Streaming

Tecnologias utilizadas:
- Python
- Apache Kafka
- PySpark Structured Streaming
- MongoDB
- Docker

El sistema utiliza un productor de Python que genera transacciones comerciales y las publica en un topico de Kafka.

PySpark consume los mensajes en tiempo real, valida los registros y calcula nuevos atributos.

Las transacciones procesadas se almacenan en MongoDB.

### Arquitectura

Python + Faker
    |
    v
Apache Kafka
    |
    v
PySpark Structured Streaming
    |
    v
MongoDB

### Ejecucion

1. Iniciar los servicios:

   docker compose up -d

2. Ejecutar el productor:

   python src/streaming/producer.py

3. Ejecutar el procesamiento:

   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/streaming/streaming_pipeline.py

4. Detener los servicios:

   docker compose stop

### Pruebas realizadas

- Produccion y consumo de mensajes en Kafka.
- Procesamiento de transacciones mediante PySpark.
- Insercion de documentos en MongoDB.
- Procesamiento continuo en tiempo real.
- Prevencion de documentos duplicados.
- Recuperacion mediante checkpoints.

## Evidencias

Las capturas se encuentran organizadas en:

- evidence/part1/
- evidence/part2/

## Estructura del proyecto

src/
  generate_dataset.py
  batch_pipeline.py
  streaming/
    producer.py
    streaming_pipeline.py
    mongodb_writer.py

docker-compose.yml
evidence/
checkpoints/ (archivos locales, no incluidos en Git)
