"""
Spark Streaming Consumer
Reads from Kafka -> Processes -> Writes to PostgreSQL
Password is loaded from environment variable for security
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, count, countDistinct,
    sum as spark_sum, avg, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# ── PostgreSQL Settings ────────────────────────────────────
# Password is read from environment variable PG_PASSWORD
# Set it in your terminal before running:
# Windows: set PG_PASSWORD=your_password
# Then run: run_spark.bat
PG_HOST     = "localhost"
PG_PORT     = "5432"
PG_DB       = "ecommerce_analytics"
PG_USER     = "postgres"
PG_PASS     = os.environ.get("PG_PASSWORD", "")   # ← secure!
PG_URL      = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPS    = {
    "user":     PG_USER,
    "password": PG_PASS,
    "driver":   "org.postgresql.Driver"
}

# ── Kafka Settings ─────────────────────────────────────────
KAFKA_SERVERS = "localhost:9092"
TOPIC         = "clickstream"

# ── Schema matching producer.py output ────────────────────
EVENT_SCHEMA = StructType([
    StructField("user_id",      StringType(),  True),
    StructField("session_id",   StringType(),  True),
    StructField("event_type",   StringType(),  True),
    StructField("product_id",   StringType(),  True),
    StructField("product_name", StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("price",        DoubleType(),  True),
    StructField("revenue",      DoubleType(),  True),
    StructField("page_url",     StringType(),  True),
    StructField("device_type",  StringType(),  True),
    StructField("country",      StringType(),  True),
    StructField("timestamp",    StringType(),  True),
])


def write_to_postgres(df, table):
    if df.count() > 0:
        df.write.jdbc(url=PG_URL, table=table, mode="append", properties=PG_PROPS)


def process_batch(batch_df, batch_id):
    rows = batch_df.count()
    print(f"\n--- Batch {batch_id} | {rows} rows ---")
    if rows == 0:
        return

    # 1. Write raw events
    raw = batch_df.select(
        col("user_id"), col("session_id"), col("event_type"),
        col("product_id"), col("product_name"), col("category"),
        col("price"), col("page_url"), col("device_type"),
        col("country"), col("timestamp").cast(TimestampType()),
    )
    write_to_postgres(raw, "clickstream_events")

    # 2. Write aggregates
    agg = batch_df.groupBy("event_type", "category").agg(
        count("*").alias("total_events"),
        countDistinct("user_id").alias("unique_users"),
        spark_sum("revenue").alias("total_revenue"),
        avg("price").alias("avg_price"),
    ).withColumn("window_start", current_timestamp()) \
     .withColumn("window_end",   current_timestamp())
    write_to_postgres(agg, "clickstream_aggregates")

    print(f"Written {rows} raw events + aggregates to PostgreSQL")


def main():
    print("Starting Spark Streaming Consumer...")

    # Check password is set
    if not PG_PASS:
        print("ERROR: PG_PASSWORD environment variable not set!")
        print("Run: set PG_PASSWORD=your_postgres_password")
        return

    spark = SparkSession.builder \
        .appName("EcommerceClickstream") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.streaming.checkpointLocation",
                "file:///C:/tmp/spark_checkpoint") \
        .config("spark.hadoop.fs.file.impl",
                "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .config("spark.sql.streaming.fileSource.log.compactInterval", "10") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw \
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("d")) \
        .select("d.*")

    query = parsed.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("Spark streaming running. Waiting for Kafka data...")
    query.awaitTermination()


if __name__ == "__main__":
    main()