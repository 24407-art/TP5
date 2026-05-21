#!/usr/bin/env python3
"""
Spark Structured Streaming — per-user fraud detection metrics from Kafka.
Writes windowed + lifetime aggregates and raw recent transactions to Parquet.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    count,
    current_timestamp,
    from_json,
    lit,
    sum as spark_sum,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bank-transactions")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/workspace/checkpoints/tp5")
OUTPUT_BASE = os.getenv("OUTPUT_BASE", "/workspace/data/tp5/output")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

TX_SCHEMA = StructType(
    [
        StructField("msg_entity", StringType()),
        StructField("app_type", StringType()),
        StructField("send_entity", StringType()),
        StructField("receive_entity", StringType()),
        StructField("send_id", StringType()),
        StructField("receive_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("date", StringType()),
        StructField("tx_type", StringType()),
        StructField("tx_id", StringType()),
    ]
)

WINDOW_SPECS = [
    ("3_hours", "3 hours", "1 minute"),
    ("7_days", "7 days", "1 hour"),
    ("3_weeks", "21 days", "6 hours"),
    ("3_months", "90 days", "1 day"),
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("BankXFraudStreaming")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "12"))
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_transactions(raw_kafka):
    parsed = (
        raw_kafka.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .select(from_json(col("json_str"), TX_SCHEMA).alias("tx"), "kafka_ts")
        .select("tx.*", "kafka_ts")
        .withColumn("event_time", to_timestamp(col("date")))
        .filter(col("event_time").isNotNull())
        .filter(col("amount") > 0)
    )
    sent = parsed.select(
        col("send_id").alias("user_id"),
        lit("sent").alias("direction"),
        col("receive_id").alias("counterparty"),
        col("amount"),
        col("event_time"),
        col("tx_id"),
        col("send_entity"),
        col("receive_entity"),
        col("tx_type"),
        col("kafka_ts"),
    )
    received = parsed.select(
        col("receive_id").alias("user_id"),
        lit("received").alias("direction"),
        col("send_id").alias("counterparty"),
        col("amount"),
        col("event_time"),
        col("tx_id"),
        col("send_entity"),
        col("receive_entity"),
        col("tx_type"),
        col("kafka_ts"),
    )
    return sent.unionByName(received)


def start_window_queries(user_events):
    queries = []
    for win_name, duration, slide in WINDOW_SPECS:
        agg = (
            user_events.withWatermark("event_time", WATERMARK_DELAY)
            .groupBy(
                col("user_id"),
                col("direction"),
                window(col("event_time"), duration, slide),
            )
            .agg(
                avg("amount").alias("avg_amount"),
                count(lit(1)).alias("tx_count"),
                spark_sum("amount").alias("total_amount"),
                approx_count_distinct("counterparty").alias("distinct_counterparties"),
            )
            .withColumn("window_name", lit(win_name))
            .withColumn("processed_at", current_timestamp())
        )
        path = f"{OUTPUT_BASE}/windowed/{win_name}"
        cp = f"{CHECKPOINT_BASE}/windowed_{win_name}"
        q = (
            agg.writeStream.outputMode("append")
            .format("parquet")
            .option("path", path)
            .option("checkpointLocation", cp)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        queries.append(q)
        print(f"Started window query: {win_name} -> {path}")
    return queries


def start_lifetime_query(user_events):
    lifetime = (
        user_events.groupBy("user_id", "direction")
        .agg(
            avg("amount").alias("lifetime_avg_amount"),
            count(lit(1)).alias("lifetime_tx_count"),
            spark_sum("amount").alias("lifetime_total_amount"),
            approx_count_distinct("counterparty").alias("lifetime_distinct_counterparties"),
        )
        .withColumn("processed_at", current_timestamp())
    )
    path = f"{OUTPUT_BASE}/lifetime"
    cp = f"{CHECKPOINT_BASE}/lifetime"

    def write_snapshot(batch_df, epoch_id: int) -> None:
        if batch_df.isEmpty():
            return
        # Parquet sink does not support Complete mode; overwrite snapshot each batch.
        batch_df.write.mode("overwrite").parquet(path)

    q = (
        lifetime.writeStream.foreachBatch(write_snapshot)
        .outputMode("complete")
        .option("checkpointLocation", cp)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    print(f"Started lifetime query -> {path}")
    return q


def start_recent_tx_query(user_events):
    recent = user_events.select(
        "user_id",
        "direction",
        "counterparty",
        "amount",
        "event_time",
        "tx_id",
        "send_entity",
        "receive_entity",
        "tx_type",
    ).withColumn("ingested_at", current_timestamp())

    path = f"{OUTPUT_BASE}/recent_transactions"
    cp = f"{CHECKPOINT_BASE}/recent_transactions"
    q = (
        recent.writeStream.outputMode("append")
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", cp)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    print(f"Started recent tx sink -> {path}")
    return q


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kafka(spark)
    user_events = parse_transactions(raw)

    queries = []
    queries.extend(start_window_queries(user_events))
    queries.append(start_lifetime_query(user_events))
    queries.append(start_recent_tx_query(user_events))

    print(f"Running {len(queries)} streaming queries. Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
