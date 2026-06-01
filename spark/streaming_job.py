"""
Spark Structured Streaming — crypto price processor.

This is the most important file in Project 4. Read it carefully.

What Spark Structured Streaming does:
  - Reads a continuous stream of messages from Kafka
  - Processes them in micro-batches (every 5 seconds here)
  - Computes windowed aggregations across time
  - Writes results to Postgres and S3 in near-real-time

Key concepts explained inline below.
"""

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import KAFKA_CONFIG, SPARK_CONFIG, DB_CONFIG, S3_BUCKET, AWS_CONFIG

logger = logging.getLogger(__name__)

# ─── Schema ──────────────────────────────────────────────────────────────────

PRICE_EVENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),  True),
    StructField("symbol",         StringType(),  True),
    StructField("price_usd",      DoubleType(),  True),
    StructField("price_open",     DoubleType(),  True),
    StructField("price_high",     DoubleType(),  True),
    StructField("price_low",      DoubleType(),  True),
    StructField("volume_24h_usd", DoubleType(),  True),
    StructField("change_24h_pct", DoubleType(),  True),
    StructField("source",         StringType(),  True),
    StructField("captured_at",    StringType(),  True),
])


# ─── Spark session ────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Build a SparkSession configured for Kafka + Postgres + S3.

    The .config() calls add Kafka and Postgres connector JARs.
    Spark downloads these from Maven on first run — expect a ~2 minute
    startup delay the first time.
    """
    builder = (
        SparkSession.builder
        .appName("CryptoStreamingPipeline")
        .master(SPARK_CONFIG["master"])
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
                "org.postgresql:postgresql:42.6.0",
            ])
        )
        .config("spark.sql.streaming.checkpointLocation",
                SPARK_CONFIG["checkpoint_dir"])
        .config("spark.sql.shuffle.partitions", "4")
    )

    # Only configure S3 when credentials are present; empty values cause
    # Hadoop's S3A connector to reject the config with IllegalArgumentException.
    if AWS_CONFIG.get("aws_access_key_id") and AWS_CONFIG.get("aws_secret_access_key"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key",
                    AWS_CONFIG["aws_access_key_id"])
            .config("spark.hadoop.fs.s3a.secret.key",
                    AWS_CONFIG["aws_secret_access_key"])
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        )

    return builder.getOrCreate()


# ─── Stream reader ────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Read from Kafka as a streaming DataFrame.

    Key concept: Spark treats the Kafka stream as an unbounded table.
    Each micro-batch reads the newest available messages since the last offset.
    The checkpoint directory stores offsets so Spark knows where it left off —
    this is what makes the stream fault-tolerant.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"])
        .option("subscribe", KAFKA_CONFIG["prices_topic"])
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


# ─── Transformations ──────────────────────────────────────────────────────────

def parse_price_events(raw_stream):
    """
    Parse the raw Kafka bytes into structured columns.

    Kafka messages arrive as (key, value) byte pairs.
    We cast value to string, then parse the JSON using our schema.
    """
    return (
        raw_stream
        .select(
            F.col("key").cast(StringType()).alias("symbol_key"),
            F.from_json(
                F.col("value").cast(StringType()),
                PRICE_EVENT_SCHEMA
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "data.*",
            "kafka_timestamp",
            F.to_timestamp("data.captured_at").alias("event_time"),
        )
        .withWatermark(
            "event_time",
            f"{SPARK_CONFIG['watermark_minutes']} minutes"
        )
        # Watermark = how long to wait for late-arriving data.
        # If a message arrives >2 minutes after its event_time, Spark drops it.
        # This is the tradeoff between completeness and latency.
    )


def compute_windowed_aggregations(parsed_stream):
    """
    Compute rolling statistics across 5-minute windows.

    Window aggregations are the core of streaming analytics.
    For each 5-minute window per symbol, we compute:
      - OHLCV (open, high, low, close, volume)
      - VWAP (volume-weighted average price)
      - Price change % across the window
      - Event count (how many ticks arrived)
    """
    return (
        parsed_stream
        .groupBy(
            F.col("symbol"),
            F.window("event_time", "5 minutes", "1 minute")
            # sliding window: 5 min wide, moves every 1 min
            # so at any point we have overlapping windows for smooth aggregations
        )
        .agg(
            F.first("price_usd").alias("price_open"),
            F.max("price_usd").alias("price_high"),
            F.min("price_usd").alias("price_low"),
            F.last("price_usd").alias("price_close"),
            F.sum("volume_24h_usd").alias("volume_usd"),
            F.avg("price_usd").alias("price_avg"),
            F.stddev("price_usd").alias("price_stddev"),
            F.count("event_id").alias("tick_count"),
            # VWAP approximation: avg(price * volume) / avg(volume)
            F.expr("sum(price_usd * volume_24h_usd) / nullif(sum(volume_24h_usd), 0)")
             .alias("vwap"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "price_open", "price_high", "price_low", "price_close",
            "volume_usd", "price_avg", "price_stddev", "vwap", "tick_count",
            # Price change % within the window
            F.round(
                F.expr("(price_close - price_open) / nullif(price_open, 0) * 100"), 4
            ).alias("window_change_pct"),
            F.current_timestamp().alias("computed_at"),
        )
    )


def add_anomaly_flags(aggregated_stream):
    """
    Flag anomalous windows for downstream alerting.
    These become columns in the analytics mart.
    """
    return aggregated_stream.withColumn(
        "flag_large_move",
        F.abs(F.col("window_change_pct")) > 3.0
    ).withColumn(
        "flag_high_volatility",
        F.col("price_stddev") > F.col("price_avg") * 0.02
    ).withColumn(
        "flag_low_liquidity",
        F.col("tick_count") < 2
    )


# ─── Output sinks ─────────────────────────────────────────────────────────────

def write_raw_ticks_to_postgres(parsed_stream):
    """
    Write every raw price tick to Postgres.
    Uses foreachBatch — runs our write function on each micro-batch DataFrame.
    """
    def write_batch(df, epoch_id):
        if df.count() == 0:
            return
        (
            df.write
            .format("jdbc")
            .option("url",      f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            .option("dbtable",  "raw.price_ticks")
            .option("user",     DB_CONFIG["user"])
            .option("password", DB_CONFIG["password"])
            .option("driver",   "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        logger.info("Epoch %d: wrote %d ticks to Postgres", epoch_id, df.count())

    return (
        parsed_stream.writeStream
        .outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation",
                f"{SPARK_CONFIG['checkpoint_dir']}/raw_ticks")
        .trigger(processingTime=f"{SPARK_CONFIG['trigger_seconds']} seconds")
        .start()
    )


def write_aggregations_to_postgres(aggregated_stream):
    """Write windowed aggregations (OHLCV + metrics) to Postgres."""
    def write_batch(df, epoch_id):
        if df.count() == 0:
            return
        (
            df.write
            .format("jdbc")
            .option("url",      f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            .option("dbtable",  "raw.price_windows")
            .option("user",     DB_CONFIG["user"])
            .option("password", DB_CONFIG["password"])
            .option("driver",   "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        logger.info("Epoch %d: wrote %d windows to Postgres", epoch_id, df.count())

    return (
        aggregated_stream.writeStream
        .outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation",
                f"{SPARK_CONFIG['checkpoint_dir']}/price_windows")
        .trigger(processingTime=f"{SPARK_CONFIG['trigger_seconds']} seconds")
        .start()
    )


def write_ticks_to_s3(parsed_stream):
    """Archive raw ticks to S3 as date-partitioned Parquet."""
    s3_path = f"s3a://{S3_BUCKET}/raw/price_ticks/"
    return (
        parsed_stream.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", s3_path)
        .option("checkpointLocation",
                f"{SPARK_CONFIG['checkpoint_dir']}/s3_ticks")
        .partitionBy("symbol")
        .trigger(processingTime="1 minute")  # S3 writes are batched per minute
        .start()
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def run_streaming_job():
    """
    Wire up the full streaming pipeline and run until interrupted.

    The stream topology:
      Kafka → parse → [raw ticks → Postgres + S3]
                    → [windowed agg → anomaly flags → Postgres]
    """
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting Spark Structured Streaming job")

    raw_stream    = read_kafka_stream(spark)
    parsed        = parse_price_events(raw_stream)
    aggregated    = compute_windowed_aggregations(parsed)
    with_flags    = add_anomaly_flags(aggregated)

    queries = [
        write_raw_ticks_to_postgres(parsed),
        write_aggregations_to_postgres(with_flags),
    ]

    if AWS_CONFIG.get("aws_access_key_id") and AWS_CONFIG.get("aws_secret_access_key"):
        queries.append(write_ticks_to_s3(parsed))

    logger.info("All streaming queries started. Awaiting termination...")

    # Block until all queries stop (Ctrl+C or error)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    run_streaming_job()
