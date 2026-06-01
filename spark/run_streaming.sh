#!/bin/bash
while true; do
  KAFKA_BOOTSTRAP_SERVERS=kafka:29092 POSTGRES_HOST=postgres-crypto \
  POSTGRES_PORT=5432 POSTGRES_USER=crypto POSTGRES_PASSWORD=crypto123 \
  POSTGRES_DB=crypto_db SPARK_MASTER=local[2] \
  SPARK_CHECKPOINT_DIR=/opt/spark-checkpoints \
  /opt/spark/bin/spark-submit \
    --master local[2] \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.6.0 \
    /opt/spark-apps/streaming_job.py >> /tmp/streaming.log 2>&1
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Spark job exited ($?). Restarting in 15s..." >> /tmp/streaming.log
  sleep 15
done
