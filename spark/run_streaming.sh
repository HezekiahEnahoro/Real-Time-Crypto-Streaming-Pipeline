#!/bin/bash
LOG=/tmp/streaming.log
FAST_RETRY_DELAY=15
SLOW_RETRY_DELAY=300
MAX_FAST_RETRIES=5
crash_count=0

record_failure() {
  python3 - "$1" "$2" <<'PYEOF'
import sys
try:
    import psycopg2
    exit_code, crash_count = sys.argv[1], sys.argv[2]
    conn = psycopg2.connect(host="postgres-crypto", port=5432, user="crypto", password="crypto123", dbname="crypto_db")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO monitoring.pipeline_failures (dag_id, task_id, execution_date, error_message) VALUES (%s,%s,NOW(),%s)",
        ("spark_streaming", "run_streaming_sh", f"Spark job exited with code {exit_code} (consecutive failed-to-start count: {crash_count})")
    )
    conn.close()
except Exception as e:
    print(f"[run_streaming.sh] Failed to record crash to monitoring.pipeline_failures: {e}")
PYEOF
}

while true; do
  batch_start_line=$(wc -l < "$LOG" 2>/dev/null || echo 0)

  KAFKA_BOOTSTRAP_SERVERS=kafka:29092 POSTGRES_HOST=postgres-crypto \
  POSTGRES_PORT=5432 POSTGRES_USER=crypto POSTGRES_PASSWORD=crypto123 \
  POSTGRES_DB=crypto_db SPARK_MASTER=local[2] \
  SPARK_CHECKPOINT_DIR=/opt/spark-checkpoints \
  /opt/spark/bin/spark-submit \
    --master local[2] \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.6.0 \
    /opt/spark-apps/streaming_job.py >> "$LOG" 2>&1
  exit_code=$?

  if tail -n +"$((batch_start_line + 1))" "$LOG" | grep -q "All streaming queries started"; then
    crash_count=0
  else
    crash_count=$((crash_count + 1))
  fi

  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Spark job exited ($exit_code). Consecutive failed-to-start count: $crash_count." >> "$LOG"
  record_failure "$exit_code" "$crash_count"

  if [ "$crash_count" -ge "$MAX_FAST_RETRIES" ]; then
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $crash_count consecutive failures to start — backing off to ${SLOW_RETRY_DELAY}s retries." >> "$LOG"
    sleep $SLOW_RETRY_DELAY
  else
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Restarting in ${FAST_RETRY_DELAY}s..." >> "$LOG"
    sleep $FAST_RETRY_DELAY
  fi
done
