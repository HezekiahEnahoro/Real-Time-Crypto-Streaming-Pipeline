from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import sys
sys.path.insert(0, "/opt/airflow")


def on_task_failure(context):
    import psycopg2
    from config.settings import DB_CONFIG
    ti = context["task_instance"]
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO monitoring.pipeline_failures "
                    "(dag_id, task_id, execution_date, error_message) VALUES (%s,%s,%s,%s)",
                    (ti.dag_id, ti.task_id, context["execution_date"],
                     str(context.get("exception", "unknown")))
                )
    except Exception as e:
        print(f"Failed to record pipeline failure: {e}")


default_args = {
    "owner":              "data-engineering",
    "depends_on_past":    False,
    "email_on_failure":   False,
    "retries":            2,
    "retry_delay":        timedelta(minutes=3),
    "on_failure_callback": on_task_failure,
}

# ─── Feed DAG — starts the CoinGecko poller ──────────────────────────────────
feed_dag = DAG(
    dag_id="crypto_price_feed",
    default_args=default_args,
    description="Polls CoinGecko and publishes prices to Kafka",
    schedule_interval="*/2 * * * *",  # every 2 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "kafka", "streaming"],
)


def run_coingecko_poll(**context):
    from feeds.coingecko_feed import fetch_prices, build_price_events, get_producer
    from config.settings import KAFKA_CONFIG
    import json

    raw    = fetch_prices()
    events = build_price_events(raw)
    producer = get_producer()

    for event in events:
        producer.send(KAFKA_CONFIG["prices_topic"], key=event["symbol"], value=event)
    producer.flush()
    producer.close()

    context["ti"].xcom_push(key="event_count", value=len(events))
    print(f"Published {len(events)} price events to Kafka")


poll_task = PythonOperator(
    task_id="poll_coingecko",
    python_callable=run_coingecko_poll,
    dag=feed_dag,
)

# ─── Analytics DAG — hourly dbt run ──────────────────────────────────────────
analytics_dag = DAG(
    dag_id="crypto_analytics",
    default_args=default_args,
    description="Hourly dbt models for crypto OHLCV and moving averages",
    schedule_interval="5 * * * *",   # 5 minutes past every hour
    start_date=days_ago(1),
    catchup=False,
    tags=["crypto", "dbt", "analytics"],
)


def check_data_freshness(**context):
    """Verify Spark has written data in the last 10 minutes before running dbt."""
    import json
    import psycopg2
    from config.settings import DB_CONFIG

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(computed_at), COUNT(*)
                FROM raw.price_windows
                WHERE computed_at > NOW() - INTERVAL '10 minutes'
            """)
            max_ts, count = cur.fetchone()

            status = "ok" if count > 0 else "error"
            cur.execute(
                "INSERT INTO monitoring.health_checks (check_name, status, details) "
                "VALUES (%s, %s, %s)",
                ("price_windows_freshness", status,
                 json.dumps({"window_count": count, "latest": str(max_ts)}))
            )

    if count == 0:
        raise ValueError(
            "No fresh data in raw.price_windows in the last 10 minutes. "
            "Check that the Spark streaming job is running."
        )

    print(f"Freshness check passed: {count} windows, latest at {max_ts}")
    context["ti"].xcom_push(key="fresh_window_count", value=count)


freshness_task = PythonOperator(
    task_id="check_data_freshness",
    python_callable=check_data_freshness,
    dag=analytics_dag,
)

run_dbt_task = BashOperator(
    task_id="run_dbt",
    bash_command=(
        "cd /opt/airflow/dbt_project && "
        "dbt run --profiles-dir /opt/airflow/dbt_project --target prod"
    ),
    dag=analytics_dag,
)

dbt_tests_task = BashOperator(
    task_id="dbt_tests",
    bash_command=(
        "cd /opt/airflow/dbt_project && "
        "dbt test --profiles-dir /opt/airflow/dbt_project --target prod"
    ),
    dag=analytics_dag,
)

freshness_task >> run_dbt_task >> dbt_tests_task
