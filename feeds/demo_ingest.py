"""
Demo ingest: runs the CoinGecko feed for N polls, publishes to Kafka,
and also writes directly to raw.price_ticks so results are visible
without the Spark streaming job.
"""

import json
import logging
import time
import uuid
import sys
import os

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import KAFKA_CONFIG, DB_CONFIG
from feeds.coingecko_feed import fetch_prices, build_price_events, get_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INSERT_SQL = """
INSERT INTO raw.price_ticks
    (event_id, symbol, price_usd, volume_24h_usd, change_24h_pct,
     source, event_time, kafka_timestamp, captured_at)
VALUES
    (%(event_id)s, %(symbol)s, %(price_usd)s, %(volume_24h_usd)s, %(change_24h_pct)s,
     %(source)s, NOW(), NOW(), %(captured_at)s)
"""


def run_demo(max_polls: int = 3):
    producer = get_producer()
    topic = KAFKA_CONFIG["prices_topic"]

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    total_rows = 0

    for poll in range(1, max_polls + 1):
        logger.info("--- Poll %d/%d ---", poll, max_polls)
        raw = fetch_prices()
        events = build_price_events(raw)

        for event in events:
            producer.send(topic, key=event["symbol"], value=event)

        producer.flush()
        logger.info("Kafka: published %d events", len(events))

        psycopg2.extras.execute_batch(cur, INSERT_SQL, events)
        total_rows += len(events)
        logger.info("Postgres: inserted %d rows (running total: %d)", len(events), total_rows)

        if poll < max_polls:
            time.sleep(12)

    producer.close()
    cur.close()
    conn.close()
    logger.info("Done. %d total rows inserted into raw.price_ticks.", total_rows)


if __name__ == "__main__":
    run_demo()
