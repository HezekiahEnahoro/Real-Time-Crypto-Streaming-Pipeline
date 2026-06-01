"""
CoinGecko price feed producer.

CoinGecko's free API gives us live prices for up to 250 coins.
No API key needed for basic usage.
We poll every 10 seconds and publish each coin as a Kafka message.
"""

from __future__ import annotations

import json
import time
import logging
import uuid
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import KAFKA_CONFIG, COINGECKO_IDS, SYMBOLS

logger = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
POLL_INTERVAL = 12  # seconds — CoinGecko free tier: 30 calls/min


def fetch_prices() -> dict:
    """Fetch current USD prices for all tracked coins."""
    ids = ",".join(COINGECKO_IDS.values())
    params = {
        "ids":             ids,
        "vs_currencies":   "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }
    resp = requests.get(COINGECKO_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def build_price_events(raw: dict) -> list[dict]:
    """Convert CoinGecko response into structured price events."""
    events = []
    id_to_symbol = {v: k for k, v in COINGECKO_IDS.items()}
    captured_at = datetime.now(timezone.utc).isoformat()

    for coin_id, data in raw.items():
        symbol = id_to_symbol.get(coin_id, coin_id.upper())
        events.append({
            "event_id":       str(uuid.uuid4()),
            "symbol":         symbol,
            "coin_id":        coin_id,
            "price_usd":      data.get("usd"),
            "volume_24h_usd": data.get("usd_24h_vol"),
            "change_24h_pct": data.get("usd_24h_change"),
            "source":         "coingecko",
            "captured_at":    captured_at,
        })
    return events


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        max_block_ms=10_000,
    )


def run_feed(max_polls: int = None):
    """
    Main loop: poll CoinGecko → publish to Kafka.
    Set max_polls for testing; leave None for continuous run.
    """
    producer = get_producer()
    topic    = KAFKA_CONFIG["prices_topic"]
    polls    = 0

    logger.info("Starting CoinGecko feed — publishing to topic '%s'", topic)

    while True:
        try:
            raw    = fetch_prices()
            events = build_price_events(raw)

            for event in events:
                producer.send(
                    topic,
                    key=event["symbol"],
                    value=event,
                )

            producer.flush()
            logger.info("Published %d price events | BTC: $%s",
                        len(events),
                        next((e["price_usd"] for e in events if e["symbol"] == "BTC"), "N/A"))

            polls += 1
            if max_polls and polls >= max_polls:
                break

            time.sleep(POLL_INTERVAL)

        except requests.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limited by CoinGecko — sleeping 60s")
                time.sleep(60)
            else:
                logger.error("HTTP error: %s", e)
                time.sleep(15)
        except KafkaError as e:
            logger.error("Kafka error: %s — retrying in 10s", e)
            time.sleep(10)
        except Exception as e:
            logger.error("Feed error: %s", e)
            time.sleep(10)

    producer.close()
    logger.info("Feed stopped after %d polls", polls)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    run_feed()
