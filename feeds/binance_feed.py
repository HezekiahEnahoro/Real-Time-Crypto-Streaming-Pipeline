"""
Binance WebSocket feed.

Binance provides free public WebSocket streams — no API key needed.
We subscribe to the @ticker stream for each symbol which gives us
live price updates as trades happen.

Key concept: WebSocket vs REST polling
  REST polling: you ask → server responds → you wait → repeat
  WebSocket: server pushes data to you as it happens → true real-time
"""

import json
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import KAFKA_CONFIG, SYMBOLS

logger = logging.getLogger(__name__)

BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"

BINANCE_PAIRS = {
    "BTC": "btcusdt", "ETH": "ethusdt", "BNB": "bnbusdt",
    "SOL": "solusdt", "ADA": "adausdt", "MATIC": "maticusdt",
    "AVAX": "avaxusdt", "DOT": "dotusdt",
}


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        retries=3,
    )


def parse_ticker_event(data: dict, symbol: str) -> dict:
    """Parse Binance 24hr ticker stream into a standardised price event."""
    return {
        "event_id":         str(uuid.uuid4()),
        "symbol":           symbol,
        "price_usd":        float(data.get("c", 0)),   # current price
        "price_open":       float(data.get("o", 0)),   # 24h open
        "price_high":       float(data.get("h", 0)),   # 24h high
        "price_low":        float(data.get("l", 0)),   # 24h low
        "volume_24h":       float(data.get("v", 0)),   # base asset volume
        "volume_24h_usd":   float(data.get("q", 0)),   # quote (USD) volume
        "change_24h_pct":   float(data.get("P", 0)),   # % change
        "trade_count_24h":  int(data.get("n", 0)),
        "source":           "binance_ws",
        "captured_at":      datetime.now(timezone.utc).isoformat(),
    }


async def stream_symbol(symbol: str, pair: str, producer: KafkaProducer):
    """Stream ticker updates for one symbol."""
    try:
        import websockets
        url = f"{BINANCE_WS_BASE}/{pair}@ticker"

        async with websockets.connect(url, ping_interval=20) as ws:
            logger.info("Connected to Binance stream: %s", pair)
            async for message in ws:
                data  = json.loads(message)
                event = parse_ticker_event(data, symbol)
                producer.send(
                    KAFKA_CONFIG["prices_topic"],
                    key=symbol,
                    value=event,
                )
    except Exception as e:
        logger.error("Binance stream error [%s]: %s", symbol, e)


async def run_all_streams():
    """Open WebSocket streams for all tracked symbols concurrently."""
    producer = get_producer()
    tasks = [
        stream_symbol(symbol, pair, producer)
        for symbol, pair in BINANCE_PAIRS.items()
    ]
    await asyncio.gather(*tasks)
    producer.close()


def run_feed():
    """Entry point — runs the async WebSocket event loop."""
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    logger.info("Starting Binance WebSocket feed for %d symbols", len(BINANCE_PAIRS))
    asyncio.run(run_all_streams())


if __name__ == "__main__":
    run_feed()
