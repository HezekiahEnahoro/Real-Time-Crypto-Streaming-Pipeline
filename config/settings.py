import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5434)),
    "database": os.getenv("POSTGRES_DB", "crypto_db"),
    "user":     os.getenv("POSTGRES_USER", "crypto"),
    "password": os.getenv("POSTGRES_PASSWORD", "crypto123"),
}

AWS_CONFIG = {
    "aws_access_key_id":     os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name":           os.getenv("AWS_REGION", "us-east-1"),
}

S3_BUCKET = os.getenv("S3_BUCKET", "crypto-streaming-pipeline")

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "prices_topic":      "crypto.prices",
    "trades_topic":      "crypto.trades",
    "consumer_group":    "crypto-spark-consumer",
}

SPARK_CONFIG = {
    "master":           os.getenv("SPARK_MASTER", "local[*]"),
    "checkpoint_dir":   os.getenv("SPARK_CHECKPOINT_DIR", "./checkpoints"),
    "trigger_seconds":  5,        # micro-batch interval
    "watermark_minutes": 2,       # late data tolerance
}

# Coins to track
SYMBOLS = ["BTC", "ETH", "BNB", "SOL", "ADA", "MATIC", "AVAX", "DOT"]

COINGECKO_IDS = {
    "BTC":   "bitcoin",
    "ETH":   "ethereum",
    "BNB":   "binancecoin",
    "SOL":   "solana",
    "ADA":   "cardano",
    "MATIC": "polygon-ecosystem-token",
    "AVAX":  "avalanche-2",
    "DOT":   "polkadot",
}

# Alert thresholds
PRICE_CHANGE_ALERT_PCT = 3.0   # alert if price moves >3% in a 5-min window
VOLUME_SPIKE_MULTIPLIER = 3.0  # alert if volume is 3x the rolling average
