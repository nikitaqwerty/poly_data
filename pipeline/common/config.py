"""Configuration for the pipeline"""

import os
from dataclasses import dataclass
from pathlib import Path


from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)


@dataclass
class RedisConfig:
    """Redis connection configuration"""

    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", 6379))
    db: int = int(os.getenv("REDIS_DB", 0))
    password: str = os.getenv("REDIS_PASSWORD", "")

    # Stream and key names
    MARKETS_STREAM = "stream:markets"
    POLYMARKET_EVENTS_STREAM = "stream:polymarket_events"
    EVENTS_STREAM = "stream:order_events"
    TRADES_STREAM = "stream:trades"
    ADDRESS_METADATA_STREAM = "stream:address_metadata"
    EVENTS_DLQ_STREAM = "stream:order_events_dlq"  # Dead letter queue for failed events

    # Consumer groups
    MARKETS_GROUP = "markets_writers"
    POLYMARKET_EVENTS_GROUP = "polymarket_events_writers"
    EVENTS_GROUP = "events_processors"
    TRADES_GROUP = "trades_writers"
    ADDRESS_METADATA_GROUP = "address_metadata_writers"

    # State keys
    STATE_PREFIX = "state:"

    # Stream retention settings
    # Maximum number of messages to keep in a stream (approximate)
    STREAM_MAX_LENGTH = 100000
    # Minimum time (in milliseconds) to keep messages before trimming
    STREAM_MIN_IDLE_TIME = 3600000  # 1 hour
    # How often to run stream cleanup (in seconds)
    STREAM_CLEANUP_INTERVAL = 300  # 5 minutes


@dataclass
class ClickHouseConfig:
    """ClickHouse connection configuration"""

    host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    port: int = int(os.getenv("CLICKHOUSE_PORT", 8123))
    user: str = os.getenv("CLICKHOUSE_USER", "default")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    database: str = os.getenv("CLICKHOUSE_DATABASE", "polymarket")


@dataclass
class APIConfig:
    """API endpoints and configuration"""

    polymarket_markets_url: str = "https://gamma-api.polymarket.com/markets"
    polymarket_events_url: str = "https://gamma-api.polymarket.com/events"
    goldsky_url: str = (
        "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
    )

    # Rate limiting and batch sizes
    polymarket_batch_size: int = 500  # max is 500
    polymarket_poll_interval: int = 30  # seconds
    goldsky_batch_size: int = 1000  # max is 1000
    goldsky_poll_interval: int = 5  # seconds
    address_checker_batch_size: int = 500  # addresses to check per batch
    address_checker_poll_interval: int = (
        60  # seconds (wait between checks when caught up)
    )
    request_timeout: int = 30  # seconds


@dataclass
class ProcessingConfig:
    """Processing configuration"""

    # Trade processor config - increased to match high throughput needs
    trade_processor_batch_size: int = 50000  # Process 50k order events at a time
    trade_processor_interval: int = 1  # Check more frequently (was 5 seconds)

    # ClickHouse writer config - optimized for high throughput
    # Read more messages per call to fill buffer faster
    clickhouse_writer_batch_size: int = 50000  # ClickHouse handles large batches well
    clickhouse_writer_interval: int = 10  # seconds
    clickhouse_writer_max_wait: int = 10  # Flush more frequently to reduce latency

    # Retry configuration for failed messages
    max_retry_attempts: int = 5  # Maximum number of retry attempts before moving to DLQ
    retry_base_delay: int = (
        60  # Base delay in seconds (exponential backoff: 1min, 2min, 4min, 8min, 16min)
    )
    retry_claim_interval: int = (
        30  # How often to check for messages that need retry (seconds)
    )


# Singleton instances
redis_config = RedisConfig()
clickhouse_config = ClickHouseConfig()
api_config = APIConfig()
processing_config = ProcessingConfig()
