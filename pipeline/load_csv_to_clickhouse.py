#!/usr/bin/env python3
"""
Load CSV files into ClickHouse

Initializes ClickHouse tables from CSV files:
- processed/trades.csv -> trades table
- markets.csv -> markets table
"""

import csv
import sys
from pathlib import Path
from typing import List, Dict
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from common.config import clickhouse_config
from common.logging_config import setup_logging
from common.redis_queue import RedisQueue

logger = setup_logging("csv_loader")

# State keys used by ingesters
POLYMARKET_MARKETS_STATE_KEY = "polymarket_markets_offset"
GOLDSKY_STATE_KEY = "goldsky_last_timestamp"
POLYMARKET_EVENTS_STATE_KEY = "polymarket_events_offset"


def get_clickhouse_client():
    """Create ClickHouse client connection"""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        username=clickhouse_config.user,
        password=clickhouse_config.password,
        database=clickhouse_config.database,
    )
    return client


def create_tables_if_needed(client):
    """Create tables if they don't exist"""
    # Create markets table
    markets_schema = f"""
    CREATE TABLE IF NOT EXISTS {clickhouse_config.database}.markets (
        createdAt DateTime,
        id UInt64,
        question String,
        answer1 String,
        answer2 String,
        neg_risk Boolean,
        market_slug String,
        token1 String,
        token2 String,
        condition_id String,
        volume Float64,
        ticker String,
        event_slug String,
        closedTime Nullable(DateTime)
    ) ENGINE = MergeTree()
    ORDER BY (id, createdAt)
    """
    client.command(markets_schema)
    logger.info("✓ Markets table ready")

    # Create trades table
    trades_schema = f"""
    CREATE TABLE IF NOT EXISTS {clickhouse_config.database}.trades (
        timestamp DateTime,
        market_id String,
        maker String,
        taker String,
        nonusdc_side String,
        maker_direction String,
        taker_direction String,
        price Float64,
        usd_amount Float64,
        token_amount Float64,
        transactionHash String
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (timestamp, market_id, transactionHash)
    """
    client.command(trades_schema)
    logger.info("✓ Trades table ready")


def load_markets_from_csv(csv_path: str, batch_size: int = 1000) -> int:
    """
    Load markets from CSV file into ClickHouse

    Expected CSV columns:
    - createdAt (ISO datetime)
    - id
    - question
    - answer1
    - answer2
    - neg_risk (bool)
    - market_slug
    - token1
    - token2
    - condition_id
    - volume (float)
    - ticker
    - event_slug
    - closedTime (ISO datetime, nullable)

    Args:
        csv_path: Path to CSV file
        batch_size: Number of records to insert per batch

    Returns:
        Total number of records loaded
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        logger.error("Markets CSV file not found: %s", csv_path)
        return 0

    client = get_clickhouse_client()
    total_loaded = 0
    batch = []

    logger.info("Loading markets from %s...", csv_path)

    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row_num, row in enumerate(reader, start=1):
                try:
                    # Parse datetime strings
                    created_at = None
                    if row.get("createdAt"):
                        try:
                            created_at = datetime.fromisoformat(
                                row["createdAt"].replace("Z", "+00:00")
                            )
                        except Exception as e:
                            logger.warning(
                                "Row %d: Invalid createdAt '%s': %s",
                                row_num,
                                row.get("createdAt"),
                                e,
                            )

                    closed_time = None
                    if row.get("closedTime"):
                        try:
                            closed_time = datetime.fromisoformat(
                                row["closedTime"].replace("Z", "+00:00")
                            )
                        except Exception as e:
                            logger.debug(
                                "Row %d: Invalid closedTime '%s': %s",
                                row_num,
                                row.get("closedTime"),
                                e,
                            )

                    # Build row tuple
                    # Handle volume - convert empty string to 0
                    volume_str = str(row.get("volume", "0")).strip()
                    volume = float(volume_str) if volume_str else 0.0

                    # Handle id - convert to integer
                    id_str = str(row.get("id", "0")).strip()
                    market_id = int(id_str) if id_str and id_str.isdigit() else 0

                    record = (
                        created_at,
                        market_id,
                        str(row.get("question", "")),
                        str(row.get("answer1", "")),
                        str(row.get("answer2", "")),
                        str(row.get("neg_risk", "false")).lower()
                        in ["true", "1", "yes"],
                        str(row.get("market_slug", "")),
                        str(row.get("token1", "")),
                        str(row.get("token2", "")),
                        str(row.get("condition_id", "")),
                        volume,
                        str(row.get("ticker", "")),
                        str(row.get("event_slug", "")),
                        closed_time,
                    )

                    batch.append(record)

                    # Insert batch when full
                    if len(batch) >= batch_size:
                        client.insert(
                            f"{clickhouse_config.database}.markets",
                            batch,
                            column_names=[
                                "createdAt",
                                "id",
                                "question",
                                "answer1",
                                "answer2",
                                "neg_risk",
                                "market_slug",
                                "token1",
                                "token2",
                                "condition_id",
                                "volume",
                                "ticker",
                                "event_slug",
                                "closedTime",
                            ],
                        )
                        total_loaded += len(batch)
                        logger.info("✓ Loaded %d markets (batch)", total_loaded)
                        batch.clear()

                except Exception as e:
                    logger.error("Error processing row %d: %s", row_num, e)
                    logger.debug("Row data: %s", row)
                    continue

        # Insert remaining records
        if batch:
            client.insert(
                f"{clickhouse_config.database}.markets",
                batch,
                column_names=[
                    "createdAt",
                    "id",
                    "question",
                    "answer1",
                    "answer2",
                    "neg_risk",
                    "market_slug",
                    "token1",
                    "token2",
                    "condition_id",
                    "volume",
                    "ticker",
                    "event_slug",
                    "closedTime",
                ],
            )
            total_loaded += len(batch)

        logger.info("✅ Total markets loaded: %d", total_loaded)
        return total_loaded

    except Exception as e:
        logger.error("Fatal error loading markets: %s", e, exc_info=True)
        return total_loaded


def load_trades_from_csv(csv_path: str, batch_size: int = 5000) -> tuple[int, int]:
    """
    Load trades from CSV file into ClickHouse

    Expected CSV columns:
    - timestamp (ISO datetime)
    - market_id
    - maker
    - taker
    - nonusdc_side
    - maker_direction
    - taker_direction
    - price (float)
    - usd_amount (float)
    - token_amount (float)
    - transactionHash

    Args:
        csv_path: Path to CSV file
        batch_size: Number of records to insert per batch

    Returns:
        Tuple of (total_records_loaded, max_timestamp)
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        logger.error("Trades CSV file not found: %s", csv_path)
        return 0, 0

    client = get_clickhouse_client()
    total_loaded = 0
    batch = []
    max_timestamp = 0

    logger.info("Loading trades from %s...", csv_path)

    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row_num, row in enumerate(reader, start=1):
                try:
                    # Parse timestamp
                    timestamp = None
                    if row.get("timestamp"):
                        try:
                            timestamp = datetime.fromisoformat(
                                row["timestamp"].replace("Z", "+00:00")
                            )
                        except Exception as e:
                            logger.warning(
                                "Row %d: Invalid timestamp '%s': %s",
                                row_num,
                                row.get("timestamp"),
                                e,
                            )
                            continue

                    if not timestamp:
                        logger.warning("Row %d: Missing timestamp, skipping", row_num)
                        continue

                    # Track max timestamp for Redis state
                    timestamp_unix = int(timestamp.timestamp())
                    if timestamp_unix > max_timestamp:
                        max_timestamp = timestamp_unix

                    # Build row tuple
                    # Handle numeric fields - convert empty strings to 0
                    price_str = str(row.get("price", "0")).strip()
                    price = float(price_str) if price_str else 0.0

                    usd_amount_str = str(row.get("usd_amount", "0")).strip()
                    usd_amount = float(usd_amount_str) if usd_amount_str else 0.0

                    token_amount_str = str(row.get("token_amount", "0")).strip()
                    token_amount = float(token_amount_str) if token_amount_str else 0.0

                    record = (
                        timestamp,
                        str(row.get("market_id", "")),
                        str(row.get("maker", "")),
                        str(row.get("taker", "")),
                        str(row.get("nonusdc_side", "")),
                        str(row.get("maker_direction", "")),
                        str(row.get("taker_direction", "")),
                        price,
                        usd_amount,
                        token_amount,
                        str(row.get("transactionHash", "")),
                    )

                    batch.append(record)

                    # Insert batch when full
                    if len(batch) >= batch_size:
                        client.insert(
                            f"{clickhouse_config.database}.trades",
                            batch,
                            column_names=[
                                "timestamp",
                                "market_id",
                                "maker",
                                "taker",
                                "nonusdc_side",
                                "maker_direction",
                                "taker_direction",
                                "price",
                                "usd_amount",
                                "token_amount",
                                "transactionHash",
                            ],
                        )
                        total_loaded += len(batch)
                        logger.info("✓ Loaded %d trades (batch)", total_loaded)
                        batch.clear()

                except Exception as e:
                    logger.error("Error processing row %d: %s", row_num, e)
                    logger.debug("Row data: %s", row)
                    continue

        # Insert remaining records
        if batch:
            client.insert(
                f"{clickhouse_config.database}.trades",
                batch,
                column_names=[
                    "timestamp",
                    "market_id",
                    "maker",
                    "taker",
                    "nonusdc_side",
                    "maker_direction",
                    "taker_direction",
                    "price",
                    "usd_amount",
                    "token_amount",
                    "transactionHash",
                ],
            )
            total_loaded += len(batch)

        logger.info("✅ Total trades loaded: %d", total_loaded)
        logger.info("Max timestamp: %d", max_timestamp)
        return total_loaded, max_timestamp

    except Exception as e:
        logger.error("Fatal error loading trades: %s", e, exc_info=True)
        return total_loaded, max_timestamp


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Load CSV files into ClickHouse tables"
    )
    parser.add_argument(
        "--trades",
        default="processed/trades.csv",
        help="Path to trades CSV file (default: processed/trades.csv)",
    )
    parser.add_argument(
        "--markets",
        default="markets.csv",
        help="Path to markets CSV file (default: markets.csv)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Batch size for inserts (default: 5000)",
    )
    parser.add_argument(
        "--skip-trades",
        action="store_true",
        help="Skip loading trades",
    )
    parser.add_argument(
        "--skip-markets",
        action="store_true",
        help="Skip loading markets",
    )
    parser.add_argument(
        "--trades-only",
        action="store_true",
        help="Load only trades (skip markets)",
    )

    args = parser.parse_args()

    # Handle --trades-only flag
    if args.trades_only:
        args.skip_markets = True

    logger.info("=" * 60)
    logger.info("📊 CSV to ClickHouse Loader")
    logger.info("=" * 60)

    try:
        # Get ClickHouse client and test connection
        client = get_clickhouse_client()
        version = client.command("SELECT version()")
        logger.info("✓ Connected to ClickHouse version: %s", version)

        # Create tables if needed
        create_tables_if_needed(client)

        # Initialize Redis queue for state management
        queue = RedisQueue()
        logger.info("✓ Connected to Redis for state management")

        # Load markets
        markets_count = 0
        if not args.skip_markets:
            logger.info("")
            logger.info("Loading markets from: %s", args.markets)
            markets_count = load_markets_from_csv(args.markets, args.batch_size)
            logger.info("Markets loaded: %d", markets_count)

            # Set Redis state for polymarket markets ingester
            if markets_count > 0:
                queue.set_state(POLYMARKET_MARKETS_STATE_KEY, markets_count)
                logger.info(
                    "✓ Set Redis state '%s' = %d",
                    POLYMARKET_MARKETS_STATE_KEY,
                    markets_count,
                )

        # Load trades
        trades_count = 0
        max_timestamp = 0
        if not args.skip_trades:
            logger.info("")
            logger.info("Loading trades from: %s", args.trades)
            trades_count, max_timestamp = load_trades_from_csv(
                args.trades, args.batch_size
            )
            logger.info("Trades loaded: %d", trades_count)

            # Set Redis state for goldsky ingester
            if trades_count > 0 and max_timestamp > 0:
                queue.set_state(GOLDSKY_STATE_KEY, max_timestamp)
                logger.info(
                    "✓ Set Redis state '%s' = %d",
                    GOLDSKY_STATE_KEY,
                    max_timestamp,
                )

        # Set polymarket events offset to 0 (events not typically loaded from CSV)
        queue.set_state(POLYMARKET_EVENTS_STATE_KEY, 0)
        logger.info(
            "✓ Set Redis state '%s' = 0 (events not loaded from CSV)",
            POLYMARKET_EVENTS_STATE_KEY,
        )

        # Close Redis connection
        queue.close()

        logger.info("")
        logger.info("=" * 60)
        logger.info("✅ CSV loading complete!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error("❌ Fatal error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
