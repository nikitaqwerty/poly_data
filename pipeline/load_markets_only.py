#!/usr/bin/env python3
"""
Load Markets Only to ClickHouse

Drops existing markets table and reloads from CSV:
- markets.csv -> markets table (fresh)
"""

import csv
import sys
from pathlib import Path
from typing import List, Dict
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from common.config import clickhouse_config
from common.logging_config import setup_logging

logger = setup_logging("markets_loader")


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


def drop_markets_table(client):
    """Drop the markets table if it exists"""
    logger.info("Dropping existing markets table...")
    try:
        client.command(f"DROP TABLE IF EXISTS {clickhouse_config.database}.markets")
        logger.info("✓ Markets table dropped")
    except Exception as e:
        logger.error("Error dropping markets table: %s", e)
        raise


def create_markets_table(client):
    """Create markets table"""
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
    logger.info("✓ Markets table created")


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


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Drop and reload markets table from CSV"
    )
    parser.add_argument(
        "--markets",
        default="markets.csv",
        help="Path to markets CSV file (default: markets.csv)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for inserts (default: 1000)",
    )
    parser.add_argument(
        "--no-drop",
        action="store_true",
        help="Skip dropping the table (append mode)",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("🏪 Markets-Only Loader for ClickHouse")
    logger.info("=" * 60)

    try:
        # Get ClickHouse client and test connection
        client = get_clickhouse_client()
        version = client.command("SELECT version()")
        logger.info("✓ Connected to ClickHouse version: %s", version)

        # Drop existing table unless --no-drop is specified
        if not args.no_drop:
            drop_markets_table(client)
        else:
            logger.info("Skipping table drop (--no-drop specified)")

        # Create fresh table
        create_markets_table(client)

        # Load markets
        logger.info("")
        logger.info("Loading markets from: %s", args.markets)
        markets_count = load_markets_from_csv(args.markets, args.batch_size)
        logger.info("Markets loaded: %d", markets_count)

        logger.info("")
        logger.info("=" * 60)
        logger.info("✅ Markets loading complete!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error("❌ Fatal error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
