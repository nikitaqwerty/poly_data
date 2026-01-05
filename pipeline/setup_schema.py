#!/usr/bin/env python3
"""
ClickHouse Schema Setup

Creates the necessary tables in ClickHouse for the pipeline
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common.config import clickhouse_config
from common.logging_config import setup_logging

logger = setup_logging("schema_setup")


def create_database():
    """Create the database if it doesn't exist"""
    import clickhouse_connect

    # Connect without specifying database
    client = clickhouse_connect.get_client(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        username=clickhouse_config.user,
        password=clickhouse_config.password,
    )

    # Create database
    client.command(f"CREATE DATABASE IF NOT EXISTS {clickhouse_config.database}")
    logger.info("✓ Database '%s' ready", clickhouse_config.database)


def create_markets_table():
    """Create markets table"""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        username=clickhouse_config.user,
        password=clickhouse_config.password,
        database=clickhouse_config.database,
    )

    schema = f"""
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
        closedTime Nullable(DateTime),
        modifiedDateTime DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(modifiedDateTime)
    ORDER BY (id, createdAt)
    """

    client.command(schema)
    logger.info("✓ Table 'markets' ready")


def create_polymarket_events_table():
    """Create polymarket events table"""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        username=clickhouse_config.user,
        password=clickhouse_config.password,
        database=clickhouse_config.database,
    )

    schema = f"""
    CREATE TABLE IF NOT EXISTS {clickhouse_config.database}.polymarket_events (
        id String,
        slug String,
        ticker String,
        title String,
        description String,
        createdAt Nullable(DateTime),
        startDate Nullable(DateTime),
        endDate Nullable(DateTime),
        tags Array(LowCardinality(String)),
        markets_count UInt32,
        active Boolean,
        closed Boolean,
        archived Boolean,
        volume Float64,
        liquidity Float64,
        modifiedDateTime DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(modifiedDateTime)
    ORDER BY (slug, id)
    """

    client.command(schema)
    logger.info("✓ Table 'polymarket_events' ready")


def create_trades_table():
    """Create trades table"""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        username=clickhouse_config.user,
        password=clickhouse_config.password,
        database=clickhouse_config.database,
    )

    schema = f"""
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
        transactionHash String,
        modifiedDateTime DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(modifiedDateTime)
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (timestamp, market_id, transactionHash)
    """

    client.command(schema)
    logger.info("✓ Table 'trades' ready")


def main():
    """Run schema setup"""
    logger.info("=" * 60)
    logger.info("🏗️  ClickHouse Schema Setup")
    logger.info("=" * 60)

    try:
        create_database()
        create_markets_table()
        create_polymarket_events_table()
        create_trades_table()

        logger.info("=" * 60)
        logger.info("✅ Schema setup complete!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error("❌ Schema setup failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
