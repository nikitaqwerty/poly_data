#!/usr/bin/env python3
"""
Migration Script: Fix Trades Table Schema

This script fixes the data loss issue by recreating the trades table
with the correct ORDER BY clause that includes maker and taker.

WHAT THIS DOES:
1. Backs up current trades table
2. Drops the old trades table
3. Creates new trades table with correct schema
4. You'll need to re-run your pipeline to reload data

WARNING: This will drop your current trades table!
Make sure you have backups or can re-ingest the data.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common.config import clickhouse_config
from common.logging_config import setup_logging

logger = setup_logging("migration")


def main():
    """Run migration"""
    logger.info("=" * 60)
    logger.info("🔧 Migration: Fix Trades Table Schema")
    logger.info("=" * 60)

    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        username=clickhouse_config.user,
        password=clickhouse_config.password,
        database=clickhouse_config.database,
    )

    # Check if trades table exists
    result = client.query(
        f"SELECT count() FROM system.tables WHERE database='{clickhouse_config.database}' AND name='trades'"
    )
    if result.result_rows[0][0] == 0:
        logger.info("No trades table exists. Creating fresh table...")
    else:
        # Get current row count
        count_result = client.query(
            f"SELECT count() FROM {clickhouse_config.database}.trades"
        )
        current_count = count_result.result_rows[0][0]
        logger.warning("⚠️  Current trades table has %d rows", current_count)
        logger.warning("⚠️  This migration will DROP the trades table!")
        logger.warning("⚠️  You'll need to re-run your pipeline to reload data.")

        response = input("\nType 'YES' to continue: ")
        if response != "YES":
            logger.info("Migration cancelled.")
            return

        # Create backup table (optional, for safety)
        logger.info("Creating backup table 'trades_backup_old_schema'...")
        try:
            client.command(
                f"DROP TABLE IF EXISTS {clickhouse_config.database}.trades_backup_old_schema"
            )
            client.command(
                f"CREATE TABLE {clickhouse_config.database}.trades_backup_old_schema AS {clickhouse_config.database}.trades"
            )
            client.command(
                f"INSERT INTO {clickhouse_config.database}.trades_backup_old_schema SELECT * FROM {clickhouse_config.database}.trades"
            )
            logger.info("✓ Backup created with %d rows", current_count)
        except Exception as e:
            logger.error("Failed to create backup: %s", e)
            logger.error("Aborting migration for safety.")
            return

        # Drop old table
        logger.info("Dropping old trades table...")
        client.command(f"DROP TABLE {clickhouse_config.database}.trades")
        logger.info("✓ Old table dropped")

    # Create new table with correct schema
    logger.info("Creating new trades table with correct schema...")

    schema = f"""
    CREATE TABLE {clickhouse_config.database}.trades (
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
    ORDER BY (timestamp, market_id, transactionHash, maker, taker)
    """

    client.command(schema)
    logger.info("✓ New trades table created with correct ORDER BY")

    logger.info("=" * 60)
    logger.info("✅ Migration complete!")
    logger.info("")
    logger.info("NEXT STEPS:")
    logger.info("1. Restart your pipeline containers to reload data")
    logger.info("2. Monitor the ingestion to ensure all trades are captured")
    logger.info("3. Verify row counts match expected values")
    logger.info("")
    logger.info("If you need to restore the backup:")
    logger.info("   DROP TABLE polymarket.trades;")
    logger.info(
        "   RENAME TABLE polymarket.trades_backup_old_schema TO polymarket.trades;"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
