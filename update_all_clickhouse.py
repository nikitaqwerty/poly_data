#!/usr/bin/env python3
"""
Complete update pipeline with ClickHouse integration

This script:
1. Updates markets from Polymarket API
2. Updates order-filled events from Goldsky
3. Processes orders into trades
4. Loads all new data into ClickHouse
"""
from update_utils.update_markets import update_markets
from update_utils.update_goldsky import update_goldsky
from update_utils.process_live import process_live
from clickhouse_utils.incremental_load import run_incremental_load
from clickhouse_utils.config import test_connection


def main():
    """
    Run the complete data pipeline with ClickHouse integration
    """
    print("\n" + "=" * 70)
    print("🚀 Polymarket Data Pipeline with ClickHouse")
    print("=" * 70)

    # First, test ClickHouse connection
    print("\n1️⃣  Testing ClickHouse connection...")
    if not test_connection():
        print("\n❌ ClickHouse connection failed!")
        print("Please ensure ClickHouse is running and configured correctly.")
        print("Check environment variables: CLICKHOUSE_HOST, CLICKHOUSE_PORT, etc.")
        return

    # Step 1: Update markets
    print("\n2️⃣  Updating markets from Polymarket API...")
    try:
        update_markets()
    except Exception as e:
        print(f"Error updating markets: {e}")
        return

    # Step 2: Update Goldsky order events
    print("\n3️⃣  Updating order events from Goldsky...")
    try:
        update_goldsky()
    except Exception as e:
        print(f"Error updating Goldsky data: {e}")
        return

    # Step 3: Process trades
    print("\n4️⃣  Processing trades...")
    try:
        process_live()
    except Exception as e:
        print(f"Error processing trades: {e}")
        return

    # Step 4: Load into ClickHouse
    print("\n5️⃣  Loading new data into ClickHouse...")
    try:
        run_incremental_load()
    except Exception as e:
        print(f"Error loading to ClickHouse: {e}")
        return

    print("\n" + "=" * 70)
    print("✅ Complete pipeline finished successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()
