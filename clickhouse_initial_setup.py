#!/usr/bin/env python3
"""
Initial setup script for ClickHouse

Run this once to:
1. Create the database and tables
2. Load all existing CSV data into ClickHouse

After this, use update_all_clickhouse.py for incremental updates
"""
from clickhouse_utils.config import test_connection
from clickhouse_utils.initial_load import run_initial_load


def main():
    """
    Initialize ClickHouse schema and load all existing data
    """
    print("\n" + "=" * 70)
    print("🗄️  ClickHouse Initial Setup")
    print("=" * 70)

    # Test connection
    print("\nTesting ClickHouse connection...")
    if not test_connection():
        print("\n❌ ClickHouse connection failed!")
        print("Please ensure ClickHouse is running and configured correctly.")
        print("\nConfiguration (can be set via environment variables):")
        print("  CLICKHOUSE_HOST (default: localhost)")
        print("  CLICKHOUSE_PORT (default: 8123)")
        print("  CLICKHOUSE_USER (default: default)")
        print("  CLICKHOUSE_PASSWORD (default: empty)")
        print("  CLICKHOUSE_DATABASE (default: polymarket)")
        return

    # Run initial load
    print("\nStarting initial data load...")
    try:
        run_initial_load()
    except Exception as e:
        print(f"\n❌ Error during initial load: {e}")
        import traceback

        traceback.print_exc()
        return

    print("\n" + "=" * 70)
    print("✅ Initial setup complete!")
    print("=" * 70)
    print("\nNext steps:")
    print("  • Use 'update_all_clickhouse.py' for incremental updates")
    print("  • Query your data in ClickHouse:")
    print("    SELECT count() FROM polymarket.trades;")
    print("    SELECT count() FROM polymarket.markets;")
    print("    SELECT count() FROM polymarket.order_filled;")


if __name__ == "__main__":
    main()
