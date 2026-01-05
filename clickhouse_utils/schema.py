"""ClickHouse table schemas and DDL operations"""

from clickhouse_utils.config import get_client, CLICKHOUSE_DATABASE


# Table definitions
MARKETS_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.markets
(
    createdAt DateTime64(3),
    id String,
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
    closedTime Nullable(DateTime64(3)),
    event_slug String
)
ENGINE = MergeTree()
ORDER BY (createdAt, id)
SETTINGS index_granularity = 8192
"""

ORDER_FILLED_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.order_filled
(
    timestamp DateTime,
    maker String,
    makerAssetId String,
    makerAmountFilled UInt64,
    taker String,
    takerAssetId String,
    takerAmountFilled UInt64,
    transactionHash String
)
ENGINE = MergeTree()
ORDER BY (timestamp, transactionHash, maker, taker)
SETTINGS index_granularity = 8192
"""

TRADES_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.trades
(
    timestamp DateTime64(6),
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
)
ENGINE = MergeTree()
ORDER BY (timestamp, market_id, transactionHash)
SETTINGS index_granularity = 8192
"""

POLYMARKET_EVENTS_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.polymarket_events
(
    id String,
    slug String,
    ticker String,
    title String,
    description String,
    createdAt Nullable(DateTime64(3)),
    startDate Nullable(DateTime64(3)),
    endDate Nullable(DateTime64(3)),
    tags Array(LowCardinality(String)),
    markets_count UInt32,
    active Boolean,
    closed Boolean,
    archived Boolean,
    volume Float64,
    liquidity Float64
)
ENGINE = MergeTree()
ORDER BY (slug, id)
SETTINGS index_granularity = 8192
"""


def create_database():
    """
    Create the ClickHouse database if it doesn't exist
    """
    from clickhouse_utils.config import get_client_without_database

    client = get_client_without_database()
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
        print(f"✓ Database '{CLICKHOUSE_DATABASE}' ready")
    except Exception as e:
        print(f"✗ Error creating database: {e}")
        raise


def create_tables():
    """
    Create all required tables in ClickHouse if they don't exist
    """
    client = get_client()

    tables = {
        "markets": MARKETS_TABLE_DDL,
        "polymarket_events": POLYMARKET_EVENTS_TABLE_DDL,
        "order_filled": ORDER_FILLED_TABLE_DDL,
        "trades": TRADES_TABLE_DDL,
    }

    print("Creating tables...")
    for table_name, ddl in tables.items():
        try:
            client.command(ddl)
            print(f"✓ Table '{table_name}' ready")
        except Exception as e:
            print(f"✗ Error creating table '{table_name}': {e}")
            raise


def get_table_count(table_name: str) -> int:
    """
    Get the number of rows in a table

    Args:
        table_name: Name of the table

    Returns:
        int: Number of rows in the table
    """
    client = get_client()
    result = client.command(f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{table_name}")
    return int(result)


def get_last_timestamp(table_name: str, timestamp_column: str = "timestamp") -> str:
    """
    Get the last (maximum) timestamp from a table

    Args:
        table_name: Name of the table
        timestamp_column: Name of the timestamp column (default: 'timestamp')

    Returns:
        str: Last timestamp as ISO string, or empty string if table is empty
    """
    client = get_client()
    result = client.query(
        f"SELECT max({timestamp_column}) as max_ts FROM {CLICKHOUSE_DATABASE}.{table_name}"
    )

    if result.result_rows and result.result_rows[0][0]:
        return str(result.result_rows[0][0])
    return ""


def get_last_trade_info() -> dict:
    """
    Get information about the last trade in the trades table
    Used for resuming incremental loads

    Returns:
        dict: Dictionary with timestamp, transactionHash, maker, taker
    """
    client = get_client()

    # Check if table has data
    count = get_table_count("trades")
    if count == 0:
        return {}

    result = client.query(
        f"""
        SELECT timestamp, transactionHash, maker, taker
        FROM {CLICKHOUSE_DATABASE}.trades
        ORDER BY timestamp DESC, transactionHash DESC
        LIMIT 1
    """
    )

    if result.result_rows:
        row = result.result_rows[0]
        return {
            "timestamp": row[0],
            "transactionHash": row[1],
            "maker": row[2],
            "taker": row[3],
        }
    return {}


def initialize_schema():
    """
    Initialize the complete ClickHouse schema (database + tables)
    """
    print("=" * 60)
    print("🗄️  Initializing ClickHouse Schema")
    print("=" * 60)

    create_database()
    create_tables()

    # Print current row counts
    print("\nCurrent table status:")
    for table in ["markets", "polymarket_events", "order_filled", "trades"]:
        try:
            count = get_table_count(table)
            print(f"  {table}: {count:,} rows")
        except:
            print(f"  {table}: table not accessible")

    print("=" * 60)
    print("✅ Schema initialization complete!")
    print("=" * 60)


if __name__ == "__main__":
    # Test the schema creation
    initialize_schema()
