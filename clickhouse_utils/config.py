"""ClickHouse connection configuration"""

import os
import clickhouse_connect

# ClickHouse connection settings
# These can be overridden via environment variables
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "polymarket")


def get_client(database: str = None):
    """
    Create and return a ClickHouse client connection

    Args:
        database: Database name to connect to. If None, connects without specifying database.

    Returns:
        clickhouse_connect.driver.Client: Connected ClickHouse client
    """
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=database or CLICKHOUSE_DATABASE,
    )
    return client


def get_client_without_database():
    """
    Create and return a ClickHouse client connection without specifying a database
    Useful for initial setup when the target database doesn't exist yet

    Returns:
        clickhouse_connect.driver.Client: Connected ClickHouse client
    """
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    return client


def test_connection():
    """
    Test ClickHouse connection and print server info

    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        # First try to connect with the target database
        client = get_client()
        result = client.command("SELECT version()")
        print(f"✓ Connected to ClickHouse version: {result}")
        return True
    except Exception as e:
        # If that fails because database doesn't exist, try connecting without database
        try:
            client = get_client_without_database()
            result = client.command("SELECT version()")
            print(
                f"✓ Connected to ClickHouse version: {result} (database '{CLICKHOUSE_DATABASE}' will be created)"
            )
            return True
        except Exception as e2:
            print(f"✗ Failed to connect to ClickHouse: {e2}")
            return False
