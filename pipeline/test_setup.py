#!/usr/bin/env python3
"""
Test script to verify pipeline setup
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common.redis_queue import RedisQueue
from common.config import clickhouse_config
from common.logging_config import setup_logging

logger = setup_logging("test_setup")


def test_redis():
    """Test Redis connection"""
    try:
        queue = RedisQueue()
        queue.client.ping()
        queue.close()
        logger.info("✓ Redis connection: OK")
        return True
    except Exception as e:
        logger.error("✗ Redis connection: FAILED - %s", e)
        return False


def test_clickhouse():
    """Test ClickHouse connection"""
    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=clickhouse_config.host,
            port=clickhouse_config.port,
            username=clickhouse_config.user,
            password=clickhouse_config.password,
            database=clickhouse_config.database,
        )

        version = client.command("SELECT version()")
        logger.info("✓ ClickHouse connection: OK (version: %s)", version)
        return True
    except Exception as e:
        logger.error("✗ ClickHouse connection: FAILED - %s", e)
        return False


def test_tables():
    """Test if tables exist"""
    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=clickhouse_config.host,
            port=clickhouse_config.port,
            username=clickhouse_config.user,
            password=clickhouse_config.password,
            database=clickhouse_config.database,
        )

        tables = ["markets", "trades"]
        for table in tables:
            result = client.query(f"EXISTS TABLE {clickhouse_config.database}.{table}")
            exists = result.result_rows[0][0]
            if exists:
                logger.info("✓ Table '%s': EXISTS", table)
            else:
                logger.warning("✗ Table '%s': NOT FOUND", table)
                return False

        return True
    except Exception as e:
        logger.error("✗ Table check: FAILED - %s", e)
        return False


def main():
    """Run all tests"""
    logger.info("=" * 60)
    logger.info("🧪 Pipeline Setup Test")
    logger.info("=" * 60)

    results = {
        "Redis": test_redis(),
        "ClickHouse": test_clickhouse(),
        "Tables": test_tables(),
    }

    logger.info("=" * 60)
    logger.info("📋 Test Results")
    logger.info("=" * 60)

    all_passed = True
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info("%s: %s", test_name, status)
        if not passed:
            all_passed = False

    logger.info("=" * 60)

    if all_passed:
        logger.info("✅ All tests passed! Pipeline is ready.")
        return 0
    else:
        logger.error("❌ Some tests failed. Please fix the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
