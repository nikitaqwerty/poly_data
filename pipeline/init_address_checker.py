#!/usr/bin/env python3
"""
Initialize Address Checker Infrastructure

Creates the Redis consumer group for address metadata stream
and initializes the address_metadata table in ClickHouse.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common.redis_queue import RedisQueue
from common.config import redis_config
from common.logging_config import setup_logging

logger = setup_logging("init_address_checker")


def create_consumer_group():
    """Create the consumer group for address metadata stream"""
    queue = RedisQueue()

    try:
        # Use the internal method to ensure consumer group exists
        # This is safe - it creates the group if it doesn't exist
        queue._ensure_consumer_group(
            redis_config.ADDRESS_METADATA_STREAM,
            redis_config.ADDRESS_METADATA_GROUP,
        )
        logger.info(
            "✓ Created/verified consumer group '%s' for stream '%s'",
            redis_config.ADDRESS_METADATA_GROUP,
            redis_config.ADDRESS_METADATA_STREAM,
        )
    except Exception as e:
        # Group might already exist, which is fine
        logger.info("Consumer group setup: %s", e)
    finally:
        queue.close()


def main():
    """Run initialization"""
    logger.info("=" * 60)
    logger.info("🔧 Initializing Address Checker Infrastructure")
    logger.info("=" * 60)

    try:
        # Create Redis consumer group
        create_consumer_group()

        # Note: ClickHouse table is created via setup_schema.py
        logger.info("\nNote: Make sure to run 'python3 setup_schema.py' to create the")
        logger.info("      address_metadata table in ClickHouse if not already done.")

        logger.info("=" * 60)
        logger.info("✅ Address Checker initialization complete!")
        logger.info("=" * 60)

        logger.info("\nNext steps:")
        logger.info("1. Set POLYGON_RPC_URL in your .env file")
        logger.info("   Example: POLYGON_RPC_URL=https://polygon-rpc.com")
        logger.info("   Or use a provider like Infura, Alchemy, or Goldsky")
        logger.info("2. Start the address_checker_ingester via supervisord")
        logger.info("3. The ingester will automatically process addresses from trades")

    except Exception as e:
        logger.error("❌ Initialization failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
