#!/usr/bin/env python3
"""
Simple test to push a sample message through the pipeline
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common.redis_queue import RedisQueue
from common.config import redis_config
from common.logging_config import setup_logging

logger = setup_logging("test_message")


def test_push_sample_market():
    """Push a sample market to the stream"""
    queue = RedisQueue()

    sample_market = {
        "createdAt": "2024-01-01T00:00:00Z",
        "id": "test_market_123",
        "question": "Will this test pass?",
        "answer1": "Yes",
        "answer2": "No",
        "neg_risk": False,
        "market_slug": "test-market",
        "token1": "token1_test",
        "token2": "token2_test",
        "condition_id": "condition_test",
        "volume": "1000.00",
        "ticker": "TEST",
        "closedTime": "",
        "event_slug": "test-event",
        "tags": "test;sample",
    }

    message_id = queue.push_to_stream(redis_config.MARKETS_STREAM, sample_market)
    logger.info("✓ Pushed test market to stream, ID: %s", message_id)

    # Check stream length
    length = queue.get_stream_length(redis_config.MARKETS_STREAM)
    logger.info("✓ Markets stream length: %d", length)

    queue.close()


def test_push_sample_event():
    """Push a sample order event to the stream"""
    queue = RedisQueue()

    sample_event = {
        "timestamp": 1704067200,  # 2024-01-01 00:00:00
        "maker": "0xmaker123",
        "makerAssetId": "1234",
        "makerAmountFilled": 1000000,
        "taker": "0xtaker456",
        "takerAssetId": "0",  # USDC
        "takerAmountFilled": 500000,
        "transactionHash": "0xhash789",
    }

    message_id = queue.push_to_stream(redis_config.EVENTS_STREAM, sample_event)
    logger.info("✓ Pushed test event to stream, ID: %s", message_id)

    # Check stream length
    length = queue.get_stream_length(redis_config.EVENTS_STREAM)
    logger.info("✓ Events stream length: %d", length)

    queue.close()


def main():
    """Run tests"""
    logger.info("=" * 60)
    logger.info("🧪 Testing Pipeline Message Flow")
    logger.info("=" * 60)

    try:
        logger.info("\n1️⃣  Testing Markets Stream...")
        test_push_sample_market()

        logger.info("\n2️⃣  Testing Events Stream...")
        test_push_sample_event()

        logger.info("\n" + "=" * 60)
        logger.info("✅ Test messages pushed successfully!")
        logger.info("=" * 60)
        logger.info("\nCheck the monitoring dashboard to see if they're processed:")
        logger.info("  http://localhost:8000")

    except Exception as e:
        logger.error("❌ Test failed: %s", e, exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
