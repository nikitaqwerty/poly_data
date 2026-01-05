#!/usr/bin/env python3
"""
Trade Processor

Reads order events from Redis stream, processes them into trades,
and pushes processed trades to another stream
"""

import time
from typing import Dict, List
from datetime import datetime

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.redis_queue import RedisQueue
from common.config import redis_config, processing_config, clickhouse_config
from common.logging_config import setup_logging

logger = setup_logging("trade_processor")


# In-memory cache for markets data
markets_cache = {}
markets_cache_last_update = None
CACHE_TTL = 300  # 5 minutes


def load_markets_from_clickhouse():
    """
    Load markets data from ClickHouse into memory cache

    Returns:
        Dictionary mapping token_id -> (market_id, side)
    """
    global markets_cache, markets_cache_last_update

    # Check if cache is still fresh
    now = time.time()
    if markets_cache_last_update and (now - markets_cache_last_update) < CACHE_TTL:
        return markets_cache

    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=clickhouse_config.host,
            port=clickhouse_config.port,
            username=clickhouse_config.user,
            password=clickhouse_config.password,
            database=clickhouse_config.database,
        )

        # Query markets
        result = client.query(
            f"SELECT id, token1, token2 FROM {clickhouse_config.database}.markets"
        )

        # Build cache: token_id -> (market_id, 'token1' or 'token2')
        new_cache = {}
        for row in result.result_rows:
            market_id, token1, token2 = row
            if token1 and token1 != "0":
                new_cache[token1] = (market_id, "token1")
            if token2 and token2 != "0":
                new_cache[token2] = (market_id, "token2")

        markets_cache = new_cache
        markets_cache_last_update = now

        logger.info("✓ Loaded %d tokens from markets cache", len(markets_cache))
        return markets_cache

    except Exception as e:
        logger.error("Error loading markets from ClickHouse: %s", e)
        # Return existing cache if available
        return markets_cache


def process_order_event(event: Dict, markets: Dict) -> Dict:
    """
    Process an order event into a trade

    Args:
        event: Order event from stream
        markets: Markets lookup cache

    Returns:
        Processed trade dictionary or None if can't be processed
    """
    try:
        # Identify the non-USDC asset (the one that isn't "0")
        maker_asset_id = event["makerAssetId"]
        taker_asset_id = event["takerAssetId"]

        if maker_asset_id != "0":
            nonusdc_asset_id = maker_asset_id
        else:
            nonusdc_asset_id = taker_asset_id

        # Look up market info
        if nonusdc_asset_id not in markets:
            # Can't process without market info
            return None

        market_id, side = markets[nonusdc_asset_id]

        # Determine maker and taker assets
        maker_asset = "USDC" if maker_asset_id == "0" else side
        taker_asset = "USDC" if taker_asset_id == "0" else side

        # Convert amounts from wei to regular units (divide by 10^6)
        maker_amount = event["makerAmountFilled"] / 1_000_000
        taker_amount = event["takerAmountFilled"] / 1_000_000

        # Determine direction
        taker_direction = "BUY" if taker_asset == "USDC" else "SELL"
        maker_direction = "SELL" if taker_direction == "BUY" else "BUY"

        # Determine nonusdc side
        nonusdc_side = side

        # Calculate USD amount and token amount
        if taker_asset == "USDC":
            usd_amount = taker_amount
            token_amount = maker_amount
        else:
            usd_amount = maker_amount
            token_amount = taker_amount

        # Calculate price
        if taker_asset == "USDC" and maker_amount > 0:
            price = taker_amount / maker_amount
        elif maker_asset == "USDC" and taker_amount > 0:
            price = maker_amount / taker_amount
        else:
            price = 0.0

        # Convert timestamp to datetime
        timestamp = datetime.fromtimestamp(event["timestamp"])

        return {
            "timestamp": timestamp.isoformat(),
            "market_id": market_id,
            "maker": event["maker"],
            "taker": event["taker"],
            "nonusdc_side": nonusdc_side,
            "maker_direction": maker_direction,
            "taker_direction": taker_direction,
            "price": price,
            "usd_amount": usd_amount,
            "token_amount": token_amount,
            "transactionHash": event["transactionHash"],
        }

    except Exception as e:
        logger.error("Error processing event: %s", e)
        return None


def run_processor():
    """Main processor loop"""
    logger.info("=" * 60)
    logger.info("🚀 Starting Trade Processor")
    logger.info("=" * 60)

    queue = RedisQueue()
    consumer_name = f"processor_{int(time.time())}"

    # Initial markets load
    logger.info("Loading markets cache...")
    load_markets_from_clickhouse()

    try:
        batch_count = 0

        while True:
            # Reload markets cache periodically
            if batch_count % 10 == 0:  # Every 10 batches
                load_markets_from_clickhouse()

            # Read from events stream
            messages = queue.read_from_stream(
                redis_config.EVENTS_STREAM,
                redis_config.EVENTS_GROUP,
                consumer_name,
                count=processing_config.trade_processor_batch_size,
                block=processing_config.trade_processor_interval * 1000,
            )

            if not messages:
                logger.debug("No messages, waiting...")
                continue

            logger.info("Processing %d order events...", len(messages))

            # Process events
            processed_trades = []
            message_ids = []

            for message_id, event_data in messages:
                trade = process_order_event(event_data, markets_cache)
                if trade:
                    processed_trades.append(trade)
                    message_ids.append(message_id)
                else:
                    # Still ack the message even if we couldn't process it
                    message_ids.append(message_id)

            # Push processed trades to trades stream
            if processed_trades:
                queue.push_batch_to_stream(redis_config.TRADES_STREAM, processed_trades)
                logger.info("✓ Pushed %d trades to stream", len(processed_trades))

            # Acknowledge processed messages
            queue.ack_messages(
                redis_config.EVENTS_STREAM, redis_config.EVENTS_GROUP, message_ids
            )

            batch_count += 1

            # Periodic stream trimming to prevent unbounded growth
            if batch_count % 100 == 0:
                queue.trim_stream(redis_config.EVENTS_STREAM, max_length=50000)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
    finally:
        queue.close()
        logger.info("Trade processor stopped")


if __name__ == "__main__":
    run_processor()
