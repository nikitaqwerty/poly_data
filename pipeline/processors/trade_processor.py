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


def process_order_event(event: Dict, markets: Dict) -> tuple:
    """
    Process an order event into a trade

    Args:
        event: Order event from stream
        markets: Markets lookup cache

    Returns:
        Tuple of (trade_dict or None, error_message or None)
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
            # Can't process without market info - this is retryable
            error_msg = (
                f"Missing market info for asset_id={nonusdc_asset_id}, "
                f"tx={event.get('transactionHash', 'unknown')}"
            )
            logger.warning("⚠️  %s", error_msg)
            return None, error_msg

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

        trade = {
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
        return trade, None

    except Exception as e:
        error_msg = (
            f"Processing error: {str(e)}, tx={event.get('transactionHash', 'unknown')}"
        )
        logger.error("❌ %s", error_msg, exc_info=True)
        return None, error_msg


def retry_failed_messages(queue: RedisQueue, consumer_name: str):
    """
    Claim and retry idle pending messages with exponential backoff

    This function claims messages that have been pending for a while
    (likely due to processing failures) and makes them available for
    retry with exponential backoff based on retry count.

    Args:
        queue: RedisQueue instance
        consumer_name: Name of this consumer
    """
    try:
        # Get pending messages info
        pending_count = queue.get_pending_count(
            redis_config.EVENTS_STREAM, redis_config.EVENTS_GROUP
        )

        if pending_count == 0:
            return

        # Claim idle messages (those pending for at least base delay)
        # Start with the base delay - messages that have been idle this long
        # are candidates for retry
        min_idle_time_ms = processing_config.retry_base_delay * 1000

        idle_messages = queue.claim_idle_messages(
            stream_name=redis_config.EVENTS_STREAM,
            consumer_group=redis_config.EVENTS_GROUP,
            consumer_name=consumer_name,
            min_idle_time=min_idle_time_ms,
            count=100,  # Claim a reasonable batch
        )

        if not idle_messages:
            return

        logger.info(
            "🔄 Found %d idle messages to retry (pending: %d)",
            len(idle_messages),
            pending_count,
        )

        # Process each idle message with exponential backoff
        for message_id, event_data in idle_messages:
            retry_count = queue.get_retry_count(redis_config.EVENTS_STREAM, message_id)

            # Calculate exponential backoff: base_delay * 2^retry_count
            backoff_seconds = processing_config.retry_base_delay * (2**retry_count)

            # Get message idle time from Redis (time since last delivery)
            try:
                # Get pending info for this specific message
                pending_info = queue.client.xpending_range(
                    redis_config.EVENTS_STREAM,
                    redis_config.EVENTS_GROUP,
                    min=message_id,
                    max=message_id,
                    count=1,
                )

                if pending_info:
                    # idle_time is in milliseconds
                    idle_time_ms = pending_info[0]["time_since_delivered"]  # type: ignore
                    idle_time_seconds = idle_time_ms / 1000

                    # Only retry if enough time has passed for exponential backoff
                    if idle_time_seconds < backoff_seconds:
                        logger.debug(
                            "Message %s not ready for retry yet (idle: %.1fs, need: %ds)",
                            message_id,
                            idle_time_seconds,
                            backoff_seconds,
                        )
                        continue

                    logger.info(
                        "🔄 Retrying message %s (attempt %d, idle: %.1fs)",
                        message_id,
                        retry_count + 1,
                        idle_time_seconds,
                    )

            except Exception as e:
                logger.warning(
                    "Could not check idle time for message %s: %s. Retrying anyway.",
                    message_id,
                    e,
                )

        logger.debug("✓ Retry check complete")

    except Exception as e:
        logger.error("Error during retry check: %s", e, exc_info=True)


def run_processor():
    """Main processor loop"""
    logger.info("=" * 60)
    logger.info("🚀 Starting Trade Processor with Retry Logic")
    logger.info("=" * 60)

    queue = RedisQueue()
    consumer_name = f"processor_{int(time.time())}"

    # Log retry configuration
    logger.info("Retry Configuration:")
    logger.info("  Max retry attempts: %d", processing_config.max_retry_attempts)
    logger.info("  Base retry delay: %d seconds", processing_config.retry_base_delay)
    logger.info(
        "  Retry delays: %s",
        ", ".join(
            f"{processing_config.retry_base_delay * (2**i)}s"
            for i in range(processing_config.max_retry_attempts)
        ),
    )
    logger.info("  DLQ stream: %s", redis_config.EVENTS_DLQ_STREAM)

    # Initial markets load
    logger.info("Loading markets cache...")
    load_markets_from_clickhouse()

    # Check EVENTS_STREAM status but don't trim automatically
    events_length = queue.get_stream_length(redis_config.EVENTS_STREAM)
    events_pending = queue.get_pending_count(
        redis_config.EVENTS_STREAM, redis_config.EVENTS_GROUP
    )
    dlq_length = queue.get_stream_length(redis_config.EVENTS_DLQ_STREAM)

    logger.info(
        "EVENTS_STREAM: %d messages (%d pending)", events_length, events_pending
    )
    logger.info("DLQ_STREAM: %d messages", dlq_length)

    if events_pending > 100000:
        logger.warning(
            "⚠️  Large backlog of %d pending order events! May take time to process.",
            events_pending,
        )

    try:
        batch_count = 0
        last_trim_time = time.time()
        last_retry_check = time.time()

        while True:
            # Reload markets cache periodically
            if batch_count % 10 == 0:  # Every 10 batches
                load_markets_from_clickhouse()

            # Periodically claim and retry idle messages
            current_time = time.time()
            if (
                current_time - last_retry_check
                >= processing_config.retry_claim_interval
            ):
                retry_failed_messages(queue, consumer_name)
                last_retry_check = current_time

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
            successful_message_ids = []
            failed_messages = []  # List of (message_id, event_data, error_message)

            for message_id, event_data in messages:
                # Check current retry count
                retry_count = queue.get_retry_count(
                    redis_config.EVENTS_STREAM, message_id
                )

                trade, error_msg = process_order_event(event_data, markets_cache)
                if trade:
                    # Success - add to processed trades and mark for acknowledgment
                    processed_trades.append(trade)
                    successful_message_ids.append(message_id)
                    # Clean up retry counter if it exists
                    if retry_count > 0:
                        queue.delete_retry_count(redis_config.EVENTS_STREAM, message_id)
                else:
                    # Failure - track for retry logic
                    failed_messages.append((message_id, event_data, error_msg))

            # Handle failed messages with retry logic
            for message_id, event_data, error_msg in failed_messages:
                retry_count = queue.get_retry_count(
                    redis_config.EVENTS_STREAM, message_id
                )

                if retry_count >= processing_config.max_retry_attempts:
                    # Max retries exceeded - move to DLQ and acknowledge
                    logger.error(
                        "❌ Message %s exceeded max retries (%d), moving to DLQ",
                        message_id,
                        retry_count,
                    )
                    queue.move_to_dlq(
                        dlq_stream=redis_config.EVENTS_DLQ_STREAM,
                        original_stream=redis_config.EVENTS_STREAM,
                        message_id=message_id,
                        message_data=event_data,
                        failure_reason=error_msg,
                        retry_count=retry_count,
                    )
                    # Acknowledge the message so it's removed from main stream
                    queue.ack_message(
                        redis_config.EVENTS_STREAM,
                        redis_config.EVENTS_GROUP,
                        message_id,
                    )
                    # Clean up retry counter
                    queue.delete_retry_count(redis_config.EVENTS_STREAM, message_id)
                else:
                    # Increment retry count and DO NOT acknowledge
                    # Message will be retried later
                    new_retry_count = queue.increment_retry_count(
                        redis_config.EVENTS_STREAM, message_id
                    )
                    logger.warning(
                        "⚠️  Message %s failed (attempt %d/%d), will retry. Error: %s",
                        message_id,
                        new_retry_count,
                        processing_config.max_retry_attempts,
                        error_msg,
                    )
                    # Do NOT add to successful_message_ids - leave it pending for retry

            # Push processed trades to trades stream
            if processed_trades:
                queue.push_batch_to_stream(redis_config.TRADES_STREAM, processed_trades)
                logger.info("✓ Pushed %d trades to stream", len(processed_trades))

            # Statistics logging
            if failed_messages:
                dlq_count = sum(
                    1
                    for msg_id, _, _ in failed_messages
                    if queue.get_retry_count(redis_config.EVENTS_STREAM, msg_id)
                    >= processing_config.max_retry_attempts
                )
                retry_count = len(failed_messages) - dlq_count
                logger.info(
                    "📊 Batch summary: %d successful, %d will retry, %d moved to DLQ",
                    len(successful_message_ids),
                    retry_count,
                    dlq_count,
                )

            # Acknowledge ONLY successfully processed messages
            if successful_message_ids:
                queue.ack_messages(
                    redis_config.EVENTS_STREAM,
                    redis_config.EVENTS_GROUP,
                    successful_message_ids,
                )
                logger.debug("✓ Acknowledged %d messages", len(successful_message_ids))

            batch_count += 1

            # Periodic stream trimming to prevent unbounded growth (every 1 minute)
            # Only trim acknowledged messages to avoid data loss
            current_time = time.time()
            time_since_trim = current_time - last_trim_time
            if time_since_trim >= 60:  # 1 minute (more frequent to prevent buildup)
                events_total = queue.get_stream_length(redis_config.EVENTS_STREAM)
                events_pending_count = queue.get_pending_count(
                    redis_config.EVENTS_STREAM, redis_config.EVENTS_GROUP
                )

                # Trim to a smaller size (20k) to prevent unbounded growth
                # Even with high throughput, 20k messages is plenty of buffer
                MAX_STREAM_LENGTH = 20000

                # Only trim if we have lots of acknowledged messages
                if events_total - events_pending_count > MAX_STREAM_LENGTH:
                    logger.info(
                        "EVENTS_STREAM: %d total, %d pending. Trimming acknowledged messages...",
                        events_total,
                        events_pending_count,
                    )
                    queue.trim_stream(
                        redis_config.EVENTS_STREAM, max_length=MAX_STREAM_LENGTH
                    )
                    after_trim = queue.get_stream_length(redis_config.EVENTS_STREAM)
                    logger.info(
                        "✓ Trimmed EVENTS_STREAM from %d to %d messages",
                        events_total,
                        after_trim,
                    )
                else:
                    logger.debug(
                        "EVENTS_STREAM: %d total, %d pending. Not trimming yet (threshold: %d)",
                        events_total,
                        events_pending_count,
                        MAX_STREAM_LENGTH,
                    )

                last_trim_time = current_time

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
    finally:
        queue.close()
        logger.info("Trade processor stopped")


if __name__ == "__main__":
    run_processor()
