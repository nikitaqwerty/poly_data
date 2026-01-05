#!/usr/bin/env python3
"""
ClickHouse Writer

Reads from Redis streams and writes to ClickHouse in batches
"""

import time
from typing import List, Dict
from datetime import datetime, timezone

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.redis_queue import RedisQueue
from common.config import redis_config, processing_config, clickhouse_config
from common.logging_config import setup_logging

logger = setup_logging("clickhouse_writer")


def get_clickhouse_client():
    """Create ClickHouse client connection"""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        username=clickhouse_config.user,
        password=clickhouse_config.password,
        database=clickhouse_config.database,
    )
    return client


def write_markets_batch(client, markets: List[Dict]) -> int:
    """
    Write markets to ClickHouse

    Args:
        client: ClickHouse client
        markets: List of market dictionaries

    Returns:
        Number of records written
    """
    if not markets:
        return 0

    try:
        logger.info("Processing %d markets for ClickHouse insertion...", len(markets))

        data = []
        for idx, market in enumerate(markets):
            # Parse datetime strings
            created_at = None
            if market.get("createdAt"):
                try:
                    created_at = datetime.fromisoformat(
                        market["createdAt"].replace("Z", "+00:00")
                    )
                except Exception as e:
                    logger.warning(
                        "Market %d: Failed to parse createdAt '%s': %s",
                        idx,
                        market.get("createdAt"),
                        e,
                    )

            closed_time = None
            if market.get("closedTime"):
                try:
                    closed_time = datetime.fromisoformat(
                        market["closedTime"].replace("Z", "+00:00")
                    )
                except Exception as e:
                    logger.warning(
                        "Market %d: Failed to parse closedTime '%s': %s",
                        idx,
                        market.get("closedTime"),
                        e,
                    )

            # Handle id - convert to integer
            id_value = market.get("id", "0")
            try:
                market_id = (
                    int(id_value)
                    if str(id_value).strip() and str(id_value).strip().isdigit()
                    else 0
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Market %d: Failed to parse id '%s': %s", idx, id_value, e
                )
                market_id = 0

            # Handle volume - convert to float, handling empty strings
            volume_value = market.get("volume", 0)
            if volume_value == "" or volume_value is None:
                volume = 0.0
            else:
                try:
                    volume = float(volume_value)
                except (ValueError, TypeError) as e:
                    logger.warning(
                        "Market %d: Failed to parse volume '%s': %s",
                        idx,
                        volume_value,
                        e,
                    )
                    volume = 0.0

            row = (
                created_at,
                market_id,
                str(market.get("question", "")),
                str(market.get("answer1", "")),
                str(market.get("answer2", "")),
                bool(market.get("neg_risk", False)),
                str(market.get("market_slug", "")),
                str(market.get("token1", "")),
                str(market.get("token2", "")),
                str(market.get("condition_id", "")),
                volume,
                str(market.get("ticker", "")),
                str(market.get("event_slug", "")),
                closed_time,
            )

            # Check for None in non-nullable fields (createdAt is non-nullable)
            if created_at is None:
                logger.error(
                    "Market %d has None createdAt! Full market data: %s", idx, market
                )
                raise ValueError(f"Market {idx} has None in createdAt field")

            data.append(row)

        logger.info("Prepared %d market rows for insertion", len(data))

        client.insert(
            f"{clickhouse_config.database}.markets",
            data,
            column_names=[
                "createdAt",
                "id",
                "question",
                "answer1",
                "answer2",
                "neg_risk",
                "market_slug",
                "token1",
                "token2",
                "condition_id",
                "volume",
                "ticker",
                "event_slug",
                "closedTime",
            ],
        )

        logger.info("Successfully inserted %d markets to ClickHouse", len(data))
        return len(data)

    except Exception as e:
        logger.error("Error writing markets to ClickHouse: %s", e)
        if "data" in locals() and data:
            logger.error(
                "Failed batch had %d rows. First row types: %s",
                len(data),
                [type(x).__name__ for x in data[0]],
            )
            logger.error("First row values: %s", data[0])
        raise


def write_events_batch(client, events: List[Dict]) -> int:
    """
    Write polymarket events to ClickHouse

    Args:
        client: ClickHouse client
        events: List of event dictionaries

    Returns:
        Number of records written
    """
    if not events:
        return 0

    try:
        logger.info("Processing %d events for ClickHouse insertion...", len(events))

        data = []
        for idx, event in enumerate(events):
            # Parse tags from semicolon-separated string
            tags_str = event.get("tags", "")
            tags_list = tags_str.split(";") if tags_str else []

            # Parse datetime strings
            created_at = None
            if event.get("createdAt"):
                try:
                    created_at = datetime.fromisoformat(
                        event["createdAt"].replace("Z", "+00:00")
                    )
                except Exception as e:
                    logger.warning(
                        "Event %d: Failed to parse createdAt '%s': %s",
                        idx,
                        event.get("createdAt"),
                        e,
                    )

            start_date = None
            if event.get("startDate"):
                try:
                    start_date = datetime.fromisoformat(
                        event["startDate"].replace("Z", "+00:00")
                    )
                except Exception as e:
                    logger.warning(
                        "Event %d: Failed to parse startDate '%s': %s",
                        idx,
                        event.get("startDate"),
                        e,
                    )

            end_date = None
            if event.get("endDate"):
                try:
                    end_date = datetime.fromisoformat(
                        event["endDate"].replace("Z", "+00:00")
                    )
                except Exception as e:
                    logger.warning(
                        "Event %d: Failed to parse endDate '%s': %s",
                        idx,
                        event.get("endDate"),
                        e,
                    )

            # Handle numeric fields - convert to proper types, handling empty strings
            markets_count_value = event.get("markets_count", 0)
            try:
                markets_count = (
                    int(markets_count_value)
                    if markets_count_value not in ("", None)
                    else 0
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Event %d: Failed to parse markets_count '%s': %s",
                    idx,
                    markets_count_value,
                    e,
                )
                markets_count = 0

            volume_value = event.get("volume", 0)
            try:
                volume = float(volume_value) if volume_value not in ("", None) else 0.0
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Event %d: Failed to parse volume '%s': %s", idx, volume_value, e
                )
                volume = 0.0

            liquidity_value = event.get("liquidity", 0)
            try:
                liquidity = (
                    float(liquidity_value) if liquidity_value not in ("", None) else 0.0
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Event %d: Failed to parse liquidity '%s': %s",
                    idx,
                    liquidity_value,
                    e,
                )
                liquidity = 0.0

            # Use epoch as default for None datetime values to avoid insertion errors
            # ClickHouse's clickhouse-connect library has issues with None in datetime columns
            epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            if created_at is None:
                logger.debug(
                    "Event %d (id=%s) has None createdAt, using epoch default",
                    idx,
                    event.get("id"),
                )
                created_at = epoch
            if start_date is None:
                logger.debug(
                    "Event %d (id=%s) has None startDate, using epoch default",
                    idx,
                    event.get("id"),
                )
                start_date = epoch
            if end_date is None:
                logger.debug(
                    "Event %d (id=%s) has None endDate, using epoch default",
                    idx,
                    event.get("id"),
                )
                end_date = epoch

            row = (
                str(event.get("id", "")),
                str(event.get("slug", "")),
                str(event.get("ticker", "")),
                str(event.get("title", "")),
                str(event.get("description", "")),
                created_at,
                start_date,
                end_date,
                tags_list,
                markets_count,
                bool(event.get("active", False)),
                bool(event.get("closed", False)),
                bool(event.get("archived", False)),
                volume,
                liquidity,
            )

            # Validate row data before adding to batch (log first 3 events for debugging)
            if idx < 3:
                logger.debug(
                    "Event %d row data: id=%s, slug=%s, ticker=%s, "
                    "createdAt=%s, startDate=%s, endDate=%s, "
                    "tags=%s, markets_count=%s, active=%s, closed=%s, archived=%s, "
                    "volume=%s, liquidity=%s",
                    idx,
                    row[0],
                    row[1],
                    row[2],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                    row[13],
                    row[14],
                )

            # Check for None values in non-nullable fields
            if None in [
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[9],
                row[10],
                row[11],
                row[12],
                row[13],
                row[14],
            ]:
                logger.error(
                    "Event %d has None in non-nullable field! Full event data: %s",
                    idx,
                    event,
                )
                logger.error("Event %d row values: %s", idx, row)
                raise ValueError(f"Event {idx} contains None in non-nullable field")

            data.append(row)

        # Log sample of data being inserted
        logger.info(
            "Prepared %d rows for insertion. Sample (first row): id=%s, slug=%s, ticker=%s",
            len(data),
            data[0][0] if data else "N/A",
            data[0][1] if data else "N/A",
            data[0][2] if data else "N/A",
        )

        client.insert(
            f"{clickhouse_config.database}.polymarket_events",
            data,
            column_names=[
                "id",
                "slug",
                "ticker",
                "title",
                "description",
                "createdAt",
                "startDate",
                "endDate",
                "tags",
                "markets_count",
                "active",
                "closed",
                "archived",
                "volume",
                "liquidity",
            ],
        )

        logger.info("Successfully inserted %d events to ClickHouse", len(data))
        return len(data)

    except Exception as e:
        logger.error("Error writing events to ClickHouse: %s", e)
        # Log more details about the data being inserted
        if "data" in locals() and data:
            logger.error(
                "Failed batch had %d rows. First row types: %s",
                len(data),
                [type(x).__name__ for x in data[0]],
            )
            logger.error("First row values: %s", data[0])
            if len(data) > 1:
                logger.error("Last row values: %s", data[-1])
        raise


def write_trades_batch(client, trades: List[Dict]) -> int:
    """
    Write trades to ClickHouse

    Args:
        client: ClickHouse client
        trades: List of trade dictionaries

    Returns:
        Number of records written
    """
    if not trades:
        return 0

    try:
        logger.info("Processing %d trades for ClickHouse insertion...", len(trades))

        data = []
        for idx, trade in enumerate(trades):
            # Parse timestamp
            timestamp = None
            if trade.get("timestamp"):
                try:
                    timestamp = datetime.fromisoformat(
                        trade["timestamp"].replace("Z", "+00:00")
                    )
                except Exception as e:
                    logger.warning(
                        "Trade %d: Failed to parse timestamp '%s': %s",
                        idx,
                        trade.get("timestamp"),
                        e,
                    )

            # Handle numeric fields - convert to proper types, handling empty strings
            price_value = trade.get("price", 0)
            try:
                price = float(price_value) if price_value not in ("", None) else 0.0
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Trade %d: Failed to parse price '%s': %s", idx, price_value, e
                )
                price = 0.0

            usd_amount_value = trade.get("usd_amount", 0)
            try:
                usd_amount = (
                    float(usd_amount_value)
                    if usd_amount_value not in ("", None)
                    else 0.0
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Trade %d: Failed to parse usd_amount '%s': %s",
                    idx,
                    usd_amount_value,
                    e,
                )
                usd_amount = 0.0

            token_amount_value = trade.get("token_amount", 0)
            try:
                token_amount = (
                    float(token_amount_value)
                    if token_amount_value not in ("", None)
                    else 0.0
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Trade %d: Failed to parse token_amount '%s': %s",
                    idx,
                    token_amount_value,
                    e,
                )
                token_amount = 0.0

            row = (
                timestamp,
                str(trade.get("market_id", "")),
                str(trade.get("maker", "")),
                str(trade.get("taker", "")),
                str(trade.get("nonusdc_side", "")),
                str(trade.get("maker_direction", "")),
                str(trade.get("taker_direction", "")),
                price,
                usd_amount,
                token_amount,
                str(trade.get("transactionHash", "")),
            )

            # Check for None in timestamp (non-nullable field)
            if timestamp is None:
                logger.error(
                    "Trade %d has None timestamp! Full trade data: %s", idx, trade
                )
                raise ValueError(f"Trade {idx} has None in timestamp field")

            data.append(row)

        logger.info("Prepared %d trade rows for insertion", len(data))

        client.insert(
            f"{clickhouse_config.database}.trades",
            data,
            column_names=[
                "timestamp",
                "market_id",
                "maker",
                "taker",
                "nonusdc_side",
                "maker_direction",
                "taker_direction",
                "price",
                "usd_amount",
                "token_amount",
                "transactionHash",
            ],
        )

        logger.info("Successfully inserted %d trades to ClickHouse", len(data))
        return len(data)

    except Exception as e:
        logger.error("Error writing trades to ClickHouse: %s", e)
        if "data" in locals() and data:
            logger.error(
                "Failed batch had %d rows. First row types: %s",
                len(data),
                [type(x).__name__ for x in data[0]],
            )
            logger.error("First row values: %s", data[0])
        raise


def run_writer():
    """Main writer loop"""
    logger.info("=" * 60)
    logger.info("🚀 Starting ClickHouse Writer")
    logger.info("=" * 60)

    queue = RedisQueue()
    client = get_clickhouse_client()

    # Test connection
    try:
        version = client.command("SELECT version()")
        logger.info("✓ Connected to ClickHouse version: %s", version)
    except Exception as e:
        logger.error("✗ Failed to connect to ClickHouse: %s", e)
        return

    consumer_name = f"writer_{int(time.time())}"

    # Check stream status but DO NOT trim automatically on startup
    logger.info("Checking stream lengths...")

    markets_length = queue.get_stream_length(redis_config.MARKETS_STREAM)
    events_length = queue.get_stream_length(redis_config.POLYMARKET_EVENTS_STREAM)
    order_events_length = queue.get_stream_length(redis_config.EVENTS_STREAM)
    trades_length = queue.get_stream_length(redis_config.TRADES_STREAM)
    trades_pending = queue.get_pending_count(
        redis_config.TRADES_STREAM, redis_config.TRADES_GROUP
    )

    logger.info("Markets stream: %d messages", markets_length)
    logger.info("Polymarket Events stream: %d messages", events_length)
    logger.info("Order Events stream: %d messages", order_events_length)
    logger.info(
        "Trades stream: %d messages (%d pending/unwritten)",
        trades_length,
        trades_pending,
    )

    if trades_pending > 100000:
        logger.warning(
            "⚠️  Large backlog of %d pending trades! Processing with batch size %d.",
            trades_pending,
            processing_config.clickhouse_writer_batch_size,
        )

        # Calculate estimated time to clear backlog
        estimated_batches = (
            trades_pending / processing_config.clickhouse_writer_batch_size
        )
        estimated_minutes = (estimated_batches * 15) / 60  # ~15 sec per batch
        logger.info(
            "Estimated time to clear backlog: %.1f minutes (%.0f batches)",
            estimated_minutes,
            estimated_batches,
        )

    markets_buffer = []
    events_buffer = []
    trades_buffer = []
    trades_message_ids = []  # Track ALL message IDs in buffer, not just current batch
    last_flush_time = time.time()
    last_trim_time = time.time()

    # Track last message IDs for simple reads to avoid skipping existing messages
    markets_last_id = "0"  # Start from beginning
    events_last_id = "0"  # Start from beginning

    try:
        while True:
            current_time = time.time()
            time_since_flush = current_time - last_flush_time

            # Adaptive blocking: if buffers are large, don't block (read fast)
            # If buffers are small, block briefly to avoid busy-waiting
            buffer_has_data = (
                len(markets_buffer) > 1000
                or len(events_buffer) > 1000
                or len(trades_buffer) > 1000
            )
            block_time = 100 if buffer_has_data else 1000  # 100ms vs 1s

            # Read from markets stream (track position to not skip existing messages)
            markets_messages = queue.read_from_stream_simple(
                redis_config.MARKETS_STREAM,
                last_id=markets_last_id,
                count=processing_config.clickhouse_writer_batch_size,
                block=block_time,
            )

            if markets_messages:
                markets_buffer.extend([data for _, data in markets_messages])
                # Update last read ID
                markets_last_id = markets_messages[-1][0]
                logger.info(
                    "Added %d markets to buffer (total: %d)",
                    len(markets_messages),
                    len(markets_buffer),
                )

            # Read from polymarket events stream (track position to not skip existing messages)
            events_messages = queue.read_from_stream_simple(
                redis_config.POLYMARKET_EVENTS_STREAM,
                last_id=events_last_id,
                count=processing_config.clickhouse_writer_batch_size,
                block=block_time,
            )

            if events_messages:
                events_buffer.extend([data for _, data in events_messages])
                # Update last read ID
                events_last_id = events_messages[-1][0]
                logger.info(
                    "Added %d events to buffer (total: %d)",
                    len(events_messages),
                    len(events_buffer),
                )

            # First, try to claim idle pending messages from dead consumers
            # This ensures we process messages that were delivered but never acknowledged
            claimed_messages = queue.claim_idle_messages(
                redis_config.TRADES_STREAM,
                redis_config.TRADES_GROUP,
                consumer_name,
                min_idle_time=60000,  # Claim messages idle for >60 seconds
                count=processing_config.clickhouse_writer_batch_size,
            )

            if claimed_messages:
                for msg_id, trade_data in claimed_messages:
                    trades_buffer.append(trade_data)
                    trades_message_ids.append(msg_id)
                logger.info(
                    "Claimed %d idle trades (total buffer: %d)",
                    len(claimed_messages),
                    len(trades_buffer),
                )

            # Then read new messages (only if we didn't get many claimed messages)
            trades_messages = []
            if len(claimed_messages) < processing_config.clickhouse_writer_batch_size:
                trades_messages = queue.read_from_stream(
                    redis_config.TRADES_STREAM,
                    redis_config.TRADES_GROUP,
                    consumer_name,
                    count=processing_config.clickhouse_writer_batch_size,
                    block=block_time,
                )

                # Append to existing message IDs (don't reset!)
                if trades_messages:
                    for msg_id, trade_data in trades_messages:
                        trades_buffer.append(trade_data)
                        trades_message_ids.append(msg_id)

                    logger.info(
                        "Added %d new trades to buffer (total: %d)",
                        len(trades_messages),
                        len(trades_buffer),
                    )

            # Flush if buffer is large enough or max wait time exceeded
            should_flush = (
                len(markets_buffer) >= processing_config.clickhouse_writer_batch_size
                or len(events_buffer) >= processing_config.clickhouse_writer_batch_size
                or len(trades_buffer) >= processing_config.clickhouse_writer_batch_size
                or time_since_flush >= processing_config.clickhouse_writer_max_wait
            )

            if should_flush and (markets_buffer or events_buffer or trades_buffer):
                logger.info("=" * 60)
                logger.info("Flushing to ClickHouse...")
                logger.info(
                    "Buffer sizes - Markets: %d, Events: %d, Trades: %d",
                    len(markets_buffer),
                    len(events_buffer),
                    len(trades_buffer),
                )

                # Write markets
                if markets_buffer:
                    logger.info("Writing markets batch...")
                    try:
                        count = write_markets_batch(client, markets_buffer)
                        logger.info("✓ Wrote %d markets to ClickHouse", count)
                        markets_buffer.clear()
                    except Exception as e:
                        logger.error("Failed to write markets batch: %s", e)
                        raise

                # Write events
                if events_buffer:
                    logger.info("Writing events batch...")
                    try:
                        count = write_events_batch(client, events_buffer)
                        logger.info("✓ Wrote %d events to ClickHouse", count)
                        events_buffer.clear()
                    except Exception as e:
                        logger.error("Failed to write events batch: %s", e)
                        raise

                # Write trades
                if trades_buffer:
                    logger.info("Writing trades batch...")
                    try:
                        count = write_trades_batch(client, trades_buffer)
                        logger.info("✓ Wrote %d trades to ClickHouse", count)
                        trades_buffer.clear()

                        # Acknowledge trades messages
                        if trades_message_ids:
                            queue.ack_messages(
                                redis_config.TRADES_STREAM,
                                redis_config.TRADES_GROUP,
                                trades_message_ids,
                            )
                            trades_message_ids.clear()
                    except Exception as e:
                        logger.error("Failed to write trades batch: %s", e)
                        raise

                logger.info("✓ Flush complete")
                logger.info("=" * 60)
                last_flush_time = time.time()

            # Periodic stream trimming (every 5 minutes)
            # Only trim streams that are safe (data already written to ClickHouse)
            time_since_trim = current_time - last_trim_time
            if time_since_trim >= 300:  # 5 minutes
                logger.info("Checking streams for safe trimming...")

                # For TRADES stream, check pending count before trimming
                trades_pending_count = queue.get_pending_count(
                    redis_config.TRADES_STREAM, redis_config.TRADES_GROUP
                )
                trades_total = queue.get_stream_length(redis_config.TRADES_STREAM)

                # Only trim if we have acknowledged messages (total - pending > 50k)
                if trades_total - trades_pending_count > 50000:
                    logger.info(
                        "TRADES: %d total, %d pending. Trimming acknowledged messages...",
                        trades_total,
                        trades_pending_count,
                    )
                    queue.trim_stream(redis_config.TRADES_STREAM, max_length=50000)
                else:
                    logger.info(
                        "TRADES: %d total, %d pending. Not trimming (would lose data)",
                        trades_total,
                        trades_pending_count,
                    )

                # For MARKETS and EVENTS, we track our position with last_id
                # These can be trimmed safely since we've read them
                markets_len = queue.get_stream_length(redis_config.MARKETS_STREAM)
                if (
                    markets_len > 100000
                ):  # Higher threshold since we're tracking position
                    logger.info("Trimming MARKETS stream (%d messages)...", markets_len)
                    queue.trim_stream(redis_config.MARKETS_STREAM, max_length=50000)

                events_len = queue.get_stream_length(
                    redis_config.POLYMARKET_EVENTS_STREAM
                )
                if events_len > 100000:
                    logger.info(
                        "Trimming POLYMARKET_EVENTS stream (%d messages)...", events_len
                    )
                    queue.trim_stream(
                        redis_config.POLYMARKET_EVENTS_STREAM, max_length=50000
                    )

                logger.info("✓ Safe trimming complete")
                last_trim_time = current_time

            # Small sleep if no data
            if (
                not markets_messages
                and not events_messages
                and not trades_messages
                and not claimed_messages
            ):
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, flushing remaining data...")

        # Flush remaining data
        if markets_buffer:
            write_markets_batch(client, markets_buffer)
            logger.info("✓ Flushed %d remaining markets", len(markets_buffer))

        if events_buffer:
            write_events_batch(client, events_buffer)
            logger.info("✓ Flushed %d remaining events", len(events_buffer))

        if trades_buffer:
            write_trades_batch(client, trades_buffer)
            logger.info("✓ Flushed %d remaining trades", len(trades_buffer))

    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)

    finally:
        queue.close()
        logger.info("ClickHouse writer stopped")


if __name__ == "__main__":
    run_writer()
