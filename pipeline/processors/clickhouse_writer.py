#!/usr/bin/env python3
"""
ClickHouse Writer

Reads from Redis streams and writes to ClickHouse in batches
"""

import time
from typing import List, Dict
from datetime import datetime

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
        data = []
        for market in markets:
            # Parse datetime strings
            created_at = (
                datetime.fromisoformat(market["createdAt"].replace("Z", "+00:00"))
                if market.get("createdAt")
                else None
            )
            closed_time = (
                datetime.fromisoformat(market["closedTime"].replace("Z", "+00:00"))
                if market.get("closedTime")
                else None
            )

            # Handle id - convert to integer
            id_value = market.get("id", "0")
            market_id = (
                int(id_value)
                if str(id_value).strip() and str(id_value).strip().isdigit()
                else 0
            )

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
                float(market.get("volume", 0)),
                str(market.get("ticker", "")),
                str(market.get("event_slug", "")),  # Added for joining with events
                closed_time,
            )
            data.append(row)

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

        return len(data)

    except Exception as e:
        logger.error("Error writing markets to ClickHouse: %s", e)
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
        data = []
        for event in events:
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
                except:
                    pass

            start_date = None
            if event.get("startDate"):
                try:
                    start_date = datetime.fromisoformat(
                        event["startDate"].replace("Z", "+00:00")
                    )
                except:
                    pass

            end_date = None
            if event.get("endDate"):
                try:
                    end_date = datetime.fromisoformat(
                        event["endDate"].replace("Z", "+00:00")
                    )
                except:
                    pass

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
                int(event.get("markets_count", 0)),
                bool(event.get("active", False)),
                bool(event.get("closed", False)),
                bool(event.get("archived", False)),
                float(event.get("volume", 0)),
                float(event.get("liquidity", 0)),
            )
            data.append(row)

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

        return len(data)

    except Exception as e:
        logger.error("Error writing events to ClickHouse: %s", e)
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
        data = []
        for trade in trades:
            timestamp = datetime.fromisoformat(
                trade["timestamp"].replace("Z", "+00:00")
            )

            row = (
                timestamp,
                str(trade.get("market_id", "")),
                str(trade.get("maker", "")),
                str(trade.get("taker", "")),
                str(trade.get("nonusdc_side", "")),
                str(trade.get("maker_direction", "")),
                str(trade.get("taker_direction", "")),
                float(trade.get("price", 0)),
                float(trade.get("usd_amount", 0)),
                float(trade.get("token_amount", 0)),
                str(trade.get("transactionHash", "")),
            )
            data.append(row)

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

        return len(data)

    except Exception as e:
        logger.error("Error writing trades to ClickHouse: %s", e)
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

    markets_buffer = []
    events_buffer = []
    trades_buffer = []
    last_flush_time = time.time()

    try:
        while True:
            current_time = time.time()
            time_since_flush = current_time - last_flush_time

            # Read from markets stream
            markets_messages = queue.read_from_stream_simple(
                redis_config.MARKETS_STREAM,
                last_id="$",  # Only new messages
                count=processing_config.clickhouse_writer_batch_size,
                block=1000,
            )

            if markets_messages:
                markets_buffer.extend([data for _, data in markets_messages])
                logger.info(
                    "Added %d markets to buffer (total: %d)",
                    len(markets_messages),
                    len(markets_buffer),
                )

            # Read from polymarket events stream
            events_messages = queue.read_from_stream_simple(
                redis_config.POLYMARKET_EVENTS_STREAM,
                last_id="$",  # Only new messages
                count=processing_config.clickhouse_writer_batch_size,
                block=1000,
            )

            if events_messages:
                events_buffer.extend([data for _, data in events_messages])
                logger.info(
                    "Added %d events to buffer (total: %d)",
                    len(events_messages),
                    len(events_buffer),
                )

            # Read from trades stream
            trades_messages = queue.read_from_stream(
                redis_config.TRADES_STREAM,
                redis_config.TRADES_GROUP,
                consumer_name,
                count=processing_config.clickhouse_writer_batch_size,
                block=1000,
            )

            trades_message_ids = []
            if trades_messages:
                for msg_id, trade_data in trades_messages:
                    trades_buffer.append(trade_data)
                    trades_message_ids.append(msg_id)

                logger.info(
                    "Added %d trades to buffer (total: %d)",
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
                logger.info("Flushing to ClickHouse...")

                # Write markets
                if markets_buffer:
                    count = write_markets_batch(client, markets_buffer)
                    logger.info("✓ Wrote %d markets to ClickHouse", count)
                    markets_buffer.clear()

                # Write events
                if events_buffer:
                    count = write_events_batch(client, events_buffer)
                    logger.info("✓ Wrote %d events to ClickHouse", count)
                    events_buffer.clear()

                # Write trades
                if trades_buffer:
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

                last_flush_time = time.time()

            # Periodic stream trimming
            if int(current_time) % 300 == 0:  # Every 5 minutes
                queue.trim_stream(redis_config.TRADES_STREAM, max_length=50000)

            # Small sleep if no data
            if not markets_messages and not events_messages and not trades_messages:
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
