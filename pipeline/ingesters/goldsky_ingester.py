#!/usr/bin/env python3
"""
Goldsky Order Events Ingester

Continuously polls Goldsky GraphQL API for new order filled events
and pushes to Redis stream
"""

import time
from datetime import datetime, timezone
from typing import List, Dict

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.redis_queue import RedisQueue
from common.config import redis_config, api_config
from common.logging_config import setup_logging

logger = setup_logging("goldsky_ingester")

STATE_KEY = "goldsky_last_timestamp"


def fetch_order_events(last_timestamp: int, batch_size: int = 1000) -> List[Dict]:
    """
    Fetch order filled events from Goldsky GraphQL API

    Args:
        last_timestamp: Last timestamp fetched
        batch_size: Number of records to fetch

    Returns:
        List of order event dictionaries
    """
    query_string = (
        """query MyQuery {
        orderFilledEvents(orderBy: timestamp 
                            first: """
        + str(batch_size)
        + '''
                            where: {timestamp_gt: "'''
        + str(last_timestamp)
        + """"}) {
            fee
            id
            maker
            makerAmountFilled
            makerAssetId
            orderHash
            taker
            takerAmountFilled
            takerAssetId
            timestamp
            transactionHash
        }
    }
    """
    )

    try:
        query = gql(query_string)
        transport = RequestsHTTPTransport(
            url=api_config.goldsky_url,
            verify=True,
            retries=3,
            timeout=api_config.request_timeout,
        )
        client = Client(transport=transport)

        result = client.execute(query)
        return result.get("orderFilledEvents", [])

    except Exception as e:
        logger.error("GraphQL query error: %s", e)
        return []


def run_ingester():
    """Main ingester loop"""
    logger.info("=" * 60)
    logger.info("🚀 Starting Goldsky Order Events Ingester")
    logger.info("=" * 60)

    queue = RedisQueue()

    # Get starting timestamp from state
    last_timestamp = queue.get_state(STATE_KEY, 0)

    if last_timestamp == 0:
        logger.info("Starting from beginning of time (timestamp 0)")
    else:
        readable_time = datetime.fromtimestamp(
            int(last_timestamp), tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        logger.info("Resuming from timestamp %d (%s)", last_timestamp, readable_time)

    # Track last cleanup time
    last_cleanup_time = time.time()

    try:
        while True:
            logger.info("Fetching order events after timestamp %d...", last_timestamp)

            events = fetch_order_events(last_timestamp, api_config.goldsky_batch_size)

            if not events:
                logger.info(
                    "No new events - waiting %d seconds",
                    api_config.goldsky_poll_interval,
                )
                time.sleep(api_config.goldsky_poll_interval)
                continue

            # Sort by timestamp to ensure ordering
            events_sorted = sorted(events, key=lambda x: int(x["timestamp"]))

            # Extract the fields we need and convert to simpler format
            processed_events = []
            for event in events_sorted:
                processed = {
                    "timestamp": int(event["timestamp"]),
                    "maker": event["maker"],
                    "makerAssetId": event["makerAssetId"],
                    "makerAmountFilled": int(event["makerAmountFilled"]),
                    "taker": event["taker"],
                    "takerAssetId": event["takerAssetId"],
                    "takerAmountFilled": int(event["takerAmountFilled"]),
                    "transactionHash": event["transactionHash"],
                }
                processed_events.append(processed)

            # Push to Redis stream in batch
            count = queue.push_batch_to_stream(
                redis_config.EVENTS_STREAM, processed_events
            )

            # Update last timestamp
            last_timestamp = int(events_sorted[-1]["timestamp"])
            queue.set_state(STATE_KEY, last_timestamp)

            readable_time = datetime.fromtimestamp(
                last_timestamp, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
            logger.info(
                "✓ Pushed %d events to stream. Last timestamp: %d (%s)",
                count,
                last_timestamp,
                readable_time,
            )

            # If we got fewer than batch size, we're caught up
            if len(events) < api_config.goldsky_batch_size:
                logger.info(
                    "Caught up! Waiting %d seconds", api_config.goldsky_poll_interval
                )
                time.sleep(api_config.goldsky_poll_interval)
            else:
                # Small delay between batches
                time.sleep(1)

            # Periodic stream cleanup
            current_time = time.time()
            if current_time - last_cleanup_time >= redis_config.STREAM_CLEANUP_INTERVAL:
                logger.info("Running periodic stream cleanup...")
                queue.cleanup_stream(
                    redis_config.EVENTS_STREAM,
                    redis_config.EVENTS_GROUP,
                    redis_config.STREAM_MAX_LENGTH,
                    redis_config.STREAM_MIN_IDLE_TIME,
                )
                last_cleanup_time = current_time

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error("Fatal error: %s", e, exc_info=True)
    finally:
        queue.close()
        logger.info("Goldsky ingester stopped")


if __name__ == "__main__":
    run_ingester()
