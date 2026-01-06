#!/usr/bin/env python3
"""
Polymarket Events Ingester

Continuously polls Polymarket API for events data (including tags) and pushes to Redis stream.
This runs independently from the markets ingester.
"""

import time
import requests
from typing import List, Dict, Optional
from datetime import datetime

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.redis_queue import RedisQueue
from common.config import redis_config, api_config
from common.logging_config import setup_logging

logger = setup_logging("polymarket_events_ingester")

STATE_KEY = "polymarket_events_offset"


def fetch_events_batch(offset: int, session: requests.Session) -> List[Dict]:
    """
    Fetch a batch of events from Polymarket API

    Args:
        offset: Offset for pagination
        session: Requests session

    Returns:
        List of event dictionaries
    """
    params = {
        "order": "createdAt",
        "ascending": "true",
        "limit": api_config.polymarket_batch_size,
        "offset": offset,
    }

    try:
        response = session.get(
            api_config.polymarket_events_url,
            params=params,
            timeout=api_config.request_timeout,
        )

        if response.status_code == 500:
            logger.warning("Server error (500) - will retry")
            time.sleep(5)
            return []
        elif response.status_code == 429:
            logger.warning("Rate limited (429) - backing off")
            time.sleep(10)
            return []
        elif response.status_code != 200:
            logger.error("API error %d: %s", response.status_code, response.text)
            time.sleep(3)
            return []

        events = response.json()
        return events if events else []

    except requests.exceptions.RequestException as e:
        logger.error("Network error: %s", e)
        time.sleep(5)
        return []
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        time.sleep(3)
        return []


def process_event(event: Dict) -> Dict:
    """
    Process an event record into the desired format

    Args:
        event: Raw event data from API

    Returns:
        Processed event dictionary
    """
    # Extract tags
    tags = event.get("tags", [])
    tags_list = [tag.get("label", "") for tag in tags if tag.get("label")]
    tags_str = ";".join(tags_list) if tags_list else ""

    # Extract markets count
    markets = event.get("markets", [])
    markets_count = len(markets) if markets else 0

    return {
        "id": event.get("id", ""),
        "slug": event.get("slug", ""),
        "ticker": event.get("ticker", ""),
        "title": event.get("title", ""),
        "description": event.get("description", ""),
        "createdAt": event.get("createdAt", ""),
        "startDate": event.get("startDate", ""),
        "endDate": event.get("endDate", ""),
        "tags": tags_str,
        "markets_count": markets_count,
        "active": event.get("active", False),
        "closed": event.get("closed", False),
        "archived": event.get("archived", False),
        "volume": event.get("volume", ""),
        "liquidity": event.get("liquidity", ""),
    }


def run_ingester():
    """Main ingester loop"""
    logger.info("=" * 60)
    logger.info("🚀 Starting Polymarket Events Ingester")
    logger.info("=" * 60)

    queue = RedisQueue()
    session = requests.Session()

    # Get starting offset from state
    offset = queue.get_state(STATE_KEY, 0)
    logger.info("Starting from offset: %d", offset)

    # Track last cleanup time
    last_cleanup_time = time.time()

    try:
        while True:
            logger.info("Fetching events at offset %d...", offset)

            events = fetch_events_batch(offset, session)

            if not events:
                logger.info(
                    "No more events found - waiting %d seconds",
                    api_config.polymarket_poll_interval,
                )
                time.sleep(api_config.polymarket_poll_interval)
                continue

            # Process events
            processed_events = []
            for event in events:
                try:
                    processed = process_event(event)
                    processed_events.append(processed)
                except Exception as e:
                    logger.error(
                        "Error processing event %s: %s", event.get("id", "unknown"), e
                    )

            # Push to Redis stream in batch
            if processed_events:
                count = queue.push_batch_to_stream(
                    redis_config.POLYMARKET_EVENTS_STREAM, processed_events
                )
                logger.info("✓ Pushed %d events to stream", count)

            # Update offset
            offset += len(events)
            queue.set_state(STATE_KEY, offset)

            # If we got fewer than expected, we're caught up
            if len(events) < api_config.polymarket_batch_size:
                logger.info(
                    "Caught up! Waiting %d seconds before next poll",
                    api_config.polymarket_poll_interval,
                )
                time.sleep(api_config.polymarket_poll_interval)
                # Reset to check for new events
                offset = queue.get_state(STATE_KEY, offset)
            else:
                # Small delay between batches to avoid hammering API
                time.sleep(1)

            # Periodic stream cleanup
            current_time = time.time()
            if current_time - last_cleanup_time >= redis_config.STREAM_CLEANUP_INTERVAL:
                logger.info("Running periodic stream cleanup...")
                queue.cleanup_stream(
                    redis_config.POLYMARKET_EVENTS_STREAM,
                    redis_config.POLYMARKET_EVENTS_GROUP,
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
        session.close()
        logger.info("Polymarket events ingester stopped")


if __name__ == "__main__":
    run_ingester()
