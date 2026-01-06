#!/usr/bin/env python3
"""
Polymarket Markets Ingester

Continuously polls Polymarket API for new markets and pushes to Redis stream.
This ingester only fetches markets data. Events data (including tags) is
fetched separately by the polymarket_events_ingester.
"""

import time
import requests
import json
from typing import List, Dict
from datetime import datetime

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.redis_queue import RedisQueue
from common.config import redis_config, api_config
from common.logging_config import setup_logging

logger = setup_logging("polymarket_markets_ingester")

STATE_KEY = "polymarket_markets_offset"


def fetch_markets_batch(offset: int, session: requests.Session) -> List[Dict]:
    """
    Fetch a batch of markets from Polymarket API

    Args:
        offset: Offset for pagination
        session: Requests session

    Returns:
        List of market dictionaries
    """
    params = {
        "order": "createdAt",
        "ascending": "true",
        "limit": api_config.polymarket_batch_size,
        "offset": offset,
    }

    try:
        response = session.get(
            api_config.polymarket_markets_url,
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

        markets = response.json()
        return markets if markets else []

    except requests.exceptions.RequestException as e:
        logger.error("Network error: %s", e)
        time.sleep(5)
        return []
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        time.sleep(3)
        return []


def process_market(market: Dict) -> Dict:
    """
    Process a market record into the desired format

    Args:
        market: Raw market data from API

    Returns:
        Processed market dictionary
    """
    # Parse outcomes
    outcomes_str = market.get("outcomes", "[]")
    if isinstance(outcomes_str, str):
        outcomes = json.loads(outcomes_str)
    else:
        outcomes = outcomes_str

    answer1 = outcomes[0] if len(outcomes) > 0 else ""
    answer2 = outcomes[1] if len(outcomes) > 1 else ""

    # Parse tokens
    clob_tokens_str = market.get("clobTokenIds", "[]")
    if isinstance(clob_tokens_str, str):
        clob_tokens = json.loads(clob_tokens_str)
    else:
        clob_tokens = clob_tokens_str

    token1 = clob_tokens[0] if len(clob_tokens) > 0 else ""
    token2 = clob_tokens[1] if len(clob_tokens) > 1 else ""

    # Negative risk
    neg_risk = market.get("negRiskAugmented", False) or market.get(
        "negRiskOther", False
    )

    # Get ticker and event_slug from events
    ticker = ""
    event_slug = ""
    if market.get("events") and len(market.get("events", [])) > 0:
        ticker = market["events"][0].get("ticker", "")
        event_slug = market["events"][0].get("slug", "")

    question_text = market.get("question", "") or market.get("title", "")

    # Convert id to string (will be converted to int by clickhouse_writer)
    market_id = market.get("id", "")

    return {
        "createdAt": market.get("createdAt", ""),
        "id": market_id,
        "question": question_text,
        "answer1": answer1,
        "answer2": answer2,
        "neg_risk": neg_risk,
        "market_slug": market.get("slug", ""),
        "token1": token1,
        "token2": token2,
        "condition_id": market.get("conditionId", ""),
        "volume": market.get("volume", ""),
        "ticker": ticker,
        "event_slug": event_slug,
        "closedTime": market.get("closedTime", ""),
    }


def run_ingester():
    """Main ingester loop"""
    logger.info("=" * 60)
    logger.info("🚀 Starting Polymarket Markets Ingester")
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
            logger.info("Fetching markets at offset %d...", offset)

            markets = fetch_markets_batch(offset, session)

            if not markets:
                logger.info(
                    "No more markets found - waiting %d seconds",
                    api_config.polymarket_poll_interval,
                )
                time.sleep(api_config.polymarket_poll_interval)
                continue

            # Process markets
            processed_markets = []
            for market in markets:
                try:
                    processed = process_market(market)
                    processed_markets.append(processed)
                except Exception as e:
                    logger.error(
                        "Error processing market %s: %s", market.get("id", "unknown"), e
                    )

            # Push to Redis stream in batch
            if processed_markets:
                count = queue.push_batch_to_stream(
                    redis_config.MARKETS_STREAM, processed_markets
                )
                logger.info("✓ Pushed %d markets to stream", count)

            # Update offset
            offset += len(markets)
            queue.set_state(STATE_KEY, offset)

            # If we got fewer than expected, we're caught up
            if len(markets) < api_config.polymarket_batch_size:
                logger.info(
                    "Caught up! Waiting %d seconds before next poll",
                    api_config.polymarket_poll_interval,
                )
                time.sleep(api_config.polymarket_poll_interval)
                # Reset to check for new markets
                offset = queue.get_state(STATE_KEY, offset)
            else:
                # Small delay between batches to avoid hammering API
                time.sleep(1)

            # Periodic stream cleanup
            current_time = time.time()
            if current_time - last_cleanup_time >= redis_config.STREAM_CLEANUP_INTERVAL:
                logger.info("Running periodic stream cleanup...")
                queue.cleanup_stream(
                    redis_config.MARKETS_STREAM,
                    redis_config.MARKETS_GROUP,
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
        logger.info("Polymarket markets ingester stopped")


if __name__ == "__main__":
    run_ingester()
