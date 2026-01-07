#!/usr/bin/env python3
"""
Address Checker Ingester

Pulls distinct maker and taker addresses from trades table, checks whether they
are contracts or wallets using Polygon RPC, and pushes metadata to Redis stream.
"""

import time
import requests
from typing import List, Dict, Set, Optional
from datetime import datetime

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.redis_queue import RedisQueue
from common.config import redis_config, api_config, clickhouse_config
from common.logging_config import setup_logging

logger = setup_logging("address_checker_ingester")

STATE_KEY = "address_checker_last_checked"


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


def get_unchecked_addresses(client, batch_size: int = 1000) -> List[str]:
    """
    Get distinct addresses from trades table that haven't been checked yet

    Args:
        client: ClickHouse client
        batch_size: Number of addresses to fetch

    Returns:
        List of unchecked addresses
    """
    try:
        # Get all distinct maker and taker addresses from trades
        # Exclude those already in address_metadata table
        query = f"""
        SELECT DISTINCT address
        FROM (
            SELECT maker AS address FROM {clickhouse_config.database}.trades
            UNION DISTINCT
            SELECT taker AS address FROM {clickhouse_config.database}.trades
        ) AS all_addresses
        WHERE address NOT IN (
            SELECT address FROM {clickhouse_config.database}.address_metadata
        )
        AND address != ''
        LIMIT {batch_size}
        """

        result = client.query(query)
        addresses = [row[0] for row in result.result_rows]

        logger.info("Found %d unchecked addresses", len(addresses))
        return addresses

    except Exception as e:
        logger.error("Error fetching unchecked addresses: %s", e)
        # If address_metadata table doesn't exist yet, get all addresses
        try:
            query = f"""
            SELECT DISTINCT address
            FROM (
                SELECT maker AS address FROM {clickhouse_config.database}.trades
                UNION DISTINCT
                SELECT taker AS address FROM {clickhouse_config.database}.trades
            ) AS all_addresses
            WHERE address != ''
            LIMIT {batch_size}
            """
            result = client.query(query)
            addresses = [row[0] for row in result.result_rows]
            logger.info(
                "Found %d addresses (address_metadata table may not exist yet)",
                len(addresses),
            )
            return addresses
        except Exception as e2:
            logger.error("Error in fallback query: %s", e2)
            return []


def check_address_type(address: str, rpc_url: str, session: requests.Session) -> str:
    """
    Check if an address is a contract or wallet using eth_getCode

    Args:
        address: Ethereum address to check
        rpc_url: Polygon RPC endpoint URL
        session: Requests session

    Returns:
        "contract" if address has bytecode, "wallet" if EOA
    """
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getCode",
        "params": [address, "latest"],
        "id": 1,
    }

    try:
        response = session.post(rpc_url, json=payload, timeout=30)
        response.raise_for_status()

        data = response.json()
        code = data.get("result", "0x")

        # If code is "0x", it's an EOA (wallet), otherwise it's a contract
        return "contract" if code and code != "0x" else "wallet"

    except Exception as e:
        logger.error("Error checking address type for %s: %s", address, e)
        return "unknown"


def get_transaction_count(address: str, rpc_url: str, session: requests.Session) -> int:
    """
    Get the total number of transactions sent from an address

    Args:
        address: Ethereum address to check
        rpc_url: Polygon RPC endpoint URL
        session: Requests session

    Returns:
        Transaction count (nonce)
    """
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getTransactionCount",
        "params": [address, "latest"],
        "id": 1,
    }

    try:
        response = session.post(rpc_url, json=payload, timeout=30)
        response.raise_for_status()

        data = response.json()
        count_hex = data.get("result", "0x0")

        # Convert hex to int
        return int(count_hex, 16)

    except Exception as e:
        logger.error("Error getting transaction count for %s: %s", address, e)
        return 0


def process_addresses_batch(
    addresses: List[str], rpc_url: str, session: requests.Session
) -> List[Dict]:
    """
    Process a batch of addresses and get their metadata

    Args:
        addresses: List of addresses to process
        rpc_url: Polygon RPC endpoint URL
        session: Requests session

    Returns:
        List of address metadata dictionaries
    """
    results = []

    for idx, address in enumerate(addresses):
        if idx > 0 and idx % 100 == 0:
            logger.info("Processed %d/%d addresses...", idx, len(addresses))

        address_type = check_address_type(address, rpc_url, session)
        tx_count = get_transaction_count(address, rpc_url, session)

        metadata = {
            "address": address,
            "address_type": address_type,
            "transaction_count": tx_count,
            "first_transaction_date": None,  # TODO: Can be populated via blockchain indexer
            "checked_at": datetime.utcnow().isoformat(),
        }

        results.append(metadata)

        # Rate limiting - small delay to avoid overwhelming RPC
        time.sleep(0.1)

    return results


def run_ingester():
    """Main ingester loop"""
    logger.info("=" * 60)
    logger.info("🔍 Starting Address Checker Ingester")
    logger.info("=" * 60)

    queue = RedisQueue()
    session = requests.Session()

    # Get Polygon RPC URL from config
    # You may need to add this to your .env file: POLYGON_RPC_URL
    import os

    rpc_url = os.getenv(
        "POLYGON_RPC_URL", "https://polygon-rpc.com"
    )  # Default public RPC
    logger.info("Using Polygon RPC URL: %s", rpc_url)

    # Get ClickHouse client
    try:
        ch_client = get_clickhouse_client()
        version = ch_client.command("SELECT version()")
        logger.info("✓ Connected to ClickHouse version: %s", version)
    except Exception as e:
        logger.error("✗ Failed to connect to ClickHouse: %s", e)
        return

    # Track last cleanup time
    last_cleanup_time = time.time()

    try:
        while True:
            logger.info("Fetching unchecked addresses from ClickHouse...")

            # Get a batch of unchecked addresses
            addresses = get_unchecked_addresses(
                ch_client, batch_size=api_config.address_checker_batch_size
            )

            if not addresses:
                logger.info(
                    "No unchecked addresses found - waiting %d seconds",
                    api_config.address_checker_poll_interval,
                )
                time.sleep(api_config.address_checker_poll_interval)
                continue

            logger.info("Processing %d addresses...", len(addresses))

            # Process addresses and get metadata
            metadata_list = process_addresses_batch(addresses, rpc_url, session)

            # Push to Redis stream in batch
            if metadata_list:
                count = queue.push_batch_to_stream(
                    redis_config.ADDRESS_METADATA_STREAM, metadata_list
                )
                logger.info("✓ Pushed %d address metadata records to stream", count)

            # If we got fewer than batch size, we're caught up
            if len(addresses) < api_config.address_checker_batch_size:
                logger.info(
                    "Caught up! Waiting %d seconds before next check",
                    api_config.address_checker_poll_interval,
                )
                time.sleep(api_config.address_checker_poll_interval)
            else:
                # Small delay between batches
                time.sleep(5)

            # Periodic stream cleanup
            current_time = time.time()
            if current_time - last_cleanup_time >= redis_config.STREAM_CLEANUP_INTERVAL:
                logger.info("Running periodic stream cleanup...")
                queue.cleanup_stream(
                    redis_config.ADDRESS_METADATA_STREAM,
                    redis_config.ADDRESS_METADATA_GROUP,
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
        logger.info("Address checker ingester stopped")


if __name__ == "__main__":
    run_ingester()
