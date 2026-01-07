"""Redis-based queue implementation using Redis Streams"""

import json
import redis
import time
from typing import Dict, List, Optional, Any
from datetime import datetime

from .config import redis_config
from .logging_config import setup_logging

logger = setup_logging("redis_queue")


class RedisQueue:
    """
    Redis Streams-based queue for pipeline data

    Uses Redis Streams for message passing between pipeline components.
    Provides state management for cursors/watermarks.
    """

    def __init__(self, max_retries: int = 30, retry_delay: float = 2.0):
        """
        Initialize Redis connection with retry logic

        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Initial delay between retries (uses exponential backoff)
        """
        self.client = redis.Redis(
            host=redis_config.host,
            port=redis_config.port,
            db=redis_config.db,
            password=redis_config.password if redis_config.password else None,
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=5,
            socket_timeout=30,  # Must be longer than block times (typically 5-10s)
        )

        # Test connection with retry logic for Redis loading scenarios
        self._wait_for_redis_ready(max_retries, retry_delay)

    def _wait_for_redis_ready(self, max_retries: int, initial_delay: float):
        """
        Wait for Redis to be ready, handling loading and connection errors

        Args:
            max_retries: Maximum number of connection attempts
            initial_delay: Initial delay between retries
        """
        delay = initial_delay
        for attempt in range(1, max_retries + 1):
            try:
                self.client.ping()
                logger.info(
                    "✓ Connected to Redis at %s:%d",
                    redis_config.host,
                    redis_config.port,
                )
                return
            except redis.BusyLoadingError:
                # Redis is loading data from disk
                if attempt == 1:
                    logger.warning(
                        "Redis is loading dataset into memory. Waiting for it to be ready..."
                    )
                logger.info(
                    "Attempt %d/%d: Redis still loading, retrying in %.1fs...",
                    attempt,
                    max_retries,
                    delay,
                )
                time.sleep(delay)
                delay = min(delay * 1.5, 30)  # Exponential backoff, max 30s
            except (redis.ConnectionError, ConnectionRefusedError) as e:
                # Connection refused or network error
                if attempt == 1:
                    logger.warning(
                        "Cannot connect to Redis at %s:%d. Retrying...",
                        redis_config.host,
                        redis_config.port,
                    )
                logger.info(
                    "Attempt %d/%d: Connection failed (%s), retrying in %.1fs...",
                    attempt,
                    max_retries,
                    str(e)[:50],
                    delay,
                )
                time.sleep(delay)
                delay = min(delay * 1.5, 30)  # Exponential backoff, max 30s
            except Exception as e:
                # Unexpected error
                logger.error("✗ Unexpected error connecting to Redis: %s", e)
                raise

        # Max retries exceeded
        logger.error(
            "✗ Failed to connect to Redis after %d attempts. "
            "Redis may be down or still loading.",
            max_retries,
        )
        raise redis.ConnectionError(
            f"Failed to connect to Redis at {redis_config.host}:{redis_config.port} "
            f"after {max_retries} attempts"
        )

    def push_to_stream(self, stream_name: str, data: Dict[str, Any]) -> str:
        """
        Push a message to a Redis stream

        Args:
            stream_name: Name of the stream
            data: Dictionary to push (will be JSON serialized)

        Returns:
            Message ID
        """
        try:
            # Serialize data to JSON
            serialized_data = {
                "data": json.dumps(data),
                "timestamp": datetime.utcnow().isoformat(),
            }

            message_id = self.client.xadd(stream_name, serialized_data)
            return message_id
        except redis.ResponseError as e:
            if "MISCONF" in str(e):
                logger.error(
                    "Redis persistence error (MISCONF). Redis cannot save to disk. "
                    "Check Redis logs and disk space: %s",
                    e,
                )
            else:
                logger.error("Error pushing to stream %s: %s", stream_name, e)
            raise
        except Exception as e:
            logger.error("Error pushing to stream %s: %s", stream_name, e)
            raise

    def push_batch_to_stream(
        self, stream_name: str, data_list: List[Dict[str, Any]]
    ) -> int:
        """
        Push multiple messages to a stream in a pipeline (batch operation)

        Args:
            stream_name: Name of the stream
            data_list: List of dictionaries to push

        Returns:
            Number of messages pushed
        """
        try:
            pipe = self.client.pipeline()

            for data in data_list:
                serialized_data = {
                    "data": json.dumps(data),
                    "timestamp": datetime.utcnow().isoformat(),
                }
                pipe.xadd(stream_name, serialized_data)

            pipe.execute()
            return len(data_list)
        except redis.ResponseError as e:
            if "MISCONF" in str(e):
                logger.error(
                    "Redis persistence error (MISCONF). Redis cannot save to disk. "
                    "Check Redis logs and disk space. Batch of %d messages failed.",
                    len(data_list),
                )
            else:
                logger.error("Error pushing batch to stream %s: %s", stream_name, e)
            raise
        except Exception as e:
            logger.error("Error pushing batch to stream %s: %s", stream_name, e)
            raise

    def read_from_stream(
        self,
        stream_name: str,
        consumer_group: str,
        consumer_name: str,
        count: int = 100,
        block: int = 5000,
    ) -> List[tuple]:
        """
        Read messages from a stream using consumer groups

        Args:
            stream_name: Name of the stream
            consumer_group: Consumer group name
            consumer_name: Consumer name
            count: Maximum number of messages to read
            block: Milliseconds to block waiting for messages (0 = no block)

        Returns:
            List of (message_id, data_dict) tuples
        """
        max_retries = 3
        retry_delay = 1.0

        for attempt in range(1, max_retries + 1):
            try:
                # Ensure consumer group exists
                self._ensure_consumer_group(stream_name, consumer_group)

                # First, try to read pending messages for this consumer (ID '0')
                # These are messages that were delivered but not acknowledged (e.g., retries)
                messages = self.client.xreadgroup(
                    groupname=consumer_group,
                    consumername=consumer_name,
                    streams={stream_name: "0"},
                    count=count,
                    block=0,  # Don't block for pending messages
                    noack=False,
                )

                # If no pending messages, read new messages (ID '>')
                if not messages or not messages[0][1]:
                    messages = self.client.xreadgroup(
                        groupname=consumer_group,
                        consumername=consumer_name,
                        streams={stream_name: ">"},
                        count=count,
                        block=block,
                        noack=False,
                    )

                result = []
                if messages:
                    for stream, message_list in messages:
                        for message_id, message_data in message_list:
                            # Deserialize JSON data
                            data = json.loads(message_data["data"])
                            result.append((message_id, data))

                return result
            except redis.BusyLoadingError:
                if attempt < max_retries:
                    logger.warning(
                        "Redis loading while reading from %s, retry %d/%d in %.1fs",
                        stream_name,
                        attempt,
                        max_retries,
                        retry_delay,
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error("Redis still loading after %d retries", max_retries)
                    raise
            except Exception as e:
                logger.error("Error reading from stream %s: %s", stream_name, e)
                raise

    def read_from_stream_simple(
        self, stream_name: str, last_id: str = "0", count: int = 100, block: int = 5000
    ) -> List[tuple]:
        """
        Simple read from stream without consumer groups (for single consumers)

        Args:
            stream_name: Name of the stream
            last_id: Last message ID read (use '0' to read from beginning, '$' for new)
            count: Maximum number of messages to read
            block: Milliseconds to block waiting for messages

        Returns:
            List of (message_id, data_dict) tuples
        """
        max_retries = 3
        retry_delay = 1.0

        for attempt in range(1, max_retries + 1):
            try:
                messages = self.client.xread(
                    streams={stream_name: last_id}, count=count, block=block
                )

                result = []
                if messages:
                    for stream, message_list in messages:
                        for message_id, message_data in message_list:
                            data = json.loads(message_data["data"])
                            result.append((message_id, data))

                return result
            except redis.BusyLoadingError:
                if attempt < max_retries:
                    logger.warning(
                        "Redis loading while reading from %s, retry %d/%d in %.1fs",
                        stream_name,
                        attempt,
                        max_retries,
                        retry_delay,
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error("Redis still loading after %d retries", max_retries)
                    raise
            except Exception as e:
                logger.error("Error reading from stream %s: %s", stream_name, e)
                raise

    def ack_message(self, stream_name: str, consumer_group: str, message_id: str):
        """
        Acknowledge a message as processed

        Args:
            stream_name: Name of the stream
            consumer_group: Consumer group name
            message_id: Message ID to acknowledge
        """
        try:
            self.client.xack(stream_name, consumer_group, message_id)
        except Exception as e:
            logger.error("Error acknowledging message: %s", e)
            raise

    def ack_messages(
        self, stream_name: str, consumer_group: str, message_ids: List[str]
    ):
        """
        Acknowledge multiple messages as processed

        Args:
            stream_name: Name of the stream
            consumer_group: Consumer group name
            message_ids: List of message IDs to acknowledge
        """
        try:
            if message_ids:
                self.client.xack(stream_name, consumer_group, *message_ids)
        except Exception as e:
            logger.error("Error acknowledging messages: %s", e)
            raise

    def _ensure_consumer_group(self, stream_name: str, group_name: str):
        """
        Ensure a consumer group exists for a stream

        Args:
            stream_name: Name of the stream
            group_name: Consumer group name
        """
        try:
            self.client.xgroup_create(stream_name, group_name, id="0", mkstream=True)
            logger.info(
                "Created consumer group '%s' for stream '%s'", group_name, stream_name
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                logger.error("Error creating consumer group: %s", e)
                raise

    def get_stream_length(self, stream_name: str) -> int:
        """Get the length of a stream"""
        try:
            return self.client.xlen(stream_name)
        except Exception as e:
            logger.error("Error getting stream length: %s", e)
            return 0

    def get_pending_count(self, stream_name: str, consumer_group: str) -> int:
        """
        Get the number of pending (unacknowledged) messages in a consumer group

        Args:
            stream_name: Name of the stream
            consumer_group: Consumer group name

        Returns:
            Number of pending messages
        """
        try:
            pending_info = self.client.xpending(stream_name, consumer_group)
            if pending_info:
                return pending_info["pending"]
            return 0
        except Exception as e:
            # Consumer group might not exist yet
            logger.debug("Error getting pending count for %s: %s", stream_name, e)
            return 0

    def claim_idle_messages(
        self,
        stream_name: str,
        consumer_group: str,
        consumer_name: str,
        min_idle_time: int = 60000,
        count: int = 100,
    ) -> List[tuple]:
        """
        Claim idle pending messages from other consumers (e.g., dead consumers)

        Args:
            stream_name: Name of the stream
            consumer_group: Consumer group name
            consumer_name: This consumer's name
            min_idle_time: Minimum idle time in milliseconds (default 60 seconds)
            count: Maximum number of messages to claim

        Returns:
            List of (message_id, data_dict) tuples
        """
        try:
            # Use XAUTOCLAIM to automatically claim idle messages
            # Returns: (next_id, messages, deleted_ids)
            result = self.client.xautoclaim(
                name=stream_name,
                groupname=consumer_group,
                consumername=consumer_name,
                min_idle_time=min_idle_time,
                start_id="0-0",
                count=count,
            )

            messages = []
            if result and len(result) >= 2:
                claimed_messages = result[1]
                for message in claimed_messages:
                    if isinstance(message, (list, tuple)) and len(message) == 2:
                        message_id, message_data = message
                        if message_data and "data" in message_data:
                            data = json.loads(message_data["data"])
                            messages.append((message_id, data))

            if messages:
                logger.info(
                    "Claimed %d idle messages from stream %s",
                    len(messages),
                    stream_name,
                )

            return messages
        except Exception as e:
            logger.error("Error claiming idle messages from %s: %s", stream_name, e)
            return []

    def trim_stream(self, stream_name: str, max_length: int = 10000):
        """
        Trim a stream to a maximum length (removes oldest messages)

        Args:
            stream_name: Name of the stream
            max_length: Maximum number of messages to keep
        """
        try:
            # Use approximate=False for exact trimming to prevent unbounded growth
            # This is more CPU-intensive but ensures messages are actually removed
            self.client.xtrim(stream_name, maxlen=max_length, approximate=False)
            logger.debug("Trimmed stream %s to max length %d", stream_name, max_length)
        except Exception as e:
            logger.error("Error trimming stream: %s", e)

    def delete_old_messages(
        self, stream_name: str, consumer_group: str, min_idle_time: int = 3600000
    ):
        """
        Delete old acknowledged messages from a stream.

        This method:
        1. Finds all pending messages that have been idle for too long
        2. Checks which messages have been acknowledged
        3. Deletes acknowledged messages to free up memory

        Note: Redis Streams keeps messages even after acknowledgment.
        This method helps clean up the stream to prevent unbounded growth.

        Args:
            stream_name: Name of the stream
            consumer_group: Consumer group name
            min_idle_time: Minimum idle time in milliseconds before considering deletion
        """
        try:
            # Get stream info to find the first and last message IDs
            try:
                stream_info = self.client.xinfo_stream(stream_name)
                if not stream_info or stream_info.get("length", 0) == 0:
                    logger.debug("Stream %s is empty, nothing to clean", stream_name)
                    return

                first_entry = stream_info.get("first-entry")
                if not first_entry:
                    return

                first_id = first_entry[0]

                # Parse the timestamp from the message ID (format: timestamp-sequence)
                first_timestamp = int(first_id.split("-")[0])
                current_timestamp = int(datetime.utcnow().timestamp() * 1000)

                # Calculate the minimum ID to keep (messages older than min_idle_time)
                min_timestamp_to_delete = current_timestamp - min_idle_time

                # Only proceed if the oldest message is old enough
                if first_timestamp >= min_timestamp_to_delete:
                    logger.debug(
                        "No messages old enough to delete in %s (oldest: %d ms ago)",
                        stream_name,
                        current_timestamp - first_timestamp,
                    )
                    return

                # Construct the minimum ID to keep (all messages before this will be deleted)
                min_id_to_keep = f"{min_timestamp_to_delete}-0"

                # Use XTRIM with MINID to remove old messages
                # This is more efficient than deleting individual messages
                deleted_count = self.client.xtrim(
                    stream_name, minid=min_id_to_keep, approximate=False
                )

                if deleted_count > 0:
                    logger.info(
                        "✓ Deleted %d old messages from stream %s (older than %d ms)",
                        deleted_count,
                        stream_name,
                        min_idle_time,
                    )
                else:
                    logger.debug("No messages deleted from stream %s", stream_name)

            except redis.ResponseError as e:
                # Consumer group might not exist yet, which is fine
                if "no such key" in str(e).lower():
                    logger.debug("Stream %s does not exist yet", stream_name)
                else:
                    logger.error("Error getting stream info for %s: %s", stream_name, e)

        except Exception as e:
            logger.error(
                "Error deleting old messages from stream %s: %s", stream_name, e
            )

    def cleanup_stream(
        self, stream_name: str, consumer_group: str, max_length: int, min_idle_time: int
    ):
        """
        Comprehensive stream cleanup: trim by length and delete old messages

        Args:
            stream_name: Name of the stream
            consumer_group: Consumer group name
            max_length: Maximum number of messages to keep
            min_idle_time: Minimum idle time in milliseconds before deletion
        """
        try:
            # First, delete old acknowledged messages
            self.delete_old_messages(stream_name, consumer_group, min_idle_time)

            # Then, trim to maximum length as a safety net
            self.trim_stream(stream_name, max_length)

        except Exception as e:
            logger.error("Error during stream cleanup for %s: %s", stream_name, e)

    # State management methods

    def get_state(self, key: str, default: Any = None) -> Any:
        """
        Get a state value

        Args:
            key: State key
            default: Default value if key doesn't exist

        Returns:
            State value or default
        """
        try:
            full_key = f"{redis_config.STATE_PREFIX}{key}"
            value = self.client.get(full_key)

            if value is None:
                return default

            # Try to parse as JSON, otherwise return as string
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            logger.error("Error getting state for key %s: %s", key, e)
            return default

    def set_state(self, key: str, value: Any):
        """
        Set a state value

        Args:
            key: State key
            value: Value to set (will be JSON serialized if not a string)
        """
        try:
            full_key = f"{redis_config.STATE_PREFIX}{key}"

            # Serialize to JSON if not already a string
            if isinstance(value, str):
                serialized = value
            else:
                serialized = json.dumps(value)

            self.client.set(full_key, serialized)
        except Exception as e:
            logger.error("Error setting state for key %s: %s", key, e)
            raise

    def delete_state(self, key: str):
        """Delete a state value"""
        try:
            full_key = f"{redis_config.STATE_PREFIX}{key}"
            self.client.delete(full_key)
        except Exception as e:
            logger.error("Error deleting state for key %s: %s", key, e)

    # Retry tracking methods

    def get_retry_count(self, stream_name: str, message_id: str) -> int:
        """
        Get the retry count for a message

        Args:
            stream_name: Name of the stream
            message_id: Message ID

        Returns:
            Current retry count (0 if never retried)
        """
        try:
            retry_key = f"retry:{stream_name}:{message_id}"
            count = self.client.get(retry_key)
            return int(count) if count else 0
        except Exception as e:
            logger.error("Error getting retry count for message %s: %s", message_id, e)
            return 0

    def increment_retry_count(
        self, stream_name: str, message_id: str, ttl_seconds: int = 86400
    ) -> int:
        """
        Increment the retry count for a message

        Args:
            stream_name: Name of the stream
            message_id: Message ID
            ttl_seconds: Time to live for the retry counter (default 24 hours)

        Returns:
            New retry count
        """
        try:
            retry_key = f"retry:{stream_name}:{message_id}"
            count = self.client.incr(retry_key)
            # Set expiry to avoid memory leaks (message IDs are unique)
            self.client.expire(retry_key, ttl_seconds)
            return count
        except Exception as e:
            logger.error(
                "Error incrementing retry count for message %s: %s", message_id, e
            )
            return 0

    def delete_retry_count(self, stream_name: str, message_id: str):
        """
        Delete the retry counter for a message

        Args:
            stream_name: Name of the stream
            message_id: Message ID
        """
        try:
            retry_key = f"retry:{stream_name}:{message_id}"
            self.client.delete(retry_key)
        except Exception as e:
            logger.error("Error deleting retry count for message %s: %s", message_id, e)

    def move_to_dlq(
        self,
        dlq_stream: str,
        original_stream: str,
        message_id: str,
        message_data: Dict[str, Any],
        failure_reason: str,
        retry_count: int,
    ):
        """
        Move a failed message to the dead letter queue (DLQ)

        Args:
            dlq_stream: Name of the DLQ stream
            original_stream: Name of the original stream
            message_id: Original message ID
            message_data: Original message data
            failure_reason: Reason for failure
            retry_count: Number of times the message was retried
        """
        try:
            # Add metadata to the message
            dlq_data = {
                "original_stream": original_stream,
                "original_message_id": message_id,
                "retry_count": retry_count,
                "failure_reason": failure_reason,
                "failed_at": datetime.utcnow().isoformat(),
                "original_data": message_data,
            }

            # Push to DLQ stream
            self.push_to_stream(dlq_stream, dlq_data)
            logger.warning(
                "⚠️  Moved message %s to DLQ after %d retries. Reason: %s",
                message_id,
                retry_count,
                failure_reason,
            )
        except Exception as e:
            logger.error(
                "❌ CRITICAL: Failed to move message %s to DLQ: %s. DATA MAY BE LOST!",
                message_id,
                e,
                exc_info=True,
            )
            raise

    def close(self):
        """Close Redis connection"""
        try:
            self.client.close()
            logger.info("Closed Redis connection")
        except Exception as e:
            logger.error("Error closing Redis connection: %s", e)
