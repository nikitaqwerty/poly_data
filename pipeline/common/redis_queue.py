"""Redis-based queue implementation using Redis Streams"""

import json
import redis
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

    def __init__(self):
        """Initialize Redis connection"""
        self.client = redis.Redis(
            host=redis_config.host,
            port=redis_config.port,
            db=redis_config.db,
            password=redis_config.password if redis_config.password else None,
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

        # Test connection
        try:
            self.client.ping()
            logger.info(
                "✓ Connected to Redis at %s:%d", redis_config.host, redis_config.port
            )
        except redis.ConnectionError as e:
            logger.error("✗ Failed to connect to Redis: %s", e)
            raise

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
        try:
            # Ensure consumer group exists
            self._ensure_consumer_group(stream_name, consumer_group)

            # Read from stream
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
            self.client.xtrim(stream_name, maxlen=max_length, approximate=True)
        except Exception as e:
            logger.error("Error trimming stream: %s", e)

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

    def close(self):
        """Close Redis connection"""
        try:
            self.client.close()
            logger.info("Closed Redis connection")
        except Exception as e:
            logger.error("Error closing Redis connection: %s", e)
