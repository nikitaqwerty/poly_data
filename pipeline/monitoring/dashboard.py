#!/usr/bin/env python3
"""
Simple monitoring dashboard for the pipeline

Provides HTTP endpoints to check pipeline status
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.redis_queue import RedisQueue
from common.config import redis_config, clickhouse_config, processing_config
from common.logging_config import setup_logging

logger = setup_logging("monitoring")

app = FastAPI(title="Polymarket Pipeline Monitor")


def get_clickhouse_stats():
    """Get ClickHouse table counts"""
    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=clickhouse_config.host,
            port=clickhouse_config.port,
            username=clickhouse_config.user,
            password=clickhouse_config.password,
            database=clickhouse_config.database,
        )

        tables = ["markets", "polymarket_events", "trades"]
        stats = {}

        for table in tables:
            try:
                result = client.query(
                    f"SELECT COUNT(*) FROM {clickhouse_config.database}.{table}"
                )
                stats[table] = result.result_rows[0][0]
            except:
                stats[table] = None

        return stats
    except Exception as e:
        logger.error("Error getting ClickHouse stats: %s", e)
        return None


def get_dlq_stats(queue: RedisQueue):
    """Get DLQ and retry statistics"""
    try:
        # Get DLQ stream length
        dlq_length = queue.get_stream_length(redis_config.EVENTS_DLQ_STREAM)

        # Get pending messages that are being retried
        pending_count = queue.get_pending_count(
            redis_config.EVENTS_STREAM, redis_config.EVENTS_GROUP
        )

        # Count messages with retry counters (approximate)
        # We can't easily count all retry keys without SCAN, so we'll use pending as proxy
        retry_pattern = f"retry:{redis_config.EVENTS_STREAM}:*"

        # Use SCAN to count retry keys (sample up to 1000 keys)
        retry_count = 0
        try:
            cursor = 0
            sample_limit = 1000
            for _ in range(10):  # Limit iterations to avoid blocking
                cursor, keys = queue.client.scan(cursor, match=retry_pattern, count=100)
                retry_count += len(keys)
                if cursor == 0 or retry_count >= sample_limit:
                    break
        except Exception as e:
            logger.debug("Could not count retry keys: %s", e)
            retry_count = None

        # Read latest DLQ messages for error analysis (last 10)
        recent_dlq_errors = []
        try:
            messages = queue.client.xrevrange(
                redis_config.EVENTS_DLQ_STREAM, "+", "-", count=10
            )
            for msg_id, msg_data in messages:
                if "data" in msg_data:
                    import json

                    data = json.loads(msg_data["data"])
                    recent_dlq_errors.append(
                        {
                            "message_id": msg_id,
                            "failure_reason": data.get("failure_reason", "Unknown"),
                            "retry_count": data.get("retry_count", 0),
                            "failed_at": data.get("failed_at", "Unknown"),
                        }
                    )
        except Exception as e:
            logger.debug("Could not read DLQ messages: %s", e)

        return {
            "dlq_length": dlq_length,
            "pending_count": pending_count,
            "retry_count": retry_count,
            "recent_errors": recent_dlq_errors,
            "max_retry_attempts": processing_config.max_retry_attempts,
            "retry_base_delay": processing_config.retry_base_delay,
        }
    except Exception as e:
        logger.error("Error getting DLQ stats: %s", e)
        return {
            "dlq_length": 0,
            "pending_count": 0,
            "retry_count": None,
            "recent_errors": [],
            "max_retry_attempts": processing_config.max_retry_attempts,
            "retry_base_delay": processing_config.retry_base_delay,
        }


@app.get("/")
async def root():
    """Root endpoint with HTML dashboard"""
    queue = RedisQueue()

    # Get stream lengths
    markets_length = queue.get_stream_length(redis_config.MARKETS_STREAM)
    polymarket_events_length = queue.get_stream_length(
        redis_config.POLYMARKET_EVENTS_STREAM
    )
    order_events_length = queue.get_stream_length(redis_config.EVENTS_STREAM)
    trades_length = queue.get_stream_length(redis_config.TRADES_STREAM)

    # Get pending counts for consumer group streams
    order_events_pending = queue.get_pending_count(
        redis_config.EVENTS_STREAM, redis_config.EVENTS_GROUP
    )
    trades_pending = queue.get_pending_count(
        redis_config.TRADES_STREAM, redis_config.TRADES_GROUP
    )

    # Get state
    polymarket_markets_offset = queue.get_state("polymarket_markets_offset", 0)
    polymarket_events_offset = queue.get_state("polymarket_events_offset", 0)
    goldsky_timestamp = queue.get_state("goldsky_last_timestamp", 0)

    # Get ClickHouse stats
    ch_stats = get_clickhouse_stats()

    # Get DLQ stats
    dlq_stats = get_dlq_stats(queue)

    queue.close()

    # Build HTML
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Polymarket Pipeline Monitor</title>
        <meta http-equiv="refresh" content="5">
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background: #f5f5f5;
            }}
            h1 {{
                color: #333;
                border-bottom: 3px solid #4CAF50;
                padding-bottom: 10px;
            }}
            .card {{
                background: white;
                border-radius: 8px;
                padding: 20px;
                margin: 20px 0;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .stat {{
                display: flex;
                justify-content: space-between;
                padding: 10px 0;
                border-bottom: 1px solid #eee;
            }}
            .stat:last-child {{
                border-bottom: none;
            }}
            .label {{
                font-weight: 600;
                color: #666;
            }}
            .value {{
                color: #333;
                font-family: 'Monaco', 'Courier New', monospace;
            }}
            .section-title {{
                color: #4CAF50;
                font-size: 1.2em;
                margin-bottom: 10px;
            }}
            .status-ok {{ color: #4CAF50; }}
            .status-warn {{ color: #FF9800; }}
            .status-error {{ color: #F44336; }}
            .error-box {{
                background: #FFF3E0;
                border-left: 4px solid #FF9800;
                padding: 10px;
                margin: 10px 0;
                border-radius: 4px;
                font-size: 0.9em;
            }}
            .error-time {{
                color: #999;
                font-size: 0.85em;
            }}
            .badge {{
                display: inline-block;
                padding: 2px 8px;
                border-radius: 12px;
                font-size: 0.85em;
                font-weight: 600;
            }}
            .badge-ok {{ background: #E8F5E9; color: #4CAF50; }}
            .badge-warn {{ background: #FFF3E0; color: #FF9800; }}
            .badge-error {{ background: #FFEBEE; color: #F44336; }}
        </style>
    </head>
    <body>
        <h1>🚀 Polymarket Pipeline Monitor</h1>
        
        <div class="card">
            <div class="section-title">Redis Streams Status</div>
            <div class="stat">
                <span class="label">Markets Stream (total messages)</span>
                <span class="value">{markets_length:,}</span>
            </div>
            <div class="stat">
                <span class="label">Polymarket Events Stream (total messages)</span>
                <span class="value">{polymarket_events_length:,}</span>
            </div>
            <div class="stat">
                <span class="label">Order Events Stream (total / pending)</span>
                <span class="value">{order_events_length:,} / {order_events_pending:,}</span>
            </div>
            <div class="stat">
                <span class="label">Trades Stream (total / pending)</span>
                <span class="value">{trades_length:,} / {trades_pending:,}</span>
            </div>
        </div>
        
        <div class="card">
            <div class="section-title">Retry & Dead Letter Queue (DLQ)</div>
            <div class="stat">
                <span class="label">DLQ Messages (Failed after {dlq_stats['max_retry_attempts']} retries)</span>
    """

    dlq_len = dlq_stats["dlq_length"]
    if dlq_len == 0:
        html += f'<span class="value status-ok">{dlq_len:,} <span class="badge badge-ok">✓ OK</span></span>'
    elif dlq_len < 100:
        html += f'<span class="value status-warn">{dlq_len:,} <span class="badge badge-warn">⚠ WARNING</span></span>'
    else:
        html += f'<span class="value status-error">{dlq_len:,} <span class="badge badge-error">✗ CRITICAL</span></span>'

    html += """
            </div>
            <div class="stat">
                <span class="label">Pending Messages (Being Retried)</span>
    """

    pending = dlq_stats["pending_count"]
    html += f'<span class="value">{pending:,}</span>'

    html += """
            </div>
    """

    if dlq_stats["retry_count"] is not None:
        html += f"""
            <div class="stat">
                <span class="label">Messages with Retry Counters</span>
                <span class="value">{dlq_stats['retry_count']:,}</span>
            </div>
        """

    html += f"""
            <div class="stat">
                <span class="label">Retry Configuration</span>
                <span class="value">Max {dlq_stats['max_retry_attempts']} attempts, {dlq_stats['retry_base_delay']}s base delay</span>
            </div>
    """

    # Show recent DLQ errors if any
    if dlq_stats["recent_errors"]:
        html += """
            <div style="margin-top: 15px;">
                <div class="label">Recent DLQ Errors (Last 10):</div>
        """
        for error in dlq_stats["recent_errors"][:5]:  # Show top 5 in dashboard
            reason = error["failure_reason"][:100]  # Truncate long errors
            failed_at = (
                error["failed_at"].split("T")[1][:8]
                if "T" in error["failed_at"]
                else error["failed_at"]
            )
            html += f"""
                <div class="error-box">
                    <strong>Retry {error['retry_count']}/{dlq_stats['max_retry_attempts']}:</strong> {reason}
                    <div class="error-time">Time: {failed_at} | ID: {error['message_id'][:20]}...</div>
                </div>
            """
        if len(dlq_stats["recent_errors"]) > 5:
            html += f'<div style="text-align: center; color: #999; padding: 10px;">...and {len(dlq_stats["recent_errors"]) - 5} more errors</div>'
        html += "</div>"

    html += f"""
        </div>
        
        <div class="card">
            <div class="section-title">Ingester State</div>
            <div class="stat">
                <span class="label">Polymarket Markets Offset</span>
                <span class="value">{polymarket_markets_offset:,}</span>
            </div>
            <div class="stat">
                <span class="label">Polymarket Events Offset</span>
                <span class="value">{polymarket_events_offset:,}</span>
            </div>
            <div class="stat">
                <span class="label">Goldsky Last Timestamp</span>
                <span class="value">{goldsky_timestamp}</span>
            </div>
        </div>
        
        <div class="card">
            <div class="section-title">ClickHouse Tables</div>
    """

    if ch_stats:
        for table, count in ch_stats.items():
            if count is not None:
                html += f"""
            <div class="stat">
                <span class="label">{table}</span>
                <span class="value status-ok">{count:,} rows</span>
            </div>
                """
            else:
                html += f"""
            <div class="stat">
                <span class="label">{table}</span>
                <span class="value status-error">Error</span>
            </div>
                """
    else:
        html += """
            <div class="stat">
                <span class="label status-error">Failed to connect to ClickHouse</span>
            </div>
        """

    html += """
        </div>
        
        <div class="card">
            <div class="stat">
                <span class="label">Auto-refresh</span>
                <span class="value">Every 5 seconds</span>
            </div>
        </div>
    </body>
    </html>
    """

    return HTMLResponse(content=html)


@app.get("/api/status")
async def api_status():
    """JSON API endpoint for status"""
    queue = RedisQueue()

    status = {
        "redis_streams": {
            "markets": {
                "total": queue.get_stream_length(redis_config.MARKETS_STREAM),
            },
            "polymarket_events": {
                "total": queue.get_stream_length(redis_config.POLYMARKET_EVENTS_STREAM),
            },
            "order_events": {
                "total": queue.get_stream_length(redis_config.EVENTS_STREAM),
                "pending": queue.get_pending_count(
                    redis_config.EVENTS_STREAM, redis_config.EVENTS_GROUP
                ),
            },
            "trades": {
                "total": queue.get_stream_length(redis_config.TRADES_STREAM),
                "pending": queue.get_pending_count(
                    redis_config.TRADES_STREAM, redis_config.TRADES_GROUP
                ),
            },
        },
        "dlq": get_dlq_stats(queue),
        "state": {
            "polymarket_markets_offset": queue.get_state(
                "polymarket_markets_offset", 0
            ),
            "polymarket_events_offset": queue.get_state("polymarket_events_offset", 0),
            "goldsky_last_timestamp": queue.get_state("goldsky_last_timestamp", 0),
        },
        "clickhouse": get_clickhouse_stats(),
    }

    queue.close()
    return status


@app.get("/health")
async def health():
    """Health check endpoint"""
    try:
        queue = RedisQueue()
        queue.client.ping()
        queue.close()
        return {"status": "healthy", "redis": "ok"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting monitoring dashboard on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
