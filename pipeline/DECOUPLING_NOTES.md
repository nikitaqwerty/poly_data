# Polymarket Ingester Decoupling

## Overview

The Polymarket data ingestion has been decoupled into two independent ingesters:

1. **Markets Ingester** (`polymarket_ingester.py`) - Fetches market data
2. **Events Ingester** (`polymarket_events_ingester.py`) - Fetches event data including tags

## Why Decouple?

### Previous Architecture (Coupled)
- Markets ingester fetched markets from `/markets` endpoint
- For each market, made additional API call to `/events` endpoint to fetch tags
- **Problem**: N+1 query pattern - if fetching 500 markets, made 500+ API calls
- Slow, inefficient, and prone to rate limiting

### New Architecture (Decoupled)
- **Markets ingester**: Polls `/markets` endpoint, pushes to `stream:markets`
- **Events ingester**: Polls `/events` endpoint, pushes to `stream:polymarket_events`
- Each runs independently at their own pace
- No cross-dependencies during ingestion
- More efficient API usage

## Data Flow

```
Polymarket API
    ├─ /markets → Markets Ingester → stream:markets
    └─ /events  → Events Ingester  → stream:polymarket_events
                                            ↓
                                     (Can be joined later by event_slug)
```

## Changes Made

### 1. Configuration (`common/config.py`)
- Added `POLYMARKET_EVENTS_STREAM = "stream:polymarket_events"`
- Added `POLYMARKET_EVENTS_GROUP = "polymarket_events_writers"`

### 2. Markets Ingester (`ingesters/polymarket_ingester.py`)
**Removed:**
- `fetch_event_tags()` function
- Session parameter from `process_market()`
- Tags fetching logic

**Modified:**
- Now includes `event_slug` in the output (for later joining)
- Removed `tags` field from market data
- Updated logger name to `polymarket_markets_ingester`
- Updated state key to `polymarket_markets_offset`

**Output schema:**
```python
{
    "createdAt": str,
    "id": str,
    "question": str,
    "answer1": str,
    "answer2": str,
    "neg_risk": bool,
    "market_slug": str,
    "token1": str,
    "token2": str,
    "condition_id": str,
    "volume": str,
    "ticker": str,
    "event_slug": str,  # NEW - for joining with events
    "closedTime": str,
}
```

### 3. Events Ingester (`ingesters/polymarket_events_ingester.py`)
**New file** that:
- Fetches events from `/events` endpoint with pagination
- Extracts tags, title, description, etc.
- Pushes to `stream:polymarket_events`
- Runs independently from markets ingester

**Output schema:**
```python
{
    "id": str,
    "slug": str,           # Join key with markets.event_slug
    "ticker": str,
    "title": str,
    "description": str,
    "createdAt": str,
    "startDate": str,
    "endDate": str,
    "tags": str,           # Semicolon-separated tags
    "markets_count": int,
    "active": bool,
    "closed": bool,
    "archived": bool,
    "volume": str,
    "liquidity": str,
}
```

### 4. Supervisor Configuration (`supervisord.conf`)
- Renamed `polymarket_ingester` to `polymarket_markets_ingester`
- Added `polymarket_events_ingester` program

### 5. Start Script (`start.sh`)
- Updated to mention both ingesters
- Updated log file names

## Running the Ingesters

### Using Supervisor (Recommended)
```bash
cd pipeline
supervisord -c supervisord.conf
supervisorctl status
```

Both ingesters will start automatically.

### Manually (for testing)
```bash
# Terminal 1: Markets ingester
cd pipeline
python3 ingesters/polymarket_ingester.py

# Terminal 2: Events ingester
cd pipeline
python3 ingesters/polymarket_events_ingester.py
```

## ClickHouse Schema Changes

### Markets Table
**Added field:**
- `event_slug String` - Join key to link with events

**Modified:**
- `tags` field will now be empty (populated from events join)

### New Table: polymarket_events
```sql
CREATE TABLE polymarket_events (
    id String,
    slug String,              -- Join key with markets.event_slug
    ticker String,
    title String,
    description String,
    createdAt Nullable(DateTime),
    startDate Nullable(DateTime),
    endDate Nullable(DateTime),
    tags Array(LowCardinality(String)),
    markets_count UInt32,
    active Boolean,
    closed Boolean,
    archived Boolean,
    volume Float64,
    liquidity Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (slug, id)
```

## Downstream Processing

### ClickHouse Writer
The ClickHouse writer now handles both streams:
- Reads from `stream:markets` → writes to `markets` table
- Reads from `stream:polymarket_events` → writes to `polymarket_events` table
- Both write operations happen independently in the same writer process

### Joining Markets with Events

To get complete market data with tags, use a JOIN query:

```sql
SELECT 
    m.id,
    m.question,
    m.ticker,
    m.event_slug,
    m.volume,
    e.title as event_title,
    e.description as event_description,
    e.tags,
    e.startDate,
    e.endDate
FROM markets m
LEFT JOIN polymarket_events e ON m.event_slug = e.slug
WHERE m.createdAt > '2024-01-01'
ORDER BY m.createdAt DESC
```

### Creating a Materialized View (Optional)

For better query performance, you can create a materialized view:

```sql
CREATE MATERIALIZED VIEW markets_enriched
ENGINE = MergeTree()
ORDER BY (createdAt, id)
AS SELECT 
    m.createdAt,
    m.id,
    m.question,
    m.answer1,
    m.answer2,
    m.neg_risk,
    m.market_slug,
    m.token1,
    m.token2,
    m.condition_id,
    m.volume,
    m.ticker,
    m.event_slug,
    m.closedTime,
    e.title as event_title,
    e.description as event_description,
    e.tags,
    e.startDate,
    e.endDate
FROM markets m
LEFT JOIN polymarket_events e ON m.event_slug = e.slug
```

## Benefits

1. **Performance**: No N+1 API queries
2. **Scalability**: Each ingester can be scaled independently
3. **Reliability**: If one API endpoint fails, the other continues
4. **Flexibility**: Can poll events less frequently than markets
5. **Maintainability**: Clear separation of concerns

## State Management

Each ingester maintains its own offset:
- Markets: `polymarket_markets_offset` in Redis
- Events: `polymarket_events_offset` in Redis

This allows each to track progress independently.

## Migration Notes

If you're running the old version:
1. Stop the old `polymarket_ingester`
2. The new markets ingester will use a new state key (`polymarket_markets_offset`)
3. Start both new ingesters
4. They will start from offset 0 and catch up
5. Update any downstream processors that expect `tags` in market data
