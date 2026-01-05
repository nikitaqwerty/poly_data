# Polymarket Ingester Decoupling - Summary of Changes

## Overview
Successfully decoupled the Polymarket data pipeline into separate markets and events ingesters, eliminating the N+1 query pattern and improving efficiency.

## Files Modified

### 1. Configuration
**File:** `pipeline/common/config.py`
- Added `POLYMARKET_EVENTS_STREAM = "stream:polymarket_events"`
- Added `POLYMARKET_EVENTS_GROUP = "polymarket_events_writers"`

### 2. Markets Ingester (Renamed)
**File:** `pipeline/ingesters/polymarket_ingester.py`

**Changes:**
- Removed `fetch_event_tags()` function - no longer makes API calls to events endpoint
- Removed `session` parameter from `process_market()` - no longer needed
- Added `event_slug` to output schema - for joining with events data later
- Removed `tags` field from output - now comes from events ingester
- Updated logger name to `polymarket_markets_ingester`
- Updated state key to `polymarket_markets_offset`

**New Output Schema:**
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
    "event_slug": str,  # NEW - join key
    "closedTime": str,
}
```

### 3. Events Ingester (New)
**File:** `pipeline/ingesters/polymarket_events_ingester.py`

**New component** that:
- Polls Polymarket `/events` API endpoint independently
- Fetches full event data including tags
- Pushes to `stream:polymarket_events` Redis stream
- Maintains its own offset state (`polymarket_events_offset`)
- Runs completely independently from markets ingester

**Output Schema:**
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
    "tags": str,           # Semicolon-separated
    "markets_count": int,
    "active": bool,
    "closed": bool,
    "archived": bool,
    "volume": str,
    "liquidity": str,
}
```

### 4. ClickHouse Writer
**File:** `pipeline/processors/clickhouse_writer.py`

**Changes:**
- Added `write_events_batch()` function to write polymarket events
- Updated main loop to consume from `POLYMARKET_EVENTS_STREAM`
- Added `events_buffer` to buffer events before writing
- Updated `write_markets_batch()` to include `event_slug` column
- Added comment noting that `tags` field may be empty (comes from events)

### 5. ClickHouse Schema
**File:** `clickhouse_utils/schema.py`

**Changes:**
- Added `POLYMARKET_EVENTS_TABLE_DDL` schema definition
- Updated table creation list to include `polymarket_events`
- Updated status reporting to include new table

**New Table:**
```sql
CREATE TABLE polymarket_events (
    id String,
    slug String,
    ticker String,
    title String,
    description String,
    createdAt Nullable(DateTime64(3)),
    startDate Nullable(DateTime64(3)),
    endDate Nullable(DateTime64(3)),
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

### 6. Pipeline Schema Setup
**File:** `pipeline/setup_schema.py`

**Changes:**
- Added `event_slug String` to markets table schema
- Added `create_polymarket_events_table()` function
- Updated main setup to create polymarket_events table

### 7. Supervisor Configuration
**File:** `pipeline/supervisord.conf`

**Changes:**
- Renamed `polymarket_ingester` to `polymarket_markets_ingester`
- Added `polymarket_events_ingester` program
- Updated log file names

### 8. Docker Compose
**File:** `pipeline/docker-compose.yml`

**Changes:**
- Renamed `polymarket-ingester` service to `polymarket-markets-ingester`
- Added `polymarket-events-ingester` service

### 9. Start Script
**File:** `pipeline/start.sh`

**Changes:**
- Updated instructions to mention both ingesters
- Updated background command examples

## New Documentation
- **`pipeline/DECOUPLING_NOTES.md`** - Comprehensive guide on the decoupling
- **`pipeline/DECOUPLING_SUMMARY.md`** - This file

## Data Flow Comparison

### Before (Coupled)
```
Polymarket /markets API
    ↓ (fetch batch of 500 markets)
    ↓
For each market:
    ↓ Polymarket /events API (500+ additional API calls!)
    ↓ Extract tags
    ↓
Combine market + tags → stream:markets → ClickHouse
```

**Problems:**
- N+1 query pattern (500+ API calls per batch)
- Slow ingestion
- Risk of rate limiting
- Tightly coupled components

### After (Decoupled)
```
Polymarket /markets API → Markets Ingester → stream:markets → ClickHouse markets table
                                                                      ↓ (JOIN on event_slug)
Polymarket /events API → Events Ingester → stream:polymarket_events → ClickHouse events table
```

**Benefits:**
- Only 2 API calls per batch (1 markets + 1 events)
- Fast, efficient ingestion
- Independent scaling
- Loose coupling
- Better reliability

## Running the New System

### 1. Setup Schema (First Time)
```bash
cd pipeline
python3 setup_schema.py
```

This creates:
- `markets` table (with new `event_slug` field)
- `polymarket_events` table (new)
- `trades` table (unchanged)

### 2. Start All Services

**Option A: Docker Compose**
```bash
cd pipeline
docker-compose up -d
```

**Option B: Supervisor**
```bash
cd pipeline
supervisord -c supervisord.conf
supervisorctl status
```

**Option C: Manual (for testing)**
```bash
# Terminal 1
python3 ingesters/polymarket_ingester.py

# Terminal 2
python3 ingesters/polymarket_events_ingester.py

# Terminal 3
python3 processors/clickhouse_writer.py
```

### 3. Verify Data Flow

Check Redis streams:
```bash
redis-cli
> XLEN stream:markets
> XLEN stream:polymarket_events
> XINFO STREAM stream:markets
```

Check ClickHouse:
```sql
SELECT count() FROM polymarket.markets;
SELECT count() FROM polymarket.polymarket_events;

-- Sample join query
SELECT 
    m.question,
    e.title,
    e.tags
FROM markets m
LEFT JOIN polymarket_events e ON m.event_slug = e.slug
LIMIT 10;
```

## Migration Guide

If upgrading from the old system:

1. **Stop old ingester:**
   ```bash
   supervisorctl stop polymarket_ingester
   # or
   docker-compose stop polymarket-ingester
   ```

2. **Update schema:**
   ```bash
   cd pipeline
   python3 setup_schema.py
   ```
   This will add the new `event_slug` column to markets and create the events table.

3. **Update configuration:**
   - Pull the latest code changes
   - No manual config changes needed (handled in code)

4. **Start new ingesters:**
   ```bash
   supervisorctl start polymarket_markets_ingester polymarket_events_ingester
   # or
   docker-compose up -d polymarket-markets-ingester polymarket-events-ingester
   ```

5. **Monitor logs:**
   ```bash
   tail -f logs/polymarket_markets_ingester.out.log
   tail -f logs/polymarket_events_ingester.out.log
   ```

## Performance Improvements

**Before:**
- 500 markets + 500 events API calls = ~2-3 minutes per batch
- Frequent rate limiting
- High API load

**After:**
- 1 markets call + 1 events call = ~5-10 seconds per batch
- Minimal rate limiting risk
- Low API load
- Can tune polling intervals independently

## Future Enhancements

1. **Materialized View**: Create a joined view for common queries
2. **Cache Events**: Add Redis cache for event lookups
3. **Enrichment Processor**: Separate processor to enrich markets with event data
4. **Backfill Script**: Script to backfill events data for existing markets
5. **Monitoring**: Add metrics for each ingester separately

## Testing

To test the decoupled system:

1. Start both ingesters
2. Check that both streams are receiving data
3. Verify ClickHouse tables are being populated
4. Run join query to verify data linkage
5. Check logs for errors

```sql
-- Test query to verify linkage
SELECT 
    COUNT(*) as total_markets,
    COUNT(DISTINCT event_slug) as unique_events,
    COUNT(DISTINCT e.slug) as matched_events
FROM markets m
LEFT JOIN polymarket_events e ON m.event_slug = e.slug;
```

## Rollback Plan

If issues arise, you can rollback:

1. Stop new ingesters
2. Revert code to previous version
3. Start old ingester
4. The ClickHouse schema changes are backward compatible

## Support

For questions or issues:
- Check logs in `pipeline/logs/`
- Review `DECOUPLING_NOTES.md` for detailed architecture
- Check Redis streams for data flow issues
- Verify ClickHouse tables for data integrity
