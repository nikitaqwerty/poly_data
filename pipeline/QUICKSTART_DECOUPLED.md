# Quick Start - Decoupled Polymarket Pipeline

## What Changed?

The Polymarket ingester has been split into two independent components:
- **Markets Ingester** - Fetches market data only
- **Events Ingester** - Fetches event data (tags, descriptions, etc.)

This eliminates the N+1 query problem and improves performance significantly.

## Getting Started

### 1. Setup Schema (First Time Only)

```bash
cd pipeline
python3 setup_schema.py
```

This creates:
- `markets` table (with `event_slug` field)
- `polymarket_events` table (new)
- `trades` table

### 2. Start the Pipeline

**Option A: Docker Compose (Recommended)**
```bash
docker-compose up -d
```

**Option B: Supervisor**
```bash
supervisord -c supervisord.conf
supervisorctl status
```

**Option C: Manual (for testing)**
```bash
# In separate terminals:
python3 ingesters/polymarket_ingester.py         # Markets
python3 ingesters/polymarket_events_ingester.py  # Events
python3 ingesters/goldsky_ingester.py            # Order events
python3 processors/trade_processor.py            # Trade processor
python3 processors/clickhouse_writer.py          # Writer
python3 monitoring/dashboard.py                  # Dashboard
```

### 3. Monitor

Open http://localhost:8000 to see:
- Stream lengths (markets, events, trades)
- Ingester offsets
- ClickHouse table counts

### 4. Query Data

```sql
-- Markets with event details
SELECT 
    m.question,
    m.ticker,
    e.title as event_title,
    e.tags,
    m.volume
FROM markets m
LEFT JOIN polymarket_events e ON m.event_slug = e.slug
ORDER BY m.createdAt DESC
LIMIT 10;
```

## Key Points

✅ **Two separate ingesters** now run independently
✅ **Better performance** - no more N+1 queries
✅ **Event data** stored in separate table
✅ **JOIN queries** to combine markets + events
✅ **Backward compatible** - existing markets data still works

## Troubleshooting

**Markets ingester not starting?**
- Check logs: `tail -f logs/polymarket_markets_ingester.out.log`
- Verify Redis: `redis-cli ping`

**Events ingester not starting?**
- Check logs: `tail -f logs/polymarket_events_ingester.out.log`
- Verify API access: `curl https://gamma-api.polymarket.com/events?limit=1`

**No data in tables?**
```sql
-- Check table status
SELECT count() FROM markets;
SELECT count() FROM polymarket_events;

-- Check Redis streams
redis-cli XLEN stream:markets
redis-cli XLEN stream:polymarket_events
```

## Documentation

- **DECOUPLING_NOTES.md** - Detailed architecture explanation
- **DECOUPLING_SUMMARY.md** - Complete list of changes
- **README.md** - Main pipeline documentation

## Need Help?

Check the logs directory: `pipeline/logs/`
- `polymarket_markets_ingester.{out,err}.log`
- `polymarket_events_ingester.{out,err}.log`
- `clickhouse_writer.{out,err}.log`
