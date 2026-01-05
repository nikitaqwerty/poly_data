# 🚀 Quick Start Guide

Get the Polymarket real-time pipeline running in 5 minutes!

## Prerequisites

Make sure you have these running:
- ✅ Redis
- ✅ ClickHouse
- ✅ Python 3.11+

## Installation Steps

### 1. Install Dependencies

```bash
cd pipeline
pip install -r requirements.txt
```

### 2. Setup ClickHouse Schema

```bash
python3 setup_schema.py
```

This creates the database and tables needed.

### 3. Test Setup

```bash
python3 test_setup.py
```

Should show all green checkmarks ✓

### 4. Start the Pipeline

**Option A: Docker Compose (Easiest)**
```bash
docker-compose up -d
```

**Option B: Using the start script**
```bash
./start.sh
```

**Option C: Manual (for debugging)**
```bash
# Terminal 1
python3 ingesters/polymarket_ingester.py

# Terminal 2
python3 ingesters/goldsky_ingester.py

# Terminal 3
python3 processors/trade_processor.py

# Terminal 4
python3 processors/clickhouse_writer.py

# Terminal 5
python3 monitoring/dashboard.py
```

### 5. Check the Dashboard

Open http://localhost:8000 in your browser

You should see:
- Redis streams filling up
- ClickHouse row counts increasing
- Ingester progress

## What Happens Next?

1. **Polymarket Ingester** starts fetching markets from offset 0
2. **Goldsky Ingester** starts fetching order events from timestamp 0
3. **Trade Processor** processes order events into trades
4. **ClickHouse Writer** batches and writes to ClickHouse
5. Data appears in your ClickHouse tables!

## Verify It's Working

### Check Redis
```bash
redis-cli XLEN stream:markets
redis-cli XLEN stream:order_events
redis-cli XLEN stream:trades
```

Should show increasing numbers.

### Check ClickHouse
```sql
SELECT COUNT(*) FROM polymarket.markets;
SELECT COUNT(*) FROM polymarket.trades;
```

Should show increasing row counts.

### Check Logs
```bash
# Docker Compose
docker-compose logs -f

# Supervisor
tail -f logs/*.log

# Manual
# Check each terminal
```

## Common Issues

### "Redis connection failed"
```bash
# Start Redis
redis-server

# Or with Docker
docker run -d -p 6379:6379 redis:7-alpine
```

### "ClickHouse connection failed"
```bash
# Check if running
curl http://localhost:8123/

# Or with Docker
docker run -d -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
```

### "No data appearing"
- Check logs for errors
- Verify APIs are accessible
- Check Redis streams have data
- Ensure all services are running

## Stop the Pipeline

```bash
# Docker Compose
docker-compose down

# Script
./stop.sh

# Supervisor
supervisorctl stop all

# Manual
# Ctrl+C in each terminal
```

## Next Steps

- Customize configuration in `common/config.py`
- Add more processors
- Set up monitoring/alerting
- Deploy to production server

## Configuration

Edit these environment variables or `common/config.py`:

```bash
# Redis
export REDIS_HOST=localhost
export REDIS_PORT=6379

# ClickHouse
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_DATABASE=polymarket
```

## Getting Help

Check the full README.md for detailed documentation!
