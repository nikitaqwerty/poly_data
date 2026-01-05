# CSV to ClickHouse Loader

This script loads CSV files into ClickHouse tables for initial data setup.

## Quick Start

### Basic Usage

```bash
# Load both markets and trades from default locations
python load_csv_to_clickhouse.py

# Load from specific file paths
python load_csv_to_clickhouse.py --markets markets.csv --trades processed/trades.csv

# Load only markets
python load_csv_to_clickhouse.py --skip-trades --markets markets.csv

# Load only trades
python load_csv_to_clickhouse.py --skip-markets --trades processed/trades.csv
```

## CSV File Formats

### Markets CSV (`markets.csv`)

Expected columns:
- `createdAt` - ISO datetime (e.g., "2024-01-01T12:00:00Z")
- `id` - Market ID string
- `question` - Market question text
- `answer1` - First answer option
- `answer2` - Second answer option
- `neg_risk` - Boolean (true/false)
- `market_slug` - Market slug
- `token1` - Token 1 address
- `token2` - Token 2 address
- `condition_id` - Condition ID
- `volume` - Trading volume (float)
- `ticker` - Market ticker
- `event_slug` - Event slug (for joining with events)
- `closedTime` - ISO datetime (nullable)
- `tags` - Semicolon-separated tags (e.g., "sports;nfl;football")

Example:
```csv
createdAt,id,question,answer1,answer2,neg_risk,market_slug,token1,token2,condition_id,volume,ticker,event_slug,closedTime,tags
2024-01-01T12:00:00Z,market123,Will it rain?,Yes,No,false,will-it-rain,0xabc...,0xdef...,cond123,10000.5,RAIN,weather-events,2024-12-31T23:59:59Z,weather;prediction
```

### Trades CSV (`processed/trades.csv`)

Expected columns:
- `timestamp` - ISO datetime (e.g., "2024-01-01T12:00:00Z")
- `market_id` - Market ID string
- `maker` - Maker address
- `taker` - Taker address
- `nonusdc_side` - Non-USDC side
- `maker_direction` - Maker direction (buy/sell)
- `taker_direction` - Taker direction (buy/sell)
- `price` - Trade price (float)
- `usd_amount` - USD amount (float)
- `token_amount` - Token amount (float)
- `transactionHash` - Transaction hash

Example:
```csv
timestamp,market_id,maker,taker,nonusdc_side,maker_direction,taker_direction,price,usd_amount,token_amount,transactionHash
2024-01-01T12:00:00Z,market123,0xabc...,0xdef...,token1,buy,sell,0.65,100.0,153.85,0x123...
```

## Command Line Options

```
--trades PATH          Path to trades CSV file (default: processed/trades.csv)
--markets PATH         Path to markets CSV file (default: markets.csv)
--batch-size N         Batch size for inserts (default: 5000)
--skip-trades          Skip loading trades
--skip-markets         Skip loading markets
```

## Features

- ✅ Automatically creates tables if they don't exist
- ✅ Batch inserts for efficiency (5000 records by default)
- ✅ Handles datetime parsing with timezone support
- ✅ Parses semicolon-separated tags into arrays
- ✅ Detailed logging with progress updates
- ✅ Error handling with row-level error reporting
- ✅ Skips invalid rows and continues processing

## Requirements

Make sure you have:
1. ClickHouse server running
2. Environment variables or config set (see `common/config.py`)
3. Required Python packages installed (see `requirements.txt`)

## Troubleshooting

### Connection Error

If you get a connection error, check:
- ClickHouse is running (`docker-compose up -d clickhouse` or equivalent)
- Config in `common/config.py` or environment variables

### CSV Not Found

Make sure CSV files exist at the specified paths. Use absolute or relative paths:
```bash
# Relative path from pipeline directory
python load_csv_to_clickhouse.py --markets ../markets.csv

# Absolute path
python load_csv_to_clickhouse.py --markets /path/to/markets.csv
```

### Invalid Date Format

Ensure dates are in ISO format with timezone:
- ✅ Good: `2024-01-01T12:00:00Z`
- ✅ Good: `2024-01-01T12:00:00+00:00`
- ❌ Bad: `2024-01-01 12:00:00`
- ❌ Bad: `01/01/2024`

### Performance

For large files (millions of rows):
- Increase batch size: `--batch-size 10000`
- Load tables separately to parallelize
- Monitor ClickHouse resources

## Examples

### Load from different directories
```bash
python load_csv_to_clickhouse.py \
  --markets ../data/markets.csv \
  --trades ../data/processed/trades.csv
```

### Large batch for better performance
```bash
python load_csv_to_clickhouse.py \
  --batch-size 10000 \
  --trades processed/trades.csv
```

### Test with markets only first
```bash
python load_csv_to_clickhouse.py \
  --skip-trades \
  --markets markets.csv
```

## Integration

This script is standalone and doesn't require the pipeline to be running. It:
- Creates tables using the same schema as `setup_schema.py`
- Uses the same ClickHouse connection config as the pipeline
- Can be run before or after starting the pipeline

After loading, you can query the data:
```python
from common.config import clickhouse_config
import clickhouse_connect

client = clickhouse_connect.get_client(
    host=clickhouse_config.host,
    port=clickhouse_config.port,
    username=clickhouse_config.user,
    password=clickhouse_config.password,
    database=clickhouse_config.database,
)

# Query markets
result = client.query("SELECT COUNT(*) FROM markets")
print(f"Markets count: {result.result_rows[0][0]}")

# Query trades
result = client.query("SELECT COUNT(*) FROM trades")
print(f"Trades count: {result.result_rows[0][0]}")
```
