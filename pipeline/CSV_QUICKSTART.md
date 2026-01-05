# CSV to ClickHouse Quick Start

Load your historical data from CSV files into ClickHouse in 3 easy steps.

## Step 1: Prepare Your CSV Files

You need two CSV files:
1. **markets.csv** - Market definitions
2. **processed/trades.csv** - Trade history

See `example_markets.csv` and `example_trades.csv` for format examples.

### Required Format

**Markets CSV:**
```csv
createdAt,id,question,answer1,answer2,neg_risk,market_slug,token1,token2,condition_id,volume,ticker,event_slug,closedTime,tags
2024-01-01T12:00:00Z,market001,Will it rain?,Yes,No,false,rain-market,...
```

**Trades CSV:**
```csv
timestamp,market_id,maker,taker,nonusdc_side,maker_direction,taker_direction,price,usd_amount,token_amount,transactionHash
2024-01-01T12:30:00Z,market001,0x111...,0x222...,token1,buy,sell,0.65,100.00,153.85,0xaaa...
```

## Step 2: Validate Your CSV Files (Optional but Recommended)

```bash
# Validate markets CSV
python validate_csv.py markets markets.csv

# Validate trades CSV
python validate_csv.py trades processed/trades.csv

# Check all rows (not just first 100)
python validate_csv.py markets markets.csv --max-rows 0
```

## Step 3: Load into ClickHouse

```bash
# Make sure ClickHouse is running
docker-compose up -d clickhouse

# Load both files
python load_csv_to_clickhouse.py

# Or specify custom paths
python load_csv_to_clickhouse.py --markets markets.csv --trades processed/trades.csv
```

## Common Use Cases

### Load only markets
```bash
python load_csv_to_clickhouse.py --skip-trades
```

### Load only trades
```bash
python load_csv_to_clickhouse.py --skip-markets
```

### Load from different location
```bash
python load_csv_to_clickhouse.py \
  --markets /path/to/markets.csv \
  --trades /path/to/trades.csv
```

### Large files - increase batch size
```bash
python load_csv_to_clickhouse.py --batch-size 10000
```

## Verify Data Loaded

After loading, verify the data:

```python
import clickhouse_connect
from common.config import clickhouse_config

client = clickhouse_connect.get_client(
    host=clickhouse_config.host,
    port=clickhouse_config.port,
    username=clickhouse_config.user,
    password=clickhouse_config.password,
    database=clickhouse_config.database,
)

# Check counts
markets_count = client.query("SELECT COUNT(*) FROM markets").result_rows[0][0]
trades_count = client.query("SELECT COUNT(*) FROM trades").result_rows[0][0]

print(f"Markets: {markets_count:,}")
print(f"Trades: {trades_count:,}")

# Sample some data
print("\nSample markets:")
print(client.query("SELECT id, question, volume FROM markets LIMIT 3").result_rows)

print("\nSample trades:")
print(client.query("SELECT timestamp, market_id, price, usd_amount FROM trades LIMIT 3").result_rows)
```

Or use the ClickHouse CLI:

```bash
docker exec -it pipeline-clickhouse-1 clickhouse-client

# In ClickHouse CLI:
USE polymarket;
SELECT COUNT(*) FROM markets;
SELECT COUNT(*) FROM trades;
SELECT * FROM markets LIMIT 3;
SELECT * FROM trades LIMIT 3;
```

## Troubleshooting

### File not found
- Check the file path is correct
- Use absolute paths if relative paths don't work
- Ensure you're running from the `pipeline` directory

### Date format errors
- Dates must be in ISO format: `2024-01-01T12:00:00Z`
- Timezone should be included (Z for UTC or +00:00)

### Connection errors
- Make sure ClickHouse is running: `docker-compose ps`
- Start ClickHouse: `docker-compose up -d clickhouse`
- Check config in `.env` or environment variables

### Large files are slow
- Increase batch size: `--batch-size 10000`
- Load tables separately (markets first, then trades)
- Monitor ClickHouse memory usage

## Files Reference

| File | Purpose |
|------|---------|
| `load_csv_to_clickhouse.py` | Main loader script |
| `validate_csv.py` | CSV format validator |
| `example_markets.csv` | Example markets CSV template |
| `example_trades.csv` | Example trades CSV template |
| `CSV_LOADER_README.md` | Detailed documentation |
| `CSV_QUICKSTART.md` | This quick start guide |

## Next Steps

After loading your data:
1. Start the full pipeline: `./start.sh`
2. View monitoring dashboard: `python monitoring/dashboard.py`
3. Query your data (see examples above)
4. Set up continuous ingestion from live sources

---

For more details, see `CSV_LOADER_README.md`
