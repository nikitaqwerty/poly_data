# ClickHouse Setup Guide

This guide will help you set up ClickHouse for the Polymarket data pipeline.

## Prerequisites

### 1. Install ClickHouse

#### macOS
```bash
brew install clickhouse
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt-get install -y apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754
echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update
sudo apt-get install -y clickhouse-server clickhouse-client
```

#### Linux (CentOS/RHEL)
```bash
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo
sudo yum install -y clickhouse-server clickhouse-client
```

#### Docker
```bash
docker run -d --name clickhouse-server \
  -p 8123:8123 -p 9000:9000 \
  --ulimit nofile=262144:262144 \
  clickhouse/clickhouse-server
```

### 2. Start ClickHouse Server

#### macOS (Homebrew)
```bash
brew services start clickhouse
```

#### Linux (systemd)
```bash
sudo systemctl start clickhouse-server
sudo systemctl enable clickhouse-server  # Start on boot
```

#### Docker
Already running if you used the docker run command above.

### 3. Verify Installation

```bash
clickhouse-client --query "SELECT version()"
```

You should see the ClickHouse version number.

## Configuration

### Environment Variables

The Python scripts use environment variables for ClickHouse connection. Create a `.env` file in the project root (optional):

```bash
# ClickHouse Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=polymarket
```

**Defaults** (if not set):
- `CLICKHOUSE_HOST`: `localhost`
- `CLICKHOUSE_PORT`: `8123`
- `CLICKHOUSE_USER`: `default`
- `CLICKHOUSE_PASSWORD`: empty string
- `CLICKHOUSE_DATABASE`: `polymarket`

### Custom Configuration

If you're running ClickHouse remotely or with custom settings:

```bash
export CLICKHOUSE_HOST=your-clickhouse-server.com
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=your_username
export CLICKHOUSE_PASSWORD=your_password
export CLICKHOUSE_DATABASE=polymarket
```

## Initial Setup

### 1. Install Python Dependencies

```bash
uv sync
```

### 2. Run Initial Setup Script

This creates the database, tables, and loads all existing CSV data:

```bash
uv run python clickhouse_initial_setup.py
```

**What it does:**
- Creates `polymarket` database
- Creates three tables: `markets`, `order_filled`, `trades`
- Loads all data from:
  - `markets.csv` + `missing_markets.csv` → `markets` table
  - `goldsky/orderFilled.csv` → `order_filled` table
  - `processed/trades.csv` → `trades` table

**Expected time:** 5-30 minutes depending on data size and hardware

### 3. Verify Data Loaded

```bash
clickhouse-client
```

```sql
USE polymarket;

-- Check table sizes
SELECT 'markets' as table, count() as rows FROM markets
UNION ALL
SELECT 'order_filled', count() FROM order_filled
UNION ALL
SELECT 'trades', count() FROM trades;

-- Sample data
SELECT * FROM trades LIMIT 5;
```

## Regular Updates

After initial setup, use the incremental update script:

```bash
uv run python update_all_clickhouse.py
```

This script:
1. Fetches new markets from Polymarket API
2. Fetches new order events from Goldsky
3. Processes new trades
4. **Incrementally loads all new data to ClickHouse**

Run this on a schedule (e.g., hourly or daily) to keep ClickHouse in sync.

## Database Schema

### markets table

Stores market metadata from Polymarket.

```sql
CREATE TABLE polymarket.markets (
    createdAt DateTime64(3),      -- Market creation timestamp
    id String,                      -- Unique market ID
    question String,                -- Market question
    answer1 String,                 -- First outcome
    answer2 String,                 -- Second outcome
    neg_risk Boolean,               -- Negative risk flag
    market_slug String,             -- URL slug
    token1 String,                  -- First outcome token ID
    token2 String,                  -- Second outcome token ID
    condition_id String,            -- Condition ID
    volume Float64,                 -- Trading volume
    ticker String,                  -- Associated ticker
    closedTime Nullable(DateTime64(3))  -- Market close time
) ENGINE = MergeTree()
ORDER BY (createdAt, id);
```

### order_filled table

Raw order-filled events from Goldsky subgraph.

```sql
CREATE TABLE polymarket.order_filled (
    timestamp DateTime,             -- Order fill timestamp
    maker String,                   -- Maker address
    makerAssetId String,           -- Maker asset ID
    makerAmountFilled UInt64,      -- Maker amount (raw units)
    taker String,                   -- Taker address
    takerAssetId String,           -- Taker asset ID
    takerAmountFilled UInt64,      -- Taker amount (raw units)
    transactionHash String          -- Transaction hash
) ENGINE = MergeTree()
ORDER BY (timestamp, transactionHash, maker, taker);
```

### trades table

Processed trade data with market mapping and calculations.

```sql
CREATE TABLE polymarket.trades (
    timestamp DateTime64(6),        -- Trade timestamp
    market_id String,               -- Market ID
    maker String,                   -- Maker address
    taker String,                   -- Taker address
    nonusdc_side String,           -- Which outcome token (token1/token2)
    maker_direction String,         -- BUY or SELL
    taker_direction String,         -- BUY or SELL
    price Float64,                  -- Price (USDC per outcome token)
    usd_amount Float64,            -- USD amount
    token_amount Float64,          -- Outcome token amount
    transactionHash String          -- Transaction hash
) ENGINE = MergeTree()
ORDER BY (timestamp, market_id, transactionHash);
```

## Example Queries

### Trading Volume Analysis

```sql
-- Top markets by volume
SELECT 
    m.question,
    count() as trade_count,
    sum(t.usd_amount) as total_volume,
    avg(t.price) as avg_price
FROM polymarket.trades t
JOIN polymarket.markets m ON t.market_id = m.id
GROUP BY m.question
ORDER BY total_volume DESC
LIMIT 10;
```

### Trader Analysis

```sql
-- Top traders (excluding platform wallets)
SELECT 
    maker as trader,
    count() as trade_count,
    sum(usd_amount) as total_volume,
    avg(usd_amount) as avg_trade_size
FROM polymarket.trades
WHERE maker NOT IN (
    '0xc5d563a36ae78145c45a50134d48a1215220f80a',
    '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e'
)
GROUP BY trader
ORDER BY total_volume DESC
LIMIT 20;
```

### Time Series Analysis

```sql
-- Daily trading activity
SELECT 
    toStartOfDay(timestamp) as day,
    count() as trades,
    sum(usd_amount) as volume,
    uniq(maker) as unique_traders
FROM polymarket.trades
GROUP BY day
ORDER BY day DESC
LIMIT 30;
```

### Market Depth

```sql
-- Price distribution for a specific market
SELECT 
    market_id,
    round(price, 2) as price_bucket,
    count() as trade_count,
    sum(usd_amount) as volume
FROM polymarket.trades
WHERE market_id = 'YOUR_MARKET_ID'
GROUP BY market_id, price_bucket
ORDER BY price_bucket;
```

## Performance Tuning

### Memory Settings

For large datasets, you may need to increase ClickHouse memory limits:

Edit `/etc/clickhouse-server/config.xml` or `/etc/clickhouse-server/config.d/memory.xml`:

```xml
<clickhouse>
    <max_memory_usage>10000000000</max_memory_usage>  <!-- 10GB -->
    <max_bytes_before_external_group_by>5000000000</max_bytes_before_external_group_by>
</clickhouse>
```

### Query Optimization

- Use appropriate date ranges to filter data
- Always filter on indexed columns when possible (timestamp, market_id, etc.)
- Use `LIMIT` for exploratory queries
- Consider materialized views for frequently-run aggregations

## Troubleshooting

### Connection Issues

**Error: Connection refused**
```bash
# Check if ClickHouse is running
clickhouse-client --query "SELECT 1"

# Check logs
tail -f /var/log/clickhouse-server/clickhouse-server.log

# Restart server
brew services restart clickhouse  # macOS
sudo systemctl restart clickhouse-server  # Linux
```

**Error: Authentication failed**
- Check CLICKHOUSE_USER and CLICKHOUSE_PASSWORD
- Default user is `default` with empty password

### Data Loading Issues

**Tables not found**
- Run initial setup: `python clickhouse_initial_setup.py`

**Duplicate data**
- ClickHouse MergeTree allows duplicates by design
- Use `OPTIMIZE TABLE tablename FINAL DEDUPLICATE` if needed

**Slow inserts**
- Increase batch_size in load scripts (default: 10k-50k rows)
- Check disk I/O and memory

### Query Performance Issues

**Slow queries**
- Check if you're filtering on indexed columns
- Use `EXPLAIN` to understand query execution
- Consider adding more specific filters

## Backup and Maintenance

### Backup

```bash
# Backup to directory
clickhouse-client --query "BACKUP DATABASE polymarket TO Disk('backups', 'polymarket_backup')"

# Or use clickhouse-backup tool
clickhouse-backup create
```

### Optimize Tables

Periodically optimize tables to improve query performance:

```sql
OPTIMIZE TABLE polymarket.markets FINAL;
OPTIMIZE TABLE polymarket.order_filled FINAL;
OPTIMIZE TABLE polymarket.trades FINAL;
```

### Monitor Disk Space

```sql
-- Check table sizes
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size,
    sum(rows) as rows
FROM system.parts
WHERE database = 'polymarket' AND active
GROUP BY table;
```

## Advanced: Remote ClickHouse

For production deployments, consider:
- Running ClickHouse on a dedicated server
- Setting up ClickHouse cluster for horizontal scaling
- Using ClickHouse Cloud for managed service

Configure remote access in `/etc/clickhouse-server/config.xml`:

```xml
<listen_host>0.0.0.0</listen_host>
```

Then use environment variables on client:

```bash
export CLICKHOUSE_HOST=your-server.com
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=your_user
export CLICKHOUSE_PASSWORD=your_password
```

## Resources

- [ClickHouse Documentation](https://clickhouse.com/docs)
- [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference)
- [Performance Optimization](https://clickhouse.com/docs/en/operations/optimizing-performance)
- [ClickHouse Python Driver](https://clickhouse.com/docs/en/integrations/python)
