# Polymarket Data

A comprehensive data pipeline for fetching, processing, and analyzing Polymarket trading data. This system collects market information, order-filled events, and processes them into structured trade data.

## Quick Download

**First-time users**: Download the [latest data snapshot](https://polydata-archive.s3.us-east-1.amazonaws.com/archive.tar.xz) and extract it in the main repository directory before your first run. This will save you over 2 days of initial data collection time.

## Overview

This pipeline performs three main operations:

1. **Market Data Collection** - Fetches all Polymarket markets with metadata
2. **Order Event Scraping** - Collects order-filled events from Goldsky subgraph
3. **Trade Processing** - Transforms raw order events into structured trade data

## Installation

This project uses [UV](https://docs.astral.sh/uv/) for fast, reliable package management.

### Install UV

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or with pip
pip install uv
```

### Install Dependencies

```bash
# Install all dependencies
uv sync

# Install with development dependencies (Jupyter, etc.)
uv sync --extra dev
```

## Quick Start

```bash
# Run with UV (recommended)
uv run python update_all.py

# Or activate the virtual environment first
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
python update_all.py
```

This will sequentially run all three pipeline stages:
- Update markets from Polymarket API
- Update order-filled events from Goldsky
- Process new orders into trades

## Project Structure

```
poly_data/
├── update_all.py              # Main orchestrator script
├── update_utils/              # Data collection modules
│   ├── update_markets.py      # Fetch markets from Polymarket API
│   ├── update_goldsky.py      # Scrape order events from Goldsky
│   └── process_live.py        # Process orders into trades
├── poly_utils/                # Utility functions
│   └── utils.py               # Market loading and missing token handling
├── markets.csv                # Main markets dataset
├── missing_markets.csv        # Markets discovered from trades (auto-generated)
├── goldsky/                   # Order-filled events (auto-generated)
│   └── orderFilled.csv
└── processed/                 # Processed trade data (auto-generated)
    └── trades.csv
```

## Data Files

### markets.csv
Market metadata including:
- Market question, outcomes, and tokens
- Creation/close times and slugs
- Trading volume and condition IDs
- Negative risk indicators

**Fields**: `createdAt`, `id`, `question`, `answer1`, `answer2`, `neg_risk`, `market_slug`, `token1`, `token2`, `condition_id`, `volume`, `ticker`, `closedTime`

### goldsky/orderFilled.csv
Raw order-filled events with:
- Maker/taker addresses and asset IDs
- Fill amounts and transaction hashes
- Unix timestamps

**Fields**: `timestamp`, `maker`, `makerAssetId`, `makerAmountFilled`, `taker`, `takerAssetId`, `takerAmountFilled`, `transactionHash`

### processed/trades.csv
Structured trade data including:
- Market ID mapping and trade direction
- Price, USD amount, and token amount
- Maker/taker roles and transaction details

**Fields**: `timestamp`, `market_id`, `maker`, `taker`, `nonusdc_side`, `maker_direction`, `taker_direction`, `price`, `usd_amount`, `token_amount`, `transactionHash`

## Pipeline Stages

### 1. Update Markets (`update_markets.py`)

Fetches all markets from Polymarket API in chronological order.

**Features**:
- Automatic resume from last offset (idempotent)
- Rate limiting and error handling
- Batch fetching (500 markets per request)

**Usage**:
```bash
uv run python -c "from update_utils.update_markets import update_markets; update_markets()"
```

### 2. Update Goldsky (`update_goldsky.py`)

Scrapes order-filled events from Goldsky subgraph API.

**Features**:
- Resumes from last timestamp automatically
- Handles GraphQL queries with pagination
- Deduplicates events

**Usage**:
```bash
uv run python -c "from update_utils.update_goldsky import update_goldsky; update_goldsky()"
```

### 3. Process Live Trades (`process_live.py`)

Processes raw order events into structured trades.

**Features**:
- Maps asset IDs to markets using token lookup
- Calculates prices and trade directions
- Identifies BUY/SELL sides
- Handles missing markets by discovering them from trades
- Incremental processing from last checkpoint

**Usage**:
```bash
uv run python -c "from update_utils.process_live import process_live; process_live()"
```

**Processing Logic**:
- Identifies non-USDC asset in each trade
- Maps to market and outcome token (token1/token2)
- Determines maker/taker directions (BUY/SELL)
- Calculates price as USDC amount per outcome token
- Converts amounts from raw units (divides by 10^6)

## Dependencies

Dependencies are managed via `pyproject.toml` and installed automatically with `uv sync`.

**Key Libraries**:
- `polars` - Fast DataFrame operations
- `pandas` - Data manipulation
- `gql` - GraphQL client for Goldsky
- `requests` - HTTP requests to Polymarket API
- `flatten-json` - JSON flattening for nested responses

**Development Dependencies** (optional, installed with `--extra dev`):
- `jupyter` - Interactive notebooks
- `notebook` - Jupyter notebook interface
- `ipykernel` - Python kernel for Jupyter

## Features

### Resumable Operations
All stages automatically resume from where they left off:
- **Markets**: Counts existing CSV rows to set offset
- **Goldsky**: Reads last timestamp from orderFilled.csv
- **Processing**: Finds last processed transaction hash

### Error Handling
- Automatic retries on network failures
- Rate limit detection and backoff
- Server error (500) handling
- Graceful fallbacks for missing data

### Missing Market Discovery
The processing stage automatically discovers markets that weren't in the initial markets.csv (e.g., markets created after last update) and fetches them via the Polymarket API, saving to `missing_markets.csv`.

## Data Schema Details

### Trade Direction Logic
- **Taker Direction**: BUY if paying USDC, SELL if receiving USDC
- **Maker Direction**: Opposite of taker direction
- **Price**: Always expressed as USDC per outcome token

### Asset Mapping
- `makerAssetId`/`takerAssetId` of "0" represents USDC
- Non-zero IDs are outcome token IDs (token1/token2 from markets)
- Each trade involves USDC and one outcome token

## Notes

- All amounts are normalized to standard decimal format (divided by 10^6)
- Timestamps are converted from Unix epoch to datetime
- Platform wallets (`0xc5d563a36ae78145c45a50134d48a1215220f80a`, `0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`) are tracked in `poly_utils/utils.py`
- Negative risk markets are flagged in the market data

## Troubleshooting

**Issue**: Markets not found during processing
**Solution**: Run `update_markets()` first, or let `process_live()` auto-discover them

**Issue**: Duplicate trades
**Solution**: Deduplication is automatic - re-run processing from scratch if needed

**Issue**: Rate limiting
**Solution**: The pipeline handles this automatically with exponential backoff

## Analysis

### Loading Data

```python
import pandas as pd
import polars as pl
from poly_utils import get_markets, PLATFORM_WALLETS

# Load markets
markets_df = get_markets()

# Load trades
df = pl.scan_csv("processed/trades.csv").collect(streaming=True)
df = df.with_columns(
    pl.col("timestamp").str.to_datetime().alias("timestamp")
)
```

### Filtering Trades by User

**Important**: When filtering for a specific user's trades, filter by the `maker` column. Even though it appears you're only getting trades where the user is the maker, this is how Polymarket generates events at the contract level. The `maker` column shows trades from that user's perspective including price.

```python
USERS = {
    'domah': '0x9d84ce0306f8551e02efef1680475fc0f1dc1344',
    '50pence': '0x3cf3e8d5427aed066a7a5926980600f6c3cf87b3',
    'fhantom': '0x6356fb47642a028bc09df92023c35a21a0b41885',
    'car': '0x7c3db723f1d4d8cb9c550095203b686cb11e5c6b',
    'theo4': '0x56687bf447db6ffa42ffe2204a05edaa20f55839'
}

# Get all trades for a specific user
trader_df = df.filter((pl.col("maker") == USERS['domah']))
```

## ClickHouse Integration

This repository includes a complete ClickHouse integration for storing and querying Polymarket data at scale.

### Prerequisites

1. **Install ClickHouse**: Follow the [official installation guide](https://clickhouse.com/docs/en/install)
   ```bash
   # macOS
   brew install clickhouse
   
   # Linux
   curl https://clickhouse.com/ | sh
   ```

2. **Start ClickHouse**:
   ```bash
   # Start server (may require sudo or use homebrew services)
   clickhouse-server
   
   # Or with homebrew
   brew services start clickhouse
   ```

3. **Install Python dependencies**:
   ```bash
   uv sync
   ```

### Configuration

Configure ClickHouse connection via environment variables (optional):

```bash
export CLICKHOUSE_HOST=localhost      # default
export CLICKHOUSE_PORT=8123           # default
export CLICKHOUSE_USER=default        # default
export CLICKHOUSE_PASSWORD=           # default: empty
export CLICKHOUSE_DATABASE=polymarket # default
```

### Initial Setup

Run this **once** to create tables and load existing CSV data:

```bash
uv run python clickhouse_initial_setup.py
```

This will:
- Create the `polymarket` database
- Create three tables: `markets`, `order_filled`, and `trades`
- Load all existing CSV data into ClickHouse

**Note**: This can take several minutes depending on data size.

### Incremental Updates

After initial setup, use this for regular updates:

```bash
uv run python update_all_clickhouse.py
```

This pipeline:
1. Updates markets from Polymarket API → `markets.csv`
2. Updates order events from Goldsky → `goldsky/orderFilled.csv`
3. Processes trades → `processed/trades.csv`
4. **Loads all new data into ClickHouse** (incremental)

### ClickHouse Schema

#### markets table
```sql
CREATE TABLE polymarket.markets (
    createdAt DateTime64(3),
    id String,
    question String,
    answer1 String,
    answer2 String,
    neg_risk Boolean,
    market_slug String,
    token1 String,
    token2 String,
    condition_id String,
    volume Float64,
    ticker String,
    closedTime Nullable(DateTime64(3))
) ENGINE = MergeTree()
ORDER BY (createdAt, id);
```

#### order_filled table
```sql
CREATE TABLE polymarket.order_filled (
    timestamp DateTime,
    maker String,
    makerAssetId String,
    makerAmountFilled UInt64,
    taker String,
    takerAssetId String,
    takerAmountFilled UInt64,
    transactionHash String
) ENGINE = MergeTree()
ORDER BY (timestamp, transactionHash, maker, taker);
```

#### trades table
```sql
CREATE TABLE polymarket.trades (
    timestamp DateTime64(6),
    market_id String,
    maker String,
    taker String,
    nonusdc_side String,
    maker_direction String,
    taker_direction String,
    price Float64,
    usd_amount Float64,
    token_amount Float64,
    transactionHash String
) ENGINE = MergeTree()
ORDER BY (timestamp, market_id, transactionHash);
```

### Querying Data

Connect to ClickHouse and run queries:

```bash
clickhouse-client
```

```sql
-- Check table sizes
SELECT 'markets' as table, count() as rows FROM polymarket.markets
UNION ALL
SELECT 'order_filled', count() FROM polymarket.order_filled
UNION ALL
SELECT 'trades', count() FROM polymarket.trades;

-- Get trading volume by market
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

-- Get top traders by volume
SELECT 
    maker as trader,
    count() as trade_count,
    sum(usd_amount) as total_volume
FROM polymarket.trades
WHERE maker NOT IN ('0xc5d563a36ae78145c45a50134d48a1215220f80a', 
                     '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e')
GROUP BY trader
ORDER BY total_volume DESC
LIMIT 20;

-- Trading activity over time
SELECT 
    toStartOfDay(timestamp) as day,
    count() as trades,
    sum(usd_amount) as volume
FROM polymarket.trades
GROUP BY day
ORDER BY day DESC
LIMIT 30;
```

### Python Analysis with ClickHouse

```python
import clickhouse_connect
from clickhouse_utils.config import get_client

# Get client
client = get_client()

# Query data directly
result = client.query("""
    SELECT 
        market_id,
        count() as trades,
        sum(usd_amount) as volume
    FROM polymarket.trades
    GROUP BY market_id
    ORDER BY volume DESC
    LIMIT 10
""")

# Convert to pandas DataFrame
df = result.result_df
print(df)
```

### Architecture

```
CSV Files (markets.csv, goldsky/, processed/)
    ↓
Initial Load (clickhouse_initial_setup.py)
    ↓
ClickHouse (polymarket database)
    ↓
Incremental Updates (update_all_clickhouse.py)
    ↑
New Data Pipeline (update_all.py)
```

### Performance

ClickHouse provides:
- **Fast aggregations**: 100M+ rows scanned in seconds
- **Efficient storage**: Columnar compression (10x+ smaller than CSV)
- **Real-time queries**: Sub-second response for most analytical queries
- **Scalability**: Handles billions of rows on a single node

### Troubleshooting

**Connection refused**:
- Ensure ClickHouse server is running: `clickhouse-client --query "SELECT 1"`
- Check logs: `tail -f /var/log/clickhouse-server/clickhouse-server.log`

**Permission denied**:
- Set proper CLICKHOUSE_USER and CLICKHOUSE_PASSWORD env vars
- Check ClickHouse user permissions

**Data not loading**:
- Run initial setup first: `python clickhouse_initial_setup.py`
- Check CSV files exist and are readable
- Review error messages in console output

## License

Go wild with it
