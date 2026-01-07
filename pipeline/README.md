# Polymarket Real-time Pipeline

A production-ready, Redis-based real-time data pipeline for Polymarket data that continuously ingests, processes, and loads data into ClickHouse.

## 🏗️ Architecture

```
┌─────────────────┐         ┌──────────────┐         ┌──────────────────┐
│   Polymarket    │────────▶│    Redis     │────────▶│   ClickHouse     │
│   API Ingester  │         │   Streams    │         │     Writer       │
└─────────────────┘         └──────────────┘         └──────────────────┘
                                   │
┌─────────────────┐                │                  ┌──────────────────┐
│    Goldsky      │────────────────┤                  │   Monitoring     │
│  GraphQL Ingester│                │                  │   Dashboard      │
└─────────────────┘                │                  └──────────────────┘
                                   │
                           ┌───────▼────────┐
                           │     Trade      │
                           │   Processor    │
                           └────────────────┘
```

## 🚀 Features

- **Real-time processing**: Data flows continuously with <30 second latency
- **Modular microservices**: Each component runs independently
- **Fault tolerant**: Auto-restart, Redis persistence, exactly-once semantics
- **Scalable**: Redis Streams provide natural scaling path
- **Observable**: Built-in monitoring dashboard and structured logging
- **Easy deployment**: Docker Compose or Supervisor for process management

## 📦 Components

### Ingesters
- **polymarket_ingester.py**: Polls Polymarket API for markets data
- **polymarket_events_ingester.py**: Polls Polymarket API for events data (including tags)
- **goldsky_ingester.py**: Polls Goldsky GraphQL for order events
- **address_checker_ingester.py**: Checks addresses from trades to determine if they're contracts or wallets

### Processors
- **trade_processor.py**: Processes order events into trades
- **clickhouse_writer.py**: Batch writes to ClickHouse (markets, events, trades, address_metadata)

### Monitoring
- **dashboard.py**: FastAPI web dashboard (http://localhost:8000)

> **Note**: Markets and events ingesters are decoupled for efficiency. See `DECOUPLING_NOTES.md` for details.

## 🛠️ Setup

### Prerequisites

1. **Redis** (local or Docker)
2. **ClickHouse** (with polymarket database)
3. **Python 3.11+**

### Quick Start with Docker Compose (Recommended)

```bash
# 1. Build and start all services
cd pipeline
docker-compose up -d

# 2. Check status
docker-compose ps
docker-compose logs -f

# 3. Open monitoring dashboard
open http://localhost:8000
```

### Manual Setup

```bash
# 1. Install dependencies
cd pipeline
pip install -r requirements.txt

# 2. Start Redis (if not running)
redis-server

# 3. Configure environment (optional)
export REDIS_HOST=localhost
export REDIS_PORT=6379
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_DATABASE=polymarket

# 4. Start services with Supervisor
supervisord -c supervisord.conf

# 5. Check status
supervisorctl status

# 6. View logs
tail -f logs/*.log
```

## 🎛️ Configuration

Edit `common/config.py` or use environment variables:

### Redis Configuration
- `REDIS_HOST` (default: localhost)
- `REDIS_PORT` (default: 6379)
- `REDIS_DB` (default: 0)
- `REDIS_PASSWORD` (optional)

### ClickHouse Configuration
- `CLICKHOUSE_HOST` (default: localhost)
- `CLICKHOUSE_PORT` (default: 8123)
- `CLICKHOUSE_USER` (default: default)
- `CLICKHOUSE_PASSWORD` (optional)
- `CLICKHOUSE_DATABASE` (default: polymarket)

### Address Checker Configuration
- `POLYGON_RPC_URL` - **Required** for address checker ingester (e.g., https://polygon-rpc.com or Infura/Alchemy endpoint)

### Processing Configuration
See `common/config.py` for batch sizes, intervals, etc.

## 📊 Monitoring

### Web Dashboard
Visit http://localhost:8000 for real-time status:
- Redis stream lengths
- Ingester progress (offsets/timestamps)
- ClickHouse table counts
- Auto-refreshes every 5 seconds

### API Endpoints
- `GET /` - HTML dashboard
- `GET /api/status` - JSON status
- `GET /health` - Health check

### Logs
All components log to:
- Console (stdout)
- Files in `logs/` directory

## 🔧 Operations

### Start/Stop Services

**Docker Compose:**
```bash
docker-compose start
docker-compose stop
docker-compose restart <service-name>
```

**Supervisor:**
```bash
supervisorctl start all
supervisorctl stop all
supervisorctl restart polymarket_ingester
supervisorctl status
```

### View Logs

**Docker:**
```bash
docker-compose logs -f polymarket-ingester
docker-compose logs -f --tail=100
```

**Supervisor:**
```bash
tail -f logs/polymarket_ingester.log
tail -f logs/*.log  # All logs
```

### Redis CLI

```bash
# Check stream lengths
redis-cli XLEN stream:markets
redis-cli XLEN stream:order_events
redis-cli XLEN stream:trades

# Check state
redis-cli GET state:polymarket_offset
redis-cli GET state:goldsky_last_timestamp

# View stream contents (last 10 messages)
redis-cli XREVRANGE stream:markets + - COUNT 10
```

### Reset Pipeline

```bash
# WARNING: This deletes all Redis data

# Delete streams
redis-cli DEL stream:markets
redis-cli DEL stream:order_events
redis-cli DEL stream:trades

# Delete state
redis-cli DEL state:polymarket_offset
redis-cli DEL state:goldsky_last_timestamp

# Or flush entire Redis DB (careful!)
redis-cli FLUSHDB
```

## 🐛 Troubleshooting

### Redis Connection Failed
```bash
# Check Redis is running
redis-cli ping  # Should return PONG

# Check connection
netstat -an | grep 6379
```

### ClickHouse Connection Failed
```bash
# Test connection
curl http://localhost:8123/
echo "SELECT 1" | curl -s "http://localhost:8123/" --data-binary @-

# Check if database exists
echo "SHOW DATABASES" | curl -s "http://localhost:8123/" --data-binary @-
```

### Service Not Starting
```bash
# Check logs
tail -f logs/<service-name>.log

# Test manually
python3 ingesters/polymarket_ingester.py
```

### High Memory Usage
- Reduce batch sizes in `common/config.py`
- Enable Redis maxmemory policy
- Increase stream trimming frequency

## 🔄 Data Flow

1. **Ingesters** poll APIs continuously
2. Push new data to Redis Streams
3. **Trade Processor** reads order events, processes trades
4. Pushes processed trades to trades stream
5. **ClickHouse Writer** reads from streams, batches writes
6. Data appears in ClickHouse tables

## 📈 Performance

**Expected throughput (single machine):**
- Markets: 500-1000/min
- Order events: 5k-10k/min (processed to trades)
- Trades: 3k-8k/min
- End-to-end latency: 10-30 seconds

**Resource usage:**
- CPU: 10-30%
- RAM: 500MB-2GB
- Redis: 100-500MB
- Network: Minimal

## 📊 ClickHouse Tables

The pipeline creates and populates these main tables:

### markets
Stores market information from Polymarket API
- ReplacingMergeTree engine (deduplication by id)
- Contains: market details, tokens, conditions, volume, tags

### polymarket_events
Stores event information from Polymarket API
- ReplacingMergeTree engine
- Contains: event details, tags, markets count, volume, liquidity

### trades
Stores processed trade data from order events
- MergeTree engine
- Partitioned by month (toYYYYMM)
- Contains: timestamp, market_id, maker, taker, price, volumes, direction

### address_metadata
Stores metadata about addresses (makers/takers from trades)
- ReplacingMergeTree engine (latest check wins)
- Contains: address, address_type (contract/wallet), transaction_count, first_transaction_date, checked_at
- Automatically populated by address_checker_ingester

## 🚦 Next Steps

### Scaling Up
- Add more processor workers (modify docker-compose.yml)
- Use Redis Cluster for HA
- Deploy to multiple machines

### Production Hardening
- Add Prometheus metrics
- Set up alerting (PagerDuty, etc.)
- Implement circuit breakers
- Add health check endpoints to load balancer

### Advanced Features
- Add replay capability (from stream history)
- Implement exactly-once with Redis transactions
- Add data validation layer
- Create dead-letter queue for failed messages

## 🔍 Address Checker Feature

The address checker ingester automatically identifies whether addresses in the trades table are smart contracts or externally owned accounts (wallets).

### How It Works
1. Pulls distinct maker and taker addresses from the `trades` table
2. Checks addresses that haven't been processed yet (not in `address_metadata`)
3. Queries Polygon RPC using `eth_getCode` to determine address type
4. Queries `eth_getTransactionCount` to get total transactions
5. Writes results to `address_metadata` table

### Setup
```bash
# 1. Add Polygon RPC URL to .env
echo "POLYGON_RPC_URL=https://polygon-rpc.com" >> .env
# Or use a provider like Infura, Alchemy, or Goldsky:
# echo "POLYGON_RPC_URL=https://polygon-mainnet.infura.io/v3/YOUR_KEY" >> .env

# 2. Initialize the infrastructure
python3 init_address_checker.py

# 3. Create the table in ClickHouse
python3 setup_schema.py

# 4. Start the ingester (if using supervisord)
supervisorctl start address_checker_ingester
```

### Query Examples
```sql
-- Get all contract addresses
SELECT address, transaction_count, checked_at
FROM polymarket.address_metadata
WHERE address_type = 'contract'
ORDER BY transaction_count DESC;

-- Join with trades to see contract trading activity
SELECT t.maker, t.taker, am.address_type, t.usd_amount
FROM polymarket.trades t
LEFT JOIN polymarket.address_metadata am ON t.maker = am.address
WHERE am.address_type = 'contract'
LIMIT 100;

-- Count addresses by type
SELECT address_type, COUNT(*) as count, SUM(transaction_count) as total_txs
FROM polymarket.address_metadata
GROUP BY address_type;
```

### Notes
- The ingester processes addresses in batches (default 500 per batch)
- Includes rate limiting to avoid overwhelming RPC endpoints
- Uses ReplacingMergeTree so addresses can be re-checked (latest wins)
- First transaction date is currently NULL (can be enhanced with blockchain indexer)

## 📝 Notes

- Redis Streams provide ordering and durability
- Consumer groups ensure messages aren't processed twice
- Batch writes optimize ClickHouse performance
- State is persisted in Redis for crash recovery
- Services auto-restart on failure

## 🤝 Contributing

Feel free to extend with:
- Additional data sources
- More processing logic
- Enhanced monitoring
- Performance optimizations

---

**Built with:** Python, Redis Streams, ClickHouse, FastAPI, Docker
