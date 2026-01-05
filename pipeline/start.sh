#!/bin/bash
# Quick start script for the pipeline

set -e

echo "🚀 Polymarket Pipeline Startup Script"
echo "======================================"
echo ""

# Check if Redis is running
echo "📡 Checking Redis connection..."
if redis-cli ping > /dev/null 2>&1; then
    echo "✓ Redis is running"
else
    echo "✗ Redis is not running!"
    echo "  Please start Redis first:"
    echo "    redis-server"
    exit 1
fi

# Check if ClickHouse is running
echo "📡 Checking ClickHouse connection..."
if curl -s http://localhost:8123/ > /dev/null 2>&1; then
    echo "✓ ClickHouse is running"
else
    echo "✗ ClickHouse is not running!"
    echo "  Please start ClickHouse first"
    exit 1
fi

# Check if Python dependencies are installed
echo "📦 Checking Python dependencies..."
if python3 -c "import redis; import clickhouse_connect; import fastapi" 2>/dev/null; then
    echo "✓ Dependencies installed"
else
    echo "⚠ Some dependencies missing, installing..."
    pip install -r requirements.txt
fi

echo ""
echo "Choose startup mode:"
echo "  1) Docker Compose (recommended)"
echo "  2) Supervisor (manual process management)"
echo "  3) Individual processes (for testing)"
echo ""
read -p "Enter choice [1-3]: " choice

case $choice in
    1)
        echo ""
        echo "🐳 Starting with Docker Compose..."
        docker-compose up -d
        echo ""
        echo "✓ All services started!"
        echo ""
        echo "📊 Monitoring dashboard: http://localhost:8000"
        echo ""
        echo "To view logs:"
        echo "  docker-compose logs -f"
        echo ""
        echo "To stop:"
        echo "  docker-compose down"
        ;;
    2)
        echo ""
        echo "🔧 Starting with Supervisor..."
        
        # Check if supervisor is installed
        if ! command -v supervisord &> /dev/null; then
            echo "⚠ Supervisor not installed, installing..."
            pip install supervisor
        fi
        
        supervisord -c supervisord.conf
        echo ""
        echo "✓ All services started!"
        echo ""
        echo "📊 Monitoring dashboard: http://localhost:8000"
        echo ""
        echo "To check status:"
        echo "  supervisorctl status"
        echo ""
        echo "To view logs:"
        echo "  tail -f logs/*.log"
        echo ""
        echo "To stop:"
        echo "  supervisorctl stop all"
        ;;
    3)
        echo ""
        echo "🔬 Starting individual processes..."
        echo ""
        echo "Starting in separate terminals:"
        echo ""
        echo "Terminal 1: python3 ingesters/polymarket_ingester.py"
        echo "Terminal 2: python3 ingesters/polymarket_events_ingester.py"
        echo "Terminal 3: python3 ingesters/goldsky_ingester.py"
        echo "Terminal 4: python3 processors/trade_processor.py"
        echo "Terminal 5: python3 processors/clickhouse_writer.py"
        echo "Terminal 6: python3 monitoring/dashboard.py"
        echo ""
        echo "Or run in background with:"
        echo "  python3 ingesters/polymarket_ingester.py > logs/polymarket_markets.log 2>&1 &"
        echo "  python3 ingesters/polymarket_events_ingester.py > logs/polymarket_events.log 2>&1 &"
        echo "  python3 ingesters/goldsky_ingester.py > logs/goldsky.log 2>&1 &"
        echo "  python3 processors/trade_processor.py > logs/processor.log 2>&1 &"
        echo "  python3 processors/clickhouse_writer.py > logs/writer.log 2>&1 &"
        echo "  python3 monitoring/dashboard.py > logs/monitoring.log 2>&1 &"
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac
