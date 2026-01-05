#!/bin/bash
# Stop all pipeline services

echo "🛑 Stopping Polymarket Pipeline"
echo "================================"
echo ""

# Check which mode is running
if docker-compose ps | grep -q "Up"; then
    echo "🐳 Stopping Docker Compose services..."
    docker-compose down
    echo "✓ Docker Compose stopped"
elif supervisorctl status > /dev/null 2>&1; then
    echo "🔧 Stopping Supervisor services..."
    supervisorctl stop all
    echo "✓ Supervisor services stopped"
else
    echo "🔬 Stopping individual processes..."
    
    # Kill Python processes
    pkill -f "polymarket_ingester.py"
    pkill -f "goldsky_ingester.py"
    pkill -f "trade_processor.py"
    pkill -f "clickhouse_writer.py"
    pkill -f "monitoring/dashboard.py"
    
    echo "✓ Processes stopped"
fi

echo ""
echo "Pipeline stopped successfully!"
