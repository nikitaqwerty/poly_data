#!/bin/bash

# Script to pause/unpause all services except clickhouse and redis
# Usage: ./pause_services.sh [pause|unpause]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Services to exclude (always keep running)
EXCLUDE_SERVICES=("redis" "clickhouse" "monitoring")

# Get all services except the excluded ones
ALL_SERVICES=$(docker compose config --services)
PAUSABLE_SERVICES=()

for service in $ALL_SERVICES; do
    exclude=false
    for excluded in "${EXCLUDE_SERVICES[@]}"; do
        if [ "$service" = "$excluded" ]; then
            exclude=true
            break
        fi
    done
    if [ "$exclude" = false ]; then
        PAUSABLE_SERVICES+=("$service")
    fi
done

# Function to pause services
pause_services() {
    echo "Pausing services (keeping redis and clickhouse running)..."
    for service in "${PAUSABLE_SERVICES[@]}"; do
        echo "  Pausing: $service"
        docker compose pause "$service" 2>/dev/null || echo "    (service not running or already paused)"
    done
    echo "✓ Services paused successfully"
    echo ""
    echo "Running services:"
    docker compose ps --filter "status=running"
}

# Function to unpause services
unpause_services() {
    echo "Unpausing all services..."
    for service in "${PAUSABLE_SERVICES[@]}"; do
        echo "  Unpausing: $service"
        docker compose unpause "$service" 2>/dev/null || echo "    (service not paused)"
    done
    echo "✓ Services unpaused successfully"
    echo ""
    echo "Running services:"
    docker compose ps --filter "status=running"
}

# Main logic
case "${1:-}" in
    pause)
        pause_services
        ;;
    unpause)
        unpause_services
        ;;
    *)
        echo "Usage: $0 [pause|unpause]"
        echo ""
        echo "Commands:"
        echo "  pause   - Pause all services except redis and clickhouse"
        echo "  unpause - Unpause all services"
        echo ""
        echo "Services that will be paused/unpaused:"
        for service in "${PAUSABLE_SERVICES[@]}"; do
            echo "  - $service"
        done
        exit 1
        ;;
esac
