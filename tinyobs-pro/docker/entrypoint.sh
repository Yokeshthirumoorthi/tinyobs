#!/bin/bash
set -e

echo "Starting ClickHouse server..."
clickhouse-server --config-file=/etc/clickhouse-server/config.xml &

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse to be ready..."
until clickhouse-client --query "SELECT 1" 2>/dev/null; do
    sleep 0.5
done
echo "ClickHouse is ready."

# Run init SQL if tables don't exist
if ! clickhouse-client --query "EXISTS TABLE otel_traces" 2>/dev/null | grep -q 1; then
    echo "Running ClickHouse init SQL..."
    clickhouse-client --multiquery < /docker-entrypoint-initdb.d/clickhouse-init.sql
    echo "ClickHouse tables created."
fi

echo "Starting tinyobs-pro..."
exec tinyobs-pro-server
