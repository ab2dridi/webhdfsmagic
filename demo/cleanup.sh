#!/bin/bash
# Cleanup script for smart cat demo environment

echo "🧹 Cleaning up smart cat demo environment..."

# Stop Docker containers
cd /workspaces/webhdfsmagic/demo
echo "Stopping Docker containers..."
docker-compose down -v

# Remove temporary test files
echo "Removing temporary files..."
rm -f /tmp/sales.csv
rm -f /tmp/employees.tsv
rm -f /tmp/records.parquet
rm -f /tmp/european_data.csv
rm -f /tmp/create_parquet.py
rm -f /tmp/upload_test_data.py

echo "✅ Cleanup complete!"
