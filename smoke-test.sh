#!/bin/bash
set -e

echo "Triggering ETL run..."
curl -s -X POST http://localhost:3000/api/refresh \
  -H "Authorization: Bearer test-token"

echo "Waiting 15 seconds for ETL to process..."
sleep 15

echo "Checking /metrics endpoint..."
METRICS=$(curl -s http://localhost:3000/metrics)
PROCESSED_ROWS=$(echo "$METRICS" | grep "etl_rows_processed_total" | awk '{print $2}')

if [ -z "$PROCESSED_ROWS" ] || [ $(echo "$PROCESSED_ROWS > 0" | bc) -eq 0 ]; then
  echo "FAIL: etl_rows_processed_total was not greater than 0."
  exit 1
else
  echo "SUCCESS: Found etl_rows_processed_total with value $PROCESSED_ROWS."
fi

echo "Checking /runs endpoint..."
RUNS_COUNT=$(curl -s http://localhost:3000/api/runs -H "Authorization: Bearer test-token" | jq 'length')

if [ "$RUNS_COUNT" -gt 0 ]; then
  echo "SUCCESS: Found $RUNS_COUNT ETL run(s)."
else
  echo "FAIL: No ETL runs found."
  exit 1
fi

echo "All smoke tests passed!"