# 🔍 Proof & Visibility (P0 Requirements)

This section provides verifiable proof of the system's core features, including observability, traceability, and resilience.

## Real-Time Metrics (`/metrics`)

The service exposes Prometheus-compatible metrics for real-time monitoring. This output shows processed rows, errors, per-source throttle events, and latency.

Example `/metrics` Output:

```
# HELP etl_rows_processed_total Total number of rows processed
# TYPE etl_rows_processed_total counter
etl_rows_processed_total 12

# HELP etl_errors_total Total number of ETL errors
# TYPE etl_errors_total counter
etl_errors_total{type="process_failure"} 0
etl_errors_total{type="schema_drift_low_confidence"} 1

# HELP throttle_events_total Total number of throttle events
# TYPE throttle_events_total counter
throttle_events_total{source="coingecko"} 2
throttle_events_total{source="blockchain"} 0

# HELP etl_latency_seconds ETL run latency in seconds
# TYPE etl_latency_seconds histogram
etl_latency_seconds_bucket{le="10"} 1
etl_latency_seconds_sum 8.3
etl_latency_seconds_count 1
```

## ETL Run Traceability (`/api/runs/:id`)

Each ETL process is tracked with a unique `run_id`. You can fetch detailed metadata, including batch status and applied schema mappings.

Example JSON Output for a Successful Run:

```json
{
  "run": {
    "run_id": "run_a4f1e9b2-7b1e-4b7e-8f5c-9c7f3e6a0d2a",
    "start_time": "2025-10-16T15:00:00.123Z",
    "end_time": "2025-10-16T15:00:15.456Z",
    "status": "completed",
    "rows_processed": 12,
    "errors": []
  },
  "checkpoints": [
    {
      "run_id": "run_a4f1e9b2-7b1e-4b7e-8f5c-9c7f3e6a0d2a",
      "source": "all_sources",
      "batch_no": 1,
      "offset": 12,
      "status": "completed"
    }
  ],
  "schema_version": {
    "source": "csv",
    "version": 1665903615000,
    "schema": ["ticker", "price_in_usd", "tx_volume", "time_int"],
    "mappings": {
      "price_in_usd": "price_usd",
      "time_int": "time"
    },
    "confidence": 0.85
  }
}
```

## Proof of Incremental Loads (Idempotency)

The ETL process is idempotent. Running it multiple times will not create duplicate records, which is critical for data integrity after a failure.

1. Before First Run (Database is empty):

```bash
docker-compose exec mongo mongosh --eval "db.getSiblingDB('market_data').marketdata.countDocuments()"
# Expected Output: 0
```

2. Trigger the First ETL Run:

```bash
make refresh
```

3. After First Run (Records are inserted):

```bash
docker-compose exec mongo mongosh --eval "db.getSiblingDB('market_data').marketdata.countDocuments()"
# Expected Output: 12
```

4. Trigger a Second Run (No new records are created):

```bash
make refresh
docker-compose exec mongo mongosh --eval "db.getSiblingDB('market_data').marketdata.countDocuments()"
# Expected Output: 12
```

If you'd like this section inline in `README.md` instead of a separate file, I can update `README.md` to include it. Otherwise I'll add a small link to it now.
