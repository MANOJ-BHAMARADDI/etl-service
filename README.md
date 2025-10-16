# ETL System

A resilient Market Data ETL and API service built with Node.js, Express, and MongoDB, fully containerized with Docker. This project ingests data from a public API and a local CSV file, normalizes it, and serves it via a RESTful API, while handling common system failures gracefully.

## 🚀 How to Run

Follow these steps to build, configure, and run the entire application on your local machine.

### 1. Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop/)
- [Docker Compose](https://docs.docker.com/compose/install/) (usually included with Docker Desktop)
- [Git](https://git-scm.com/)

### 2. Clone the Repository

Open your terminal and clone the project:

```bash
git clone https://github.com/MANOJ-BHAMARADDI/etl-service
cd etl-service
```

### 3. Configure Environment Variables

The application requires a `.env` file for the database connection string and API secrets.

1.  Create a copy of the example file:
    ```bash
    cp .env.example .env
    ```
2.  Open the newly created `.env` file. It should contain the following content. **Add your own unique secret** for the `SECRET_REFRESH_TOKEN`.
    ```env
    PORT=3000
    MONGO_URI=mongodb://root:example@mongo:27017/market_data?authSource=admin
    SECRET_REFRESH_TOKEN=mySuperSecretToken123
    ```

### 4. Build and Start the Services

Run the following command from the project's root directory. This will build the Docker images and start the API and database containers in the background.

```bash
docker-compose up -d --build
```

The `-d` flag runs the containers in detached mode.

### 5. Run the Initial ETL Process

The database starts empty. To populate it with data, you must trigger the ETL process by sending a request to the `/refresh` endpoint.

```bash
curl -X POST http://localhost:3000/api/refresh \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer mySuperSecretToken123"
```

_(**Note**: Replace `mySuperSecretToken123` with the actual token you set in your `.env` file.)_

Your API is now running and accessible at `http://localhost:3000`. The ETL process is also scheduled to run automatically every hour.

## ⚙️ API Usage

### Health Check

Reports the status of the API and its database connection.

**Request:** `GET /health`

---

### Trigger ETL

Manually triggers a new ETL run. This endpoint is protected and requires a Bearer Token.

**Request:** `POST /api/refresh`

**Headers:** `Authorization: Bearer <your_secret_token>`

---

### Get ETL Statistics

Returns metadata about the ETL process, including total record count and the status of the last run.

**Request:** `GET /api/stats`

---

### Fetch Market Data

Fetches market data with support for filtering, sorting, and pagination.

**Request:** `GET /api/data`

**Query Parameters:**

- `symbol` (string): Filter by a specific stock/crypto symbol (e.g., `?symbol=BTC`).
- `sortBy` (string): Sort the results (e.g., `?sortBy=price_usd:desc`).
- `page` (number): The page number for pagination (e.g., `?page=2`).
- `limit` (number): The number of results per page (e.g., `?limit=50`).

## 🏗️ Architecture Diagram

The system consists of two primary services orchestrated by Docker Compose:

- **API Service (`api`)**: A Node.js container running an Express.js server. It exposes the REST API for clients and contains the `node-cron` scheduler that triggers the ETL process.
- **Database Service (`mongo`)**: A standard MongoDB container that provides persistent storage for the market data and ETL run logs.

![System Architecture Diagram](./AD.png)

## 🛡️ How It Recovers From Failure

The system is designed with multiple layers of resilience to handle common failures gracefully.

**Failure Recovery (Idempotency):** The data loading process is idempotent. A unique compound index (`symbol`, `timestamp`) is enforced on the database collection. When loading data, the service uses an upsert operation. If a record already exists, it is updated; otherwise, it is inserted. This guarantees that re-running a failed job will not create duplicate data.

**Rate Limiting:** The ETL service automatically retries API calls with exponential backoff if it detects a rate-limit error (HTTP 429) or a temporary server error (HTTP 5xx). This prevents a temporary external issue from causing a complete pipeline failure.

**Schema Drift:** The pipeline gracefully handles changes in the source CSV file's column names. The transformation logic checks for known variations (e.g., `price_usd` or `usd_price`) and logs a warning without crashing, ensuring the run can complete even with minor schema changes.

## Advanced Proof Checklist

### Rate Limiting and Throttling

To demonstrate rate limiting, you can rapidly trigger the ETL process.

```bash
# Trigger the ETL process multiple times in a row
for i in {1..15}; do curl -X POST http://localhost:3000/api/refresh -H "Authorization: Bearer mySuperSecretToken123"; done
```

## 🔍 Proof & Visibility (P0 Requirements)

This section provides verifiable proof of the system's core features, including observability, traceability, and resilience.

### Real-Time Metrics (/metrics)

The service exposes Prometheus-compatible metrics for real-time monitoring. This output shows processed rows, errors, per-source throttle events, and latency.

Example /metrics Output:

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

### ETL Run Traceability (/api/runs/:id)

Each ETL process is tracked with a unique run_id. You can fetch detailed metadata, including batch status and applied schema mappings.

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

### Proof of Incremental Loads (Idempotency)

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

### Rate Limiting and Throttling

To demonstrate rate limiting, you can rapidly trigger the ETL process.

```bash
# Trigger the ETL process multiple times in a row
for i in {1..15}; do curl -X POST http://localhost:3000/api/refresh -H "Authorization: Bearer mySuperSecretToken123"; done
```
