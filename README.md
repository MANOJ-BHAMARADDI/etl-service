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

```mermaid
graph TD
    subgraph "External"
        User([fa:fa-user User/Postman])
        PublicAPI[fa:fa-server Public API<br>(api.coincap.io)]
        CSVFile[fa:fa-file-csv Local CSV File]
    end

    subgraph "Your Docker Environment"
        subgraph "API Service (api)"
            APIServer[Express.js API]
            Scheduler[Cron Job (Hourly)]
            ETL[ETL Service]
        end

        subgraph "Database Service (mongo)"
            MongoDB[fa:fa-database MongoDB]
        end

        %% Data Flow (ETL Process)
        Scheduler -- Triggers --> ETL
        ETL -- "1. Fetches data via HTTP/S" --> PublicAPI
        ETL -- "2. Reads local file" --> CSVFile
        ETL -- "3. Transforms & Loads data" --> MongoDB

        %% User Interaction Flow
        User -- "Sends API Requests to" --> APIServer
        APIServer -- "Reads/Writes" --> MongoDB
    end

    %% Styling
    style User fill:#d6b656,stroke:#333
    style PublicAPI fill:#e6a349,stroke:#333
    style CSVFile fill:#e6a349,stroke:#333
    style APIServer fill:#7fb069,stroke:#333
    style Scheduler fill:#b6d7a8,stroke:#333
    style ETL fill:#b6d7a8,stroke:#333
    style MongoDB fill:#6993b0,stroke:#333
```

## 🛡️ How It Recovers From Failure

The system is designed with multiple layers of resilience to handle common failures gracefully.

**Failure Recovery (Idempotency):** The data loading process is idempotent. A unique compound index (`symbol`, `timestamp`) is enforced on the database collection. When loading data, the service uses an upsert operation. If a record already exists, it is updated; otherwise, it is inserted. This guarantees that re-running a failed job will not create duplicate data.

**Rate Limiting:** The ETL service automatically retries API calls with exponential backoff if it detects a rate-limit error (HTTP 429) or a temporary server error (HTTP 5xx). This prevents a temporary external issue from causing a complete pipeline failure.

**Schema Drift:** The pipeline gracefully handles changes in the source CSV file's column names. The transformation logic checks for known variations (e.g., `price_usd` or `usd_price`) and logs a warning without crashing, ensuring the run can complete even with minor schema changes.