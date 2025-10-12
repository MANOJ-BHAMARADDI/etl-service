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
