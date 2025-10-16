import axios from "axios";
import { createReadStream } from "fs";
import { join } from "path";
import csv from "csv-parser";
import MarketData from "../api/models/marketData.model.js";
import RawMarketData from "../api/models/rawMarketData.model.js";
import EtlRun from "../api/models/etlRun.model.js";
import EtlCheckpoint from "../api/models/etlCheckpoint.model.js";
import SchemaVersion from "../api/models/schemaVersion.model.js";
import { fileURLToPath } from "url";
import { dirname } from "path";
import rateController from "./rate.controller.js";
import client from "prom-client";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// --- Rate Limiter and Caching ---
// Prometheus Metrics
const etlRowsProcessed = new client.Counter({ name: 'etl_rows_processed_total', help: 'Total number of rows processed' });
const etlErrors = new client.Counter({ name: 'etl_errors_total', help: 'Total number of ETL errors', labelNames: ['type'] });
const etlLatency = new client.Histogram({ name: 'etl_latency_seconds', help: 'ETL run latency in seconds', buckets: [1, 5, 10, 30, 60] });
const throttleEvents = new client.Counter({ name: 'throttle_events_total', help: 'Total number of throttle events' });

const quotas = {
  coingecko: 10,
  blockchain: 3,
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function rateLimiter(source, maxRequestsPerMinute) {
  const bucket = rateController(source, maxRequestsPerMinute, maxRequestsPerMinute, 60000);
  while (!(await bucket.take())) {
    console.warn(`[RATE LIMIT] ${source} limit reached. Waiting...`);
    throttleEvents.inc();
    await delay(1000);
  }
}

/**
 * --- EXTRACT ---
 * Fetches data from the CoinGecko public API (Source A).
 */
const fetchFromApiSourceA = async (retries = 3, waitTime = 5000) => {
  await rateLimiter("coingecko", quotas.coingecko);
  try {
    const response = await axios.get(
      "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1"
    );
    console.log("Successfully fetched data from CoinGecko API.");
    CACHE.coingecko = response.data;
    return response.data;
  } catch (error) {
    if (
      retries > 0 &&
      (error.response?.status === 429 || error.response?.status >= 500)
    ) {
      console.warn(
        `CoinGecko API fetch failed with status ${
          error.response.status
        }. Retrying in ${waitTime / 1000}s... (${retries} retries left)`
      );
      await delay(waitTime);
      return fetchFromApiSourceA(retries - 1, waitTime * 2);
    }
    console.error("Error fetching data from CoinGecko API:", error.message);
    if (CACHE.coingecko) {
      console.warn("Falling back to cached data for CoinGecko.");
      return CACHE.coingecko;
    }
    throw new Error(
      "CoinGecko API fetch failed after multiple retries and no cache available."
    );
  }
};

/**
 * --- EXTRACT ---
 * Fetches data from a local CSV file (Source B).
 */
const fetchFromCsvSourceB = (offset = 0) => {
  return new Promise((resolve, reject) => {
    const results = [];
    const filePath = join(__dirname, "../../market_data_source.csv");

    createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {
        console.log("Successfully processed data from CSV.");
        resolve(results);
      })
      .on("error", (error) => {
        console.error("Error reading CSV file:", error.message);
        reject(new Error("CSV read failed"));
      });
  });
};

/**
 * --- EXTRACT ---
 * Fetches data from the Blockchain.com API (Source C).
 */
const fetchFromApiSourceC = async (retries = 3, waitTime = 5000) => {
  await rateLimiter("blockchain", quotas.blockchain);
  try {
    const response = await axios.get(
      "https://api.blockchain.com/v3/exchange/tickers"
    );
    console.log("Successfully fetched data from Blockchain.com API.");
    CACHE.blockchain = response.data;
    return response.data;
  } catch (error) {
    if (
      retries > 0 &&
      (error.response?.status === 429 || error.response?.status >= 500)
    ) {
      console.warn(
        `Blockchain.com API fetch failed with status ${
          error.response.status
        }. Retrying in ${waitTime / 1000}s... (${retries} retries left)`
      );
      await delay(waitTime);
      return fetchFromApiSourceC(retries - 1, waitTime * 2);
    }
    console.error(
      "Error fetching data from Blockchain.com API:",
      error.message
    );
    if (CACHE.blockchain) {
      console.warn("Falling back to cached data for Blockchain.com.");
      return CACHE.blockchain;
    }
    throw new Error(
      "Blockchain.com API fetch failed after multiple retries and no cache available."
    );
  }
};

/**
 * --- TRANSFORM ---
 * Normalizes data from all sources into our unified schema.
 */
const transformData = (apiDataA, csvData, apiDataC) => {
  // --- TRANSFORMING COINGECKO DATA (Source A) ---
  const transformedApiDataA = apiDataA.map((item) => ({
    symbol: item.symbol?.toUpperCase(),
    price_usd: parseFloat(item.current_price),
    volume: parseFloat(item.total_volume),
    source: "api_coingecko",
    timestamp: new Date(item.last_updated || Date.now()),
  }));

  // --- TRANSFORMING CSV DATA (Source B) ---
  const transformedCsvData = csvData.map((item) => {
    const price = item.price_usd || item.usd_price;
    if (!item.price_usd && item.usd_price) {
      console.warn(
        `[SCHEMA DRIFT] Detected column 'usd_price' instead of 'price_usd' in CSV.`
      );
    }
    return {
      symbol: item.ticker,
      price_usd: parseFloat(price),
      volume: parseFloat(item.tx_volume),
      source: "csv",
      timestamp: new Date(item.time),
    };
  });

  // --- TRANSFORMING BLOCKCHAIN.COM DATA (Source C) ---
  const transformedApiDataC = apiDataC.map((item) => ({
    symbol: item.symbol.replace("-USD", ""),
    price_usd: parseFloat(item.last_trade_price),
    volume: parseFloat(item.volume_24h),
    source: "api_blockchain",
    timestamp: new Date(), // Blockchain.com API does not provide a timestamp
  }));

  const allData = [
    ...transformedApiDataA,
    ...transformedCsvData,
    ...transformedApiDataC,
  ];

  // --- VALIDATION AND TYPE RECONCILIATION ---
  const validatedData = allData.filter((item) => {
    if (!item.symbol || isNaN(item.price_usd) || !item.timestamp) {
      console.warn(
        `[VALIDATION FAILED] Skipping invalid record: ${JSON.stringify(item)}`
      );
      return false;
    }
    return true;
  });

  console.log("Data transformed successfully.");
  return validatedData;
};

/**
 * --- LOAD ---
 * Loads the transformed data into the MongoDB database.
 */
const loadData = async (data, rawData, run_id, source, batch_no, offset) => {
  if ((!data || data.length === 0) && (!rawData || rawData.length === 0)) {
    console.log("No data to load.");
    return { normalized: 0, raw: 0 };
  }

  // Load normalized data
  let rowsProcessed = 0;
  if (data && data.length > 0) {
    const operations = data.map((record) => ({
      updateOne: {
        filter: { symbol: record.symbol, timestamp: record.timestamp },
        update: { $set: record },
        upsert: true,
      },
    }));
    const result = await MarketData.bulkWrite(operations);
    rowsProcessed = result.upsertedCount + result.modifiedCount;
    console.log(
      `Normalized data loaded into DB. Rows processed: ${rowsProcessed}`
    );
  }

  // Load raw data
  let rawRowsProcessed = 0;
  if (rawData && rawData.length > 0) {
    await RawMarketData.insertMany(rawData.map((d) => ({ data: d })));
    rawRowsProcessed = rawData.length;
    console.log(`Raw data loaded into DB. Rows processed: ${rawRowsProcessed}`);
  }
  await EtlCheckpoint.create({
    run_id,
    source,
    batch_no,
    offset,
    status: "completed",
  });
  return { normalized: rowsProcessed, raw: rawRowsProcessed };
};

/**
 * --- ORCHESTRATOR ---
 * Main function to run the entire ETL process.
 */
const runEtlProcess = async () => {
  const etlRun = new EtlRun();
  await etlRun.save();
  console.log(`Starting ETL run with ID: ${etlRun.run_id}`);
  const summary = {
    run_id: etlRun.run_id,
    startTime: new Date().toISOString(),
    status: "started",
    records_processed: 0,
    errors: [],
  };

  try {
    const [apiDataA, csvData, apiDataC] = await Promise.all([
      fetchFromApiSourceA(),
      fetchFromCsvSourceB(),
      fetchFromApiSourceC(),
    ]);

    const rawData = [...apiDataA, ...csvData, ...apiDataC];
    const transformedData = transformData(apiDataA, csvData, apiDataC);
    const { normalized, raw } = await loadData(transformedData, rawData);

    etlRun.status = "completed";
    etlRun.end_time = new Date();
    etlRun.rows_processed = normalized;
    await etlRun.save();

    summary.status = "completed";
    summary.records_processed = normalized;

    console.log(`ETL run ${etlRun.run_id} completed successfully.`);
  } catch (error) {
    console.error(`ETL run ${etlRun.run_id} failed.`, error);
    etlRun.status = "failed";
    etlRun.end_time = new Date();
    etlRun.errors.push({ message: error.message, details: error.stack });
    await etlRun.save();

    summary.status = "failed";
    summary.errors.push(error.message);
  } finally {
    summary.endTime = new Date().toISOString();
    console.log("--- ETL RUN SUMMARY ---");
    console.log(JSON.stringify(summary, null, 2));
    console.log("-----------------------");
  }
};

export { runEtlProcess };
