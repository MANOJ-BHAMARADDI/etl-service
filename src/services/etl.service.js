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
import { findBestMatch } from "string-similarity";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const CACHE = {
  coingecko: null,
  blockchain: null,
};

// --- Rate Limiter and Caching ---
// Prometheus Metrics
const etlRowsProcessed = new client.Counter({
  name: "etl_rows_processed_total",
  help: "Total number of rows processed",
});
const etlErrors = new client.Counter({
  name: "etl_errors_total",
  help: "Total number of ETL errors",
  labelNames: ["type"],
});
const etlLatency = new client.Histogram({
  name: "etl_latency_seconds",
  help: "ETL run latency in seconds",
  buckets: [1, 5, 10, 30, 60],
});
const throttleEvents = new client.Counter({
  name: "throttle_events_total",
  help: "Total number of throttle events",
  labelNames: ["source"],
});

const quotas = {
  coingecko: 10,
  blockchain: 3,
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function rateLimiter(source, maxRequestsPerMinute) {
  const bucket = rateController(
    source,
    maxRequestsPerMinute,
    maxRequestsPerMinute,
    60000
  );
  while (!(await bucket.take())) {
    console.warn(`[RATE LIMIT] ${source} limit reached. Waiting...`);
    throttleEvents.inc({ source });
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
    let currentRow = 0;

    createReadStream(filePath)
      .pipe(csv())
      // FIX: Implemented the offset logic to skip already processed rows
      .on("data", (data) => {
        if (currentRow >= offset) {
          results.push(data);
        }
        currentRow++;
      })
      .on("end", () => {
        console.log(
          `Successfully processed data from CSV, skipping ${offset} rows.`
        );
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
const transformData = async (apiDataA, csvData, apiDataC) => {
  const canonicalSchema = ["ticker", "price_usd", "tx_volume", "time"];
  const csvHeaders = Object.keys(csvData[0] || {});
  const { bestMatch, bestMatchIndex } = findBestMatch(
    canonicalSchema.join(","),
    [csvHeaders.join(",")]
  );

  const mappings = {};
  if (bestMatch.rating < 0.8) {
    console.warn(
      `[SCHEMA DRIFT] Low confidence score (${bestMatch.rating}) for schema match. Skipping CSV data.`
    );
    etlErrors.inc({ type: "schema_drift_low_confidence" });
    csvData = [];
  } else if (bestMatch.rating < 1.0) {
    console.log(
      `[SCHEMA DRIFT] Detected schema drift with confidence ${bestMatch.rating}. Applying mappings.`
    );
    const driftedHeaders = csvHeaders;
    canonicalSchema.forEach((header) => {
      const { bestMatch: bestHeaderMatch } = findBestMatch(
        header,
        driftedHeaders
      );
      if (bestHeaderMatch.target !== header && bestHeaderMatch.rating >= 0.8) {
        mappings[bestHeaderMatch.target] = header;
      }
    });
    await new SchemaVersion({
      source: "csv",
      version: Date.now(),
      schema: driftedHeaders,
      mappings,
    }).save();
  }

  // --- TRANSFORMING COINGECKO DATA (Source A) ---
  const transformedApiDataA = apiDataA.map((item) => ({
    symbol: item.symbol ? item.symbol.toUpperCase() : item.id || null,
    price_usd:
      item.current_price != null ? parseFloat(item.current_price) : null,
    volume: item.total_volume != null ? parseFloat(item.total_volume) : null,
    source: "coingecko",
    timestamp: item.last_updated ? new Date(item.last_updated) : new Date(),
    metadata: {
      id: item.id || null,
      name: item.name || null,
      market_cap: item.market_cap != null ? parseFloat(item.market_cap) : null,
      market_cap_rank:
        item.market_cap_rank != null ? item.market_cap_rank : null,
      raw: item,
    },
  }));

  // --- TRANSFORMING CSV DATA (Source B) ---
  const transformedCsvData = csvData.map((item) => {
    const mappedItem = {};
    for (const key in item) {
      mappedItem[mappings[key] || key] = item[key];
    }
    return {
      symbol: mappedItem.ticker,
      price_usd: parseFloat(mappedItem.price_usd),
      volume: parseFloat(mappedItem.tx_volume),
      source: "csv",
      timestamp: new Date(mappedItem.time),
    };
  });

  const transformedApiDataC = apiDataC.map((item) => {
    const raw = item;

    const parseNumber = (v) => {
      if (v == null || v === "") return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };

    const toDate = (t) => {
      if (!t) return new Date();
      if (typeof t === "number") {
        // if likely seconds -> convert to ms, otherwise treat as ms
        return t < 1e11 ? new Date(t * 1000) : new Date(t);
      }
      const parsed = Date.parse(t);
      return isNaN(parsed) ? new Date() : new Date(parsed);
    };

    const rawSymbol =
      item.symbol ||
      item.pair ||
      item.base_currency ||
      item.base ||
      item.baseCurrency ||
      item.asset ||
      item.currency ||
      item.ticker ||
      item.id ||
      item.name ||
      null;

    const normalizeSymbol = (s) => {
      if (!s) return null;
      const str = String(s);
      // common formats: "BTC-USD", "BTC/USD", "BTCUSD"
      const parts = str.split(/[-_/]/);
      return parts[0].toUpperCase();
    };

    const price =
      parseNumber(item.last_trade_price) ??
      parseNumber(item.last) ??
      parseNumber(item.price) ??
      parseNumber(item.close_price) ??
      parseNumber(item.last_price) ??
      parseNumber(item.rate) ??
      parseNumber(item.ask) ??
      parseNumber(item.bid) ??
      parseNumber(item.price_usd) ??
      null;

    const volume =
      parseNumber(item.volume) ??
      parseNumber(item.volume_24h) ??
      parseNumber(item.base_volume) ??
      parseNumber(item.quote_volume) ??
      parseNumber(item.trade_volume) ??
      parseNumber(item.volume24h) ??
      parseNumber(item.total_volume) ??
      null;

    const timestamp = toDate(
      item.timestamp ||
        item.updated_at ||
        item.last_trade_time ||
        item.last_update ||
        item.time ||
        item.date ||
        null
    );

    return {
      symbol: normalizeSymbol(rawSymbol),
      price_usd: price,
      volume: volume,
      source: "blockchain",
      timestamp,
      metadata: {
        raw,
      },
    };
  });

  const allData = [
    ...transformedApiDataA,
    ...transformedCsvData,
    ...transformedApiDataC,
  ];
  // --- VALIDATION AND TYPE RECONCILIATION ---
  const validatedData = allData.filter((item) => {
    return (
      item.symbol &&
      item.price_usd != null &&
      item.timestamp instanceof Date &&
      !isNaN(item.timestamp)
    );
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

  const endTimer = etlLatency.startTimer();

  try {
    const lastUnfinishedRun = await EtlRun.findOne({
      status: { $ne: "completed" },
      run_id: { $ne: etlRun.run_id },
    }).sort({
      start_time: -1,
    });

    let offset = 0;
    if (lastUnfinishedRun) {
      const lastCheckpoint = await EtlCheckpoint.findOne({
        run_id: lastUnfinishedRun.run_id,
        status: "completed",
      }).sort({ batch_no: -1 });
      if (lastCheckpoint) {
        console.log(
          `Resuming from last successful checkpoint of run ${lastUnfinishedRun.run_id}: batch ${lastCheckpoint.batch_no}`
        );
        offset = lastCheckpoint.offset;
      }
    }

    const [apiDataA, csvData, apiDataC] = await Promise.all([
      fetchFromApiSourceA(),
      fetchFromCsvSourceB(offset),
      fetchFromApiSourceC(),
    ]);

    const rawData = [...apiDataA, ...csvData, ...apiDataC];
    const transformedData = await transformData(apiDataA, csvData, apiDataC);

    // **CRITICAL FIX**: Wrap the loadData call in its own try/catch
    // to prevent crashes on database errors.
    try {
      const { normalized, raw } = await loadData(
        transformedData,
        rawData,
        etlRun.run_id,
        "all_sources",
        1,
        transformedData.length
      );
      etlRun.status = "completed";
      etlRun.end_time = new Date();
      etlRun.stats.loaded = normalized; // Use stats.loaded
      etlRowsProcessed.inc(normalized);
    } catch (dbError) {
      console.error(
        `Database operation failed for run ${etlRun.run_id}.`,
        dbError
      );
      etlRun.status = "failed";
      etlRun.errors.push({
        message: "Database load error: " + dbError.message,
        details: dbError.stack,
      });
      etlErrors.inc({ type: "database_failure" });
    }

    await etlRun.save();
    console.log(
      `ETL run ${etlRun.run_id} finished with status: ${etlRun.status}.`
    );
  } catch (error) {
    console.error(
      `A critical error occurred during ETL run ${etlRun.run_id}.`,
      error
    );
    etlRun.status = "failed";
    etlRun.end_time = new Date();
    etlRun.errors.push({ message: error.message, details: error.stack });
    await etlRun.save();
    etlErrors.inc({ type: "process_failure" });
  } finally {
    endTimer();
  }
};

export { runEtlProcess };
