import axios from "axios";
import { createReadStream } from "fs";
import { join } from "path";
import csv from "csv-parser";
import MarketData from "../api/models/marketData.model.js";
import EtlRun from "../api/models/etlRun.model.js";

/**
 * --- EXTRACT ---
 * Fetches data from the CoinCap public API.
 */
const fetchFromApi = async () => {
  try {
    // We'll use the free CoinCap API for cryptocurrency data
    const response = await get(
      "https://api.coincap.io/v2/assets?limit=10"
    );
    console.log("Successfully fetched data from API.");
    return response.data.data; // The assets are in the 'data' property
  } catch (error) {
    console.error("Error fetching data from API:", error.message);
    throw new Error("API fetch failed");
  }
};

/**
 * --- EXTRACT ---
 * Fetches data from the local CSV file.
 */
const fetchFromCsv = () => {
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
 * --- TRANSFORM ---
 * Normalizes data from both sources into our unified schema.
 */
const transformData = (apiData, csvData) => {
  // Transform API data
  const transformedApiData = apiData.map((item) => ({
    symbol: item.symbol,
    price_usd: parseFloat(item.priceUsd),
    volume: parseFloat(item.volumeUsd24Hr),
    source: "api",
    timestamp: new Date(item.timestamp || Date.now()), // Use provided timestamp or now
  }));

  // Transform CSV data
  const transformedCsvData = csvData.map((item) => ({
    symbol: item.ticker, // Map 'ticker' to 'symbol'
    price_usd: parseFloat(item.price_usd),
    volume: parseFloat(item.tx_volume),
    source: "csv",
    timestamp: new Date(item.time),
  }));

  console.log("Data transformed successfully.");
  return [...transformedApiData, ...transformedCsvData];
};

/**
 * --- LOAD ---
 * Loads the transformed data into the MongoDB database.
 * This function is idempotent.
 */
const loadData = async (data) => {
  if (!data || data.length === 0) {
    console.log("No data to load.");
    return 0;
  }

 
  const operations = data.map((record) => ({
    updateOne: {
      filter: { symbol: record.symbol, timestamp: record.timestamp },
      update: { $set: record },
      upsert: true, // If no document matches the filter, a new one is created
    },
  }));

  const result = await MarketData.bulkWrite(operations);
  const rowsProcessed = result.upsertedCount + result.modifiedCount;
  console.log(`Data loaded into DB. Rows processed: ${rowsProcessed}`);
  return rowsProcessed;
};

/**
 * --- ORCHESTRATOR ---
 * Main function to run the entire ETL process.
 */
const runEtlProcess = async () => {
  // 1. Log the start of the run
  const etlRun = new EtlRun();
  await etlRun.save();
  console.log(`Starting ETL run with ID: ${etlRun.run_id}`);

  try {
    // 2. EXTRACT data from all sources concurrently
    const [apiData, csvData] = await Promise.all([
      fetchFromApi(),
      fetchFromCsv(),
    ]);

    // 3. TRANSFORM data into a unified model
    const transformedData = transformData(apiData, csvData);

    // 4. LOAD data into the database
    const rowsProcessed = await loadData(transformedData);

    // 5. Update the run log with completion status
    etlRun.status = "completed";
    etlRun.end_time = new Date();
    etlRun.rows_processed = rowsProcessed;
    await etlRun.save();
    console.log(`ETL run ${etlRun.run_id} completed successfully.`);
    return {
      success: true,
      run_id: etlRun.run_id,
      rows_processed: rowsProcessed,
    };
  } catch (error) {
    // 6. If any step fails, update the run log with an error status
    console.error(`ETL run ${etlRun.run_id} failed.`, error);
    etlRun.status = "failed";
    etlRun.end_time = new Date();
    etlRun.errors.push({ message: error.message, details: error.stack });
    await etlRun.save();
    return { success: false, run_id: etlRun.run_id, error: error.message };
  }
};

export default { runEtlProcess };
