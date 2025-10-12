import axios from "axios";
import { createReadStream } from "fs";
import { join } from "path";
import csv from "csv-parser";
import MarketData from "../api/models/marketData.model.js";
import EtlRun from "../api/models/etlRun.model.js";
import { fileURLToPath } from "url";
import { dirname } from "path";
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * --- EXTRACT ---
 * Fetches data from the CoinCap public API.
 */
// A simple helper function to wait for a specified time
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const fetchFromApi = async (retries = 3, waitTime = 5000) => {
  try {
    const response = await axios.get("https://api.coincap.io/v2/assets?limit=10");
    console.log("Successfully fetched data from API.");
    return response.data.data;
  } catch (error) {
    // If we have retries left and the error is a rate limit or server error
    if (retries > 0 && (error.response?.status === 429 || error.response?.status >= 500)) {
      console.warn(`API fetch failed with status ${error.response.status}. Retrying in ${waitTime / 1000}s... (${retries} retries left)`);
      await delay(waitTime);
      return fetchFromApi(retries - 1, waitTime * 2); // Exponential backoff
    }
    console.error("Error fetching data from API:", error.message);
    throw new Error("API fetch failed after multiple retries");
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
  // (No changes to transformedApiData)
  const transformedApiData = apiData.map(/* ... */);

  // Make the CSV transformation more robust
  const transformedCsvData = csvData.map((item) => {
    // Check for the original column name first, then the drifted name
    const price = item.price_usd || item.usd_price;

    if (!item.price_usd && item.usd_price) {
      console.warn(
        `[SCHEMA DRIFT] Detected column 'usd_price' instead of 'price_usd' in CSV.`
      );
    }

    return {
      symbol: item.ticker,
      price_usd: parseFloat(price), // Use the resilient price variable
      volume: parseFloat(item.tx_volume),
      source: "csv",
      timestamp: new Date(item.time),
    };
  });

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

export { runEtlProcess };
