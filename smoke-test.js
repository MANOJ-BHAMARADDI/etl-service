#!/usr/bin/env node

/**
 * Cross-platform Smoke Test for ETL Service
 * Replaces the bash script with Node.js, works on Windows/macOS/Linux
 */

import fetch from "node-fetch";

const BASE_URL = "http://localhost:3000";
const TOKEN = process.env.SECRET_REFRESH_TOKEN || "mySuperSecretToken123";

async function triggerETL() {
  console.log("Triggering ETL run...");
  const res = await fetch(`${BASE_URL}/api/refresh`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    },
  });
  const data = await res.json();
  if (!res.ok) {
    console.error("ETL Trigger Failed:", data);
    process.exit(1);
  }
  console.log("ETL triggered successfully:", data);
}

async function checkMetrics() {
  console.log("Waiting 15 seconds for ETL to process...");
  await new Promise((r) => setTimeout(r, 15000));

  console.log("Checking /metrics endpoint...");
  const res = await fetch(`${BASE_URL}/metrics`);
  const metrics = await res.text();

  const match = metrics.match(/etl_rows_processed_total\s+(\d+)/);
  if (!match || Number(match[1]) <= 0) {
    console.error("FAIL: etl_rows_processed_total was not greater than 0.");
    process.exit(1);
  } else {
    console.log(
      `SUCCESS: Found etl_rows_processed_total with value ${match[1]}.`
    );
  }
}

async function checkRuns() {
  console.log("Checking /runs endpoint...");
  const res = await fetch(`${BASE_URL}/api/runs`, {
    headers: { Authorization: `Bearer ${TOKEN}` },
  });
  const runs = await res.json();

  if (Array.isArray(runs) && runs.length > 0) {
    console.log(`SUCCESS: Found ${runs.length} ETL run(s).`);
  } else {
    console.error("FAIL: No ETL runs found.");
    process.exit(1);
  }
}

async function main() {
  await triggerETL();
  await checkMetrics();
  await checkRuns();
  console.log("✅ All smoke tests passed!");
}

main();
