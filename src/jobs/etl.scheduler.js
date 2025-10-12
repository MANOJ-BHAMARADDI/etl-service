import cron from "node-cron";
import { runEtlProcess } from "../services/etl.service.js";

// This schedule runs the job at 0 minutes past every hour (e.g., 1:00, 2:00, etc.)
cron.schedule("0 * * * *", () => {
  console.log("⏰ Running scheduled hourly ETL process...");
  runEtlProcess();
});

console.log("ETL Scheduler has been initialized.");
