import { createWriteStream } from "fs";
import { join } from "path";

const filePath = join(__dirname, "market_data_source.csv");

const driftedData = [
  "ticker,usd_price,tx_volume,time",
  "BITCOIN,68500.50,500000,2025-10-10T12:00:00Z",
  "ETHEREUM,3500.75,250000,2025-10-10T12:00:00Z",
];

const writer = createWriteStream(filePath);
writer.write(driftedData.join("\n"));
writer.end();

console.log("Successfully seeded drifted data to market_data_source.csv");
