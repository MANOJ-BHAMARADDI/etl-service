import { createWriteStream } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const filePath = join(__dirname, "market_data_source.csv");

const driftedData = [
  "ticker,price_in_usd,tx_volume,time_int",
  `BITCOIN,69500.50,510000,${Math.floor(Date.now() / 1000)}`,
  `ETHEREUM,3600.75,260000,${Math.floor(Date.now() / 1000)}`,
];

const writer = createWriteStream(filePath);
writer.write(driftedData.join("\n"));
writer.end();
console.log("Successfully seeded drifted and type-changed data.");
