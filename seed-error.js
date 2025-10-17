import { createWriteStream } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const filePath = join(__dirname, "market_data_source.csv");

const badData = [
  "column_a,column_b,column_c,column_d", // Very different headers
  `data1,100,2000,${Math.floor(Date.now() / 1000)}`,
];

const writer = createWriteStream(filePath);
writer.write(badData.join("\n"));
writer.end();
console.log("Successfully seeded CSV with bad headers to trigger an error.");
