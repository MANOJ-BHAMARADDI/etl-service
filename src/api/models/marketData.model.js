import { Schema, model } from "mongoose";

const MarketDataSchema = new Schema(
  {
    symbol: {
      type: String,
      required: true,
      uppercase: true,
      trim: true,
    },
    price_usd: {
      type: Number,
      required: true,
    },
    volume: {
      type: Number,
    },
    source: {
      // To track if it came from the API or CSV
      type: String,
      required: true,
      enum: ["api", "csv"],
    },
    // The timestamp from the source data
    timestamp: {
      type: Date,
      required: true,
    },
  },
  {
    timestamps: true,
  }
);

MarketDataSchema.index({ symbol: 1, timestamp: 1 }, { unique: true });

export default model("MarketData", MarketDataSchema);
