import { Schema, model } from "mongoose";

const RawMarketDataSchema = new Schema(
  {
    data: {
      type: Schema.Types.Mixed,
      required: true,
    },
  },
  {
    timestamps: true,
  }
);

export default model("RawMarketData", RawMarketDataSchema);
