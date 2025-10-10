import { Schema, model } from "mongoose";
import { v4 as uuidv4 } from "uuid";

const EtlRunSchema = new Schema({
  run_id: {
    type: String,
    default: () => `run_${uuidv4()}`,
    unique: true,
  },
  start_time: {
    type: Date,
    default: Date.now,
  },
  end_time: {
    type: Date,
  },
  status: {
    type: String,
    enum: ["started", "completed", "failed"],
    default: "started",
  },
  rows_processed: {
    type: Number,
    default: 0,
  },
  errors: [
    {
      message: String,
      details: Schema.Types.Mixed,
    },
  ],
});

export default model("EtlRun", EtlRunSchema);
