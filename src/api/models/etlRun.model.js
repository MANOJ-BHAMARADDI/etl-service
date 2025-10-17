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
    enum: ["started", "completed", "failed", "completed_with_warnings"],
    default: "started",
  },
  stats: {
    extracted: { type: Number, default: 0 },
    loaded: { type: Number, default: 0 },
    duplicates: { type: Number, default: 0 },
    quarantined: { type: Number, default: 0 },
    errors: { type: Number, default: 0 },
    throttle_events: { type: Number, default: 0 },
  },
  resume_from: {
    batch_no: Number,
    offset: Number,
  },
  errors: [
    {
      message: String,
      details: Schema.Types.Mixed,
      timestamp: { type: Date, default: Date.now },
    },
  ],
});

export default model("EtlRun", EtlRunSchema);
