import { Schema, model } from "mongoose";

const EtlCheckpointSchema = new Schema({
  run_id: {
    type: String,
    required: true,
  },
  source: {
    type: String,
    required: true,
  },
  batch_no: {
    type: Number,
    required: true,
  },
  offset: {
    type: Number,
    required: true,
  },
  status: {
    type: String,
    enum: ["pending", "completed", "failed"],
    default: "pending",
  },
});

EtlCheckpointSchema.index(
  { run_id: 1, source: 1, batch_no: 1 },
  { unique: true }
);

export default model("EtlCheckpoint", EtlCheckpointSchema);
