import { Schema, model } from "mongoose";

const QuarantinedDataSchema = new Schema(
  {
    run_id: {
      type: String,
      required: true,
      index: true,
    },
    source: {
      type: String,
      required: true,
    },
    reason: {
      type: String,
      required: true,
      enum: ["low_confidence_schema", "validation_error", "other"],
    },
    payload: {
      type: Schema.Types.Mixed,
      required: true,
    },
    confidence_score: {
      type: Number,
    },
  },
  {
    timestamps: true,
  }
);

export default model("QuarantinedData", QuarantinedDataSchema);
