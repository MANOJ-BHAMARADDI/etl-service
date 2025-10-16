// src/api/models/schemaVersion.model.js

import { Schema, model } from "mongoose";

const SchemaVersionSchema = new Schema({
  source: {
    type: String,
    required: true,
  },
  version: {
    type: Number,
    required: true,
  },
  schema: {
    type: Schema.Types.Mixed,
    required: true,
  },
  mappings: {
    type: Schema.Types.Mixed,
  },
  timestamp: {
    type: Date,
    default: Date.now,
  },
});

SchemaVersionSchema.index({ source: 1, version: 1 }, { unique: true });

export default model("SchemaVersion", SchemaVersionSchema);
