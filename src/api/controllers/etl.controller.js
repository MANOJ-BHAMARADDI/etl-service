import { runEtlProcess } from "../../services/etl.service.js";
import EtlRun from "../models/etlRun.model.js";
import EtlCheckpoint from "../models/etlCheckpoint.model.js";
import SchemaVersion from "../models/schemaVersion.model.js";

const triggerEtl = async (req, res) => {
  console.log("Received request to trigger ETL process.");
  runEtlProcess();
  res.status(202).json({
    message:
      "ETL process has been triggered successfully. It will run in the background.",
  });
};

const getRuns = async (req, res) => {
  try {
    const runs = await EtlRun.find().sort({ start_time: -1 });
    res.status(200).json(runs);
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error fetching ETL runs", error: error.message });
  }
};

const getRunById = async (req, res) => {
  try {
    const run = await EtlRun.findOne({ run_id: req.params.id }).lean();
    if (!run) {
      return res.status(404).json({ message: "ETL run not found" });
    }

    const checkpoints = await EtlCheckpoint.find({
      run_id: req.params.id,
    }).lean();
    const schemaVersion = await SchemaVersion.findOne({})
      .sort({ version: -1 })
      .lean();

    const response = {
      ...run,
      batches: checkpoints.map((c) => ({
        no: c.batch_no,
        offset: c.offset,
        status: c.status,
      })),
      failed_batches: checkpoints
        .filter((c) => c.status === "failed")
        .map((c) => c.batch_no),
      schema_version: schemaVersion
        ? {
            version: schemaVersion.version,
            confidence: schemaVersion.confidence,
            applied_mappings: schemaVersion.mappings,
          }
        : null,
    };

    res.status(200).json(response);
  } catch (error) {
    res
      .status(500)
      .json({
        message: "Error fetching ETL run details",
        error: error.message,
      });
  }
};

export { triggerEtl, getRuns, getRunById };
