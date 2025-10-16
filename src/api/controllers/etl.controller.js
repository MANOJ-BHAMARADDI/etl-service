import { runEtlProcess } from "../../services/etl.service.js";
import EtlRun from "../models/etlRun.model.js";
import EtlCheckpoint from "../models/etlCheckpoint.model.js";

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
    const run = await EtlRun.findOne({ run_id: req.params.id });
    if (!run) {
      return res.status(404).json({ message: "ETL run not found" });
    }
    const checkpoints = await EtlCheckpoint.find({ run_id: req.params.id });
    res.status(200).json({ run, checkpoints });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error fetching ETL run", error: error.message });
  }
};

export { triggerEtl, getRuns, getRunById };
