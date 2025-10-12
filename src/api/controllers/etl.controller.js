import { runEtlProcess } from "../../services/etl.service.js";

const triggerEtl = async (req, res) => {
  console.log("Received request to trigger ETL process.");

  runEtlProcess();

  res.status(202).json({
    message:
      "ETL process has been triggered successfully. It will run in the background.",
  });
};

export { triggerEtl };
