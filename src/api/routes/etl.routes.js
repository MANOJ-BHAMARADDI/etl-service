import express from "express";
import {
  triggerEtl,
  getRuns,
  getRunById,
} from "../controllers/etl.controller.js";
import { protectWithToken } from "../middleware/auth.js";

const router = express.Router();

router.post("/refresh", protectWithToken, triggerEtl);
router.get("/runs", protectWithToken, getRuns);
router.get("/runs/:id", protectWithToken, getRunById);

export default router;
