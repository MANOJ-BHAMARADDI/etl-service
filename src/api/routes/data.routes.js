import express from "express";
import { getData, getStats } from "../controllers/data.controller.js";

const router = express.Router();

router.get("/data", getData);
router.get("/stats", getStats);

export default router;
