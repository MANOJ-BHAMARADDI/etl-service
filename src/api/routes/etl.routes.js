import express from "express";
import { triggerEtl } from "../controllers/etl.controller.js";
import { protectWithToken } from "../middleware/auth.js";

const router = express.Router();

router.post("/refresh", protectWithToken, triggerEtl);

export default router;
