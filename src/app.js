import "dotenv/config";
import express from "express";
import connection from "mongoose";
import connectDB from "./config/database.js";
import etlRoutes from "./api/routes/etl.routes.js";
import dataRoutes from "./api/routes/data.routes.js";
import "./jobs/etl.scheduler.js";

// Connect to MongoDB
connectDB();

const app = express();

// Middleware to parse JSON bodies
app.use(express.json());

// --- Mount Routers ---
// All etl routes will be prefixed with /api
app.use("/api", etlRoutes);
app.use("/api", dataRoutes);

app.get("/health", (req, res) => {
  // A more detailed health check
  const healthcheck = {
    uptime: process.uptime(),
    message: "OK",
    db_state: connection.readyState === 1 ? "connected" : "disconnected",
    timestamp: Date.now(),
  };
  res.status(200).json(healthcheck);
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Server is listening on http://localhost:${PORT}`);
});
