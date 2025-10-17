import "dotenv/config";
import express from "express";
import connection from "mongoose";
import connectDB from "./config/database.js";
import etlRoutes from "./api/routes/etl.routes.js";
import dataRoutes from "./api/routes/data.routes.js";
import "./jobs/etl.scheduler.js";
import client from "prom-client";

// Connect to MongoDB
connectDB();

const app = express();

// Middleware to parse JSON bodies
app.use(express.json());

// --- Mount Routers ---
// All etl routes will be prefixed with /api
app.use("/api", etlRoutes);
app.use("/api", dataRoutes);

// --- Application Routes ---

// Add a root route to provide a landing page
app.get("/", (req, res) => {
  res.status(200).send(`
    <div style="font-family: sans-serif; text-align: center; padding: 40px;">
      <h1>ETL Service is Up and Running!</h1>
      <p>Welcome to the Market Data ETL Service.</p>
      <p>Here are some available endpoints:</p>
      <ul style="list-style: none; padding: 0;">
        <li style="margin: 10px 0;"><a href="/health" style="text-decoration: none; color: #007BFF;">/health</a> - Check the application's health.</li>
        <li style="margin: 10px 0;"><a href="/metrics" style="text-decoration: none; color: #007BFF;">/metrics</a> - View Prometheus metrics.</li>
      </ul>
      <p>API endpoints are available under the <code>/api</code> path.</p>
    </div>
  `);
});

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

app.get("/metrics", async (req, res) => {
  res.set("Content-Type", client.register.contentType);
  res.end(await client.register.metrics());
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Server is listening on http://localhost:${PORT}`);
});
