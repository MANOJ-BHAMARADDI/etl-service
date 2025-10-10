import MarketData from "../models/marketData.model.js";
import EtlRun from "../models/etlRun.model.js";
import mongoose from "mongoose";

/**
 * Controller to fetch market data with filtering, sorting, and pagination.
 */
const getData = async (req, res) => {
  try {
    // --- Filtering ---
    const filter = {};
    if (req.query.symbol) {
      filter.symbol = req.query.symbol.toUpperCase();
    }
    if (req.query.startDate || req.query.endDate) {
      filter.timestamp = {};
      if (req.query.startDate) {
        filter.timestamp.$gte = new Date(req.query.startDate);
      }
      if (req.query.endDate) {
        filter.timestamp.$lte = new Date(req.query.endDate);
      }
    }

    // --- Sorting ---
    const sort = {};
    if (req.query.sortBy) {
      const [field, order] = req.query.sortBy.split(":");
      sort[field] = order === "desc" ? -1 : 1;
    } else {
      sort.timestamp = -1; // Default sort by most recent
    }

    // --- Pagination ---
    const page = parseInt(req.query.page, 10) || 1;
    const limit = parseInt(req.query.limit, 10) || 10;
    const skip = (page - 1) * limit;

    const data = await MarketData.find(filter)
      .sort(sort)
      .skip(skip)
      .limit(limit);

    const totalRecords = await MarketData.countDocuments(filter);

    res.status(200).json({
      totalRecords,
      currentPage: page,
      totalPages: Math.ceil(totalRecords / limit),
      data,
    });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error fetching data", error: error.message });
  }
};

/**
 * Controller to fetch ETL statistics.
 */
const getStats = async (req, res) => {
  try {
    const recordCount = await MarketData.countDocuments();

    const lastRun = await EtlRun.findOne().sort({ start_time: -1 });

    // Use MongoDB Aggregation Pipeline to calculate average latency efficiently
    const avgLatencyResult = await EtlRun.aggregate([
      {
        $match: {
          status: "completed",
          start_time: { $ne: null },
          end_time: { $ne: null },
        },
      },
      { $project: { latency: { $subtract: ["$end_time", "$start_time"] } } },
      { $group: { _id: null, avgLatency: { $avg: "$latency" } } },
    ]);

    const averageLatency =
      avgLatencyResult.length > 0 ? avgLatencyResult[0].avgLatency : 0;

    res.status(200).json({
      record_count: recordCount,
      last_run_time: lastRun ? lastRun.start_time : null,
      last_run_status: lastRun ? lastRun.status : "N/A",
      average_etl_latency_ms: averageLatency,
    });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error fetching stats", error: error.message });
  }
};

export { getData, getStats };

