import MarketData from "../models/marketData.model.js";
import EtlRun from "../models/etlRun.model.js";
import client from "prom-client";

/**
 * Controller to fetch market data with cursor-based pagination, filtering, and sorting.
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

    // --- Cursor-based Pagination ---
    const limit = parseInt(req.query.limit, 10) || 10;
    if (req.query.cursor) {
      filter._id = { $lt: req.query.cursor }; // fetch records older than the cursor
    }

    const data = await MarketData.find(filter)
      .sort({ _id: -1 }) // always sort by _id for consistent pagination
      .limit(limit);

    const nextCursor = data.length === limit ? data[data.length - 1]._id : null;

    res.status(200).json({
      data,
      nextCursor,
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

    // FIX: Modified the aggregation to handle cases where no completed runs exist
    const avgLatencyResult = await EtlRun.aggregate([
      {
        $match: {
          status: "completed",
          start_time: { $ne: null },
          end_time: { $ne: null },
        },
      },
      {
        $group: {
          _id: null,
          avgLatency: { $avg: { $subtract: ["$end_time", "$start_time"] } },
        },
      },
    ]);

    const averageLatency =
      avgLatencyResult.length > 0 ? avgLatencyResult[0].avgLatency : 0;

    const metrics = await client.register.getMetricsAsJSON();

    const throttleMetric = metrics.find(
      (m) => m.name === "throttle_events_total"
    );
    const throttleCount = throttleMetric?.values?.[0]?.value || 0;

    const errorMetric = metrics.find((m) => m.name === "etl_errors_total");
    const errorCount = errorMetric?.values?.[0]?.value || 0;

    res.status(200).json({
      record_count: recordCount,
      last_run_time: lastRun ? lastRun.start_time : null,
      last_run_status: lastRun ? lastRun.status : "N/A",
      average_etl_latency_ms: averageLatency,
      throttle_events_total: throttleCount,
      etl_errors_total: errorCount,
    });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error fetching stats", error: error.message });
  }
};

export { getData, getStats };
