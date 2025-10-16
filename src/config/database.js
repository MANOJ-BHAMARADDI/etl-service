import { connect } from "mongoose";

const connectDB = async () => {
  // Use provided MONGO_URI or fall back to a sensible localhost value for local dev
  const fallback = "mongodb://localhost:27017/market_data";
  const mongoUri = process.env.MONGO_URI || fallback;

  try {
    console.log(`Attempting MongoDB connection using: ${mongoUri}`);
    await connect(mongoUri, { connectTimeoutMS: 10000 });
    console.log("MongoDB connected successfully! 🍃");
  } catch (error) {
    console.error("MongoDB connection failed:", error.message);

    // If user configured a Docker 'mongo' hostname but is running the app locally,
    // try a sensible localhost fallback before giving up.
    const isDockerHost = String(mongoUri).includes("mongo");
    if (isDockerHost && mongoUri !== fallback) {
      console.warn(
        "Configured MONGO_URI looks like it targets the Docker hostname 'mongo'.\n" +
          "Attempting to connect to a localhost fallback instead..."
      );

      try {
        console.log(
          `Attempting MongoDB connection using fallback: ${fallback}`
        );
        await connect(fallback, { connectTimeoutMS: 10000 });
        console.log("MongoDB connected successfully using fallback! 🍃");
        return;
      } catch (err2) {
        console.error("Fallback connection also failed:", err2.message);
      }
    }

    console.error(
      "Possible causes: the 'mongo' hostname is only available inside Docker Compose, or MongoDB is not running locally."
    );
    console.error("If you use Docker, run: docker-compose up -d --build");
    console.error(
      "Or set MONGO_URI in your .env to a reachable MongoDB instance."
    );
    // Exit process with failure so upstream callers know DB is unavailable
    process.exit(1);
  }
};

export default connectDB;
