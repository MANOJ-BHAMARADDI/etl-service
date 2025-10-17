import mongoose from "mongoose";

const connectDB = async () => {
  // CRITICAL FIX: The MONGO_URI must come from the environment.
  // We remove the fallback to 'localhost' which was causing the error.
  const mongoUri = process.env.MONGO_URI;

  if (!mongoUri) {
    console.error("FATAL ERROR: MONGO_URI environment variable is not set.");
    process.exit(1); // Exit if the database URI is not provided.
  }

  const connectWithRetry = async () => {
    try {
      console.log(`Attempting MongoDB connection using: ${mongoUri}`);
      await mongoose.connect(mongoUri, {
        serverSelectionTimeoutMS: 30000, // Increase timeout to 30 seconds
      });
    } catch (err) {
      console.error(
        "MongoDB connection unsuccessful, will retry in 5 seconds.",
        err.name
      );
      setTimeout(connectWithRetry, 5000);
    }
  };

  mongoose.connection.on("connected", () => {
    console.log("MongoDB connected successfully! 🍃");
  });

  mongoose.connection.on("error", (err) => {
    console.error(`MongoDB connection error: ${err}`);
  });

  mongoose.connection.on("disconnected", () => {
    console.warn("MongoDB disconnected. The driver will attempt to reconnect.");
  });

  await connectWithRetry();
};

export default connectDB;
