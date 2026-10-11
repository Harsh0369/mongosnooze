import { MongoClient } from "mongodb";
import { MongoSnooze } from "../src/index.js";

async function main() {
  const uri = process.env.MONGODB_URI || "mongodb://localhost:27017/?replicaSet=rs0";
  const client = new MongoClient(uri);

  await client.connect();
  console.log("Connected to MongoDB");

  const db = client.db("mongosnooze_demo");

  const scheduler = new MongoSnooze({
    db,
    collectionPrefix: "myapp",
    leaseDurationMs: 30_000,
    recoveryIntervalMs: 30_000,
    concurrency: 5,
  });

  // 1. Define job handlers
  scheduler.define<{ email: string; subject: string }>("send-welcome-email", async (job) => {
    console.log(`[Job ${job.id}] Sending email to ${job.data.email}...`);
    // Simulated work
    await new Promise((r) => setTimeout(r, 1000));
    console.log(`[Job ${job.id}] Email sent!`);
    return { delivered: true, sentAt: new Date() };
  });

  // 2. Listen to lifecycle events
  scheduler.on("job:scheduled", (job) => console.log(`[Event] Job scheduled: ${job._id} (RunAt: ${job.runAt})`));
  scheduler.on("job:started", (job) => console.log(`[Event] Job started: ${job.id}`));
  scheduler.on("job:completed", (job, res) => console.log(`[Event] Job completed: ${job.id}`, res));
  scheduler.on("job:retrying", (job, err, nextRun) => console.log(`[Event] Retrying ${job.id} at ${nextRun}:`, err.message));
  scheduler.on("job:failed", (job, err) => console.error(`[Event] Job permanently failed: ${job.id}:`, err.message));

  // 3. Start scheduler (Initializes indexes & Change Stream)
  await scheduler.start();
  console.log("MongoSnooze worker started.");

  // 4. Schedule delayed jobs
  const jobId = await scheduler.schedule(
    "send-welcome-email",
    { email: "user@example.com", subject: "Welcome to MongoSnooze" },
    { delayMs: 10_000 } // Runs 10 seconds from now
  );
  console.log(`Scheduled job ID: ${jobId}`);

  // Graceful shutdown on termination signals
  const shutdown = async () => {
    console.log("\nShutting down MongoSnooze gracefully...");
    await scheduler.stop();
    await client.close();
    console.log("Shutdown complete.");
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch(console.error);
