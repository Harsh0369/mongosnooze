import { describe, it, expect, vi, beforeEach } from "vitest";
import { MongoSnooze } from "../src/snooze.js";
import { createMockDb, MockCollection } from "./mockDb.js";
import { MongoSnoozeError } from "../src/errors.js";

describe("MongoSnooze", () => {
  let mockDb: any;

  beforeEach(() => {
    mockDb = createMockDb();
  });

  describe("Initialization & Validation", () => {
    it("should throw if db instance is not provided", () => {
      expect(() => new MongoSnooze({} as any)).toThrow(MongoSnoozeError);
    });

    it("should instantiate with default settings", () => {
      const snooze = new MongoSnooze({ db: mockDb });
      expect(snooze.workerId).toBeDefined();
    });

    it("should initialize indexes on start/initialize", async () => {
      const snooze = new MongoSnooze({ db: mockDb, collectionPrefix: "test" });
      await snooze.initialize();

      const wakeups = mockDb.collection("test_wakeups") as MockCollection;
      const jobs = mockDb.collection("test_jobs") as MockCollection;

      expect(wakeups.indexes).toHaveLength(1);
      expect(wakeups.indexes[0].options.expireAfterSeconds).toBe(0);
      expect(jobs.indexes).toHaveLength(3);
    });
  });

  describe("Job Scheduling & Management", () => {
    it("should schedule a delayed job and store both durable job and wakeup doc", async () => {
      const snooze = new MongoSnooze({ db: mockDb });
      const runAt = new Date(Date.now() + 60_000);

      const jobId = await snooze.schedule("send-email", { to: "user@test.com" }, { runAt });
      expect(jobId).toBeDefined();

      const job = await snooze.getJob(jobId);
      expect(job).not.toBeNull();
      expect(job?._id).toBe(jobId);
      expect(job?.name).toBe("send-email");
      expect(job?.data).toEqual({ to: "user@test.com" });
      expect(job?.status).toBe("pending");
      expect(job?.attempts).toBe(0);

      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      const wakeupDoc = await wakeups.findOne({ _id: jobId });
      expect(wakeupDoc).not.toBeNull();
      expect(new Date(wakeupDoc.expireAt).getTime()).toBe(runAt.getTime());
    });

    it("should cancel a scheduled job", async () => {
      const snooze = new MongoSnooze({ db: mockDb });
      const jobId = await snooze.schedule("reminder", { msg: "Hello" }, { delayMs: 10_000 });

      const cancelled = await snooze.cancel(jobId);
      expect(cancelled).toBe(true);

      const job = await snooze.getJob(jobId);
      expect(job?.status).toBe("cancelled");

      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      const wakeupDoc = await wakeups.findOne({ _id: jobId });
      expect(wakeupDoc).toBeNull();
    });

    it("should reschedule a job", async () => {
      const snooze = new MongoSnooze({ db: mockDb });
      const initialRunAt = new Date(Date.now() + 5_000);
      const newRunAt = new Date(Date.now() + 60_000);

      const jobId = await snooze.schedule("reminder", {}, { runAt: initialRunAt });
      const rescheduled = await snooze.reschedule(jobId, newRunAt);
      expect(rescheduled).toBe(true);

      const job = await snooze.getJob(jobId);
      expect(new Date(job!.runAt).getTime()).toBe(newRunAt.getTime());

      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      const wakeupDoc = await wakeups.findOne({ _id: jobId });
      expect(new Date(wakeupDoc.expireAt).getTime()).toBe(newRunAt.getTime());
    });
  });

  describe("Execution & Lifecycle", () => {
    it("should execute a handler when a wakeup triggers", async () => {
      const snooze = new MongoSnooze({ db: mockDb });
      const handlerFn = vi.fn().mockResolvedValue("email_sent_ok");

      snooze.define("send-email", handlerFn);
      await snooze.start();

      const runAt = new Date(Date.now() - 1000); // already due
      const jobId = await snooze.schedule("send-email", { email: "alice@test.com" }, { runAt });

      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      // Simulate MongoDB TTL deletion
      await wakeups.deleteOne({ _id: jobId });

      // Wait a tick for async processing
      await new Promise((r) => setTimeout(r, 50));

      expect(handlerFn).toHaveBeenCalledTimes(1);
      expect(handlerFn.mock.calls[0][0].id).toBe(jobId);
      expect(handlerFn.mock.calls[0][0].data).toEqual({ email: "alice@test.com" });

      const job = await snooze.getJob(jobId);
      expect(job?.status).toBe("completed");
      expect(job?.result).toBe("email_sent_ok");

      await snooze.stop();
    });

    it("should retry with exponential backoff on handler failure", async () => {
      const snooze = new MongoSnooze({
        db: mockDb,
        defaultMaxAttempts: 3,
        defaultBackoffMs: 1000,
        defaultBackoffFactor: 2,
      });

      const failingHandler = vi.fn().mockRejectedValue(new Error("Network timeout"));
      snooze.define("failing-job", failingHandler);
      await snooze.start();

      const runAt = new Date(Date.now() - 1000);
      const jobId = await snooze.schedule("failing-job", {}, { runAt });

      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      await wakeups.deleteOne({ _id: jobId });

      await new Promise((r) => setTimeout(r, 50));

      expect(failingHandler).toHaveBeenCalledTimes(1);

      const job = await snooze.getJob(jobId);
      expect(job?.status).toBe("pending"); // Reset to pending for retry
      expect(job?.attempts).toBe(1);
      expect(job?.error?.message).toBe("Network timeout");

      // Verify new wakeup document was set with backoff
      const wakeupDoc = await wakeups.findOne({ _id: jobId });
      expect(wakeupDoc).not.toBeNull();
      expect(new Date(wakeupDoc.expireAt).getTime()).toBeGreaterThan(Date.now());

      await snooze.stop();
    });

    it("should mark job as permanently failed after exhausting max attempts", async () => {
      const snooze = new MongoSnooze({
        db: mockDb,
        defaultMaxAttempts: 1, // Only 1 attempt allowed
      });

      snooze.define("fatal-job", () => {
        throw new Error("Fatal DB crash");
      });
      await snooze.start();

      const runAt = new Date(Date.now() - 1000);
      const jobId = await snooze.schedule("fatal-job", {}, { runAt });

      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      await wakeups.deleteOne({ _id: jobId });

      await new Promise((r) => setTimeout(r, 50));

      const job = await snooze.getJob(jobId);
      expect(job?.status).toBe("failed");
      expect(job?.attempts).toBe(1);
      expect(job?.error?.message).toBe("Fatal DB crash");

      await snooze.stop();
    });
  });

  describe("Safety Recovery Sweeper", () => {
    it("should recover pending due jobs that missed change stream events", async () => {
      const snooze = new MongoSnooze({ db: mockDb });
      const handlerFn = vi.fn().mockResolvedValue("recovered_ok");
      snooze.define("missed-job", handlerFn);

      // Directly insert a pending job that was scheduled while worker was offline
      const jobs = mockDb.collection("mongosnooze_jobs") as MockCollection;
      const dueTime = new Date(Date.now() - 10_000);
      await jobs.insertOne({
        _id: "missed_1",
        name: "missed-job",
        data: {},
        status: "pending",
        runAt: dueTime,
        attempts: 0,
        maxAttempts: 3,
        backoffMs: 5000,
        backoffFactor: 2,
        lockedBy: null,
        lockedAt: null,
        leaseExpiresAt: null,
        createdAt: dueTime,
        updatedAt: dueTime,
      });

      // Run recovery sweep
      const stats = await snooze.runRecoverySweep();
      expect(stats.recoveredPending).toBe(1);

      await new Promise((r) => setTimeout(r, 50));
      expect(handlerFn).toHaveBeenCalledTimes(1);

      const job = await snooze.getJob("missed_1");
      expect(job?.status).toBe("completed");
    });

    it("should recover orphaned jobs whose worker lease expired", async () => {
      const snooze = new MongoSnooze({ db: mockDb });
      const handlerFn = vi.fn().mockResolvedValue("lease_recovered_ok");
      snooze.define("crashed-worker-job", handlerFn);

      // Directly insert a job locked by a worker that crashed 2 minutes ago
      const jobs = mockDb.collection("mongosnooze_jobs") as MockCollection;
      const expiredLease = new Date(Date.now() - 60_000);
      await jobs.insertOne({
        _id: "crashed_1",
        name: "crashed-worker-job",
        data: {},
        status: "running",
        runAt: new Date(Date.now() - 120_000),
        attempts: 1,
        maxAttempts: 3,
        backoffMs: 5000,
        backoffFactor: 2,
        lockedBy: "dead_worker_999",
        lockedAt: new Date(Date.now() - 90_000),
        leaseExpiresAt: expiredLease,
        createdAt: new Date(Date.now() - 120_000),
        updatedAt: new Date(Date.now() - 90_000),
      });

      const stats = await snooze.runRecoverySweep();
      expect(stats.recoveredLeases).toBe(1);

      await new Promise((r) => setTimeout(r, 50));
      expect(handlerFn).toHaveBeenCalledTimes(1);

      const job = await snooze.getJob("crashed_1");
      expect(job?.status).toBe("completed");
    });
  });
});
