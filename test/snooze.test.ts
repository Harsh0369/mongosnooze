import { describe, it, expect, vi, beforeEach } from "vitest";
import { MongoSnooze } from "../src/snooze.js";
import { createMockDb, MockCollection } from "./mockDb.js";
import {
  MongoSnoozeError,
  IndexConflictError,
  LeaseLostError,
  ReplicaSetRequiredError,
} from "../src/errors.js";

describe("MongoSnooze - Production Hardened", () => {
  let mockDb: any;

  beforeEach(() => {
    mockDb = createMockDb();
  });

  describe("Validation & Security", () => {
    it("should throw if db instance is not provided", () => {
      expect(() => new MongoSnooze({} as any)).toThrow(MongoSnoozeError);
    });

    it("should validate job name on define()", () => {
      const snooze = new MongoSnooze({ db: mockDb });
      expect(() => snooze.define("", () => {})).toThrow("Job name must be a non-empty string");
      expect(() => snooze.define(123 as any, () => {})).toThrow("Job name must be a non-empty string");
      expect(() => snooze.define("valid", "not-a-fn" as any)).toThrow("must be a function");
    });

    it("should validate schedule parameters", async () => {
      const snooze = new MongoSnooze({ db: mockDb });

      await expect(snooze.schedule("", {})).rejects.toThrow("Job name must be a non-empty string");
      await expect(snooze.schedule("test", {}, { delayMs: -50 })).rejects.toThrow("delayMs");
      await expect(snooze.schedule("test", {}, { runAt: new Date("invalid") })).rejects.toThrow("runAt");
      await expect(snooze.schedule("test", {}, { maxAttempts: 0 })).rejects.toThrow("maxAttempts");
      await expect(snooze.schedule("test", {}, { backoffMs: -10 })).rejects.toThrow("backoffMs");
      await expect(snooze.schedule("test", {}, { backoffFactor: 0.5 })).rejects.toThrow("backoffFactor");
      await expect(snooze.schedule("test", {}, { jobId: "" })).rejects.toThrow("jobId");
    });

    it("should validate reschedule parameters", async () => {
      const snooze = new MongoSnooze({ db: mockDb });
      await expect(snooze.reschedule("", new Date())).rejects.toThrow("Job ID must be a non-empty string");
      await expect(snooze.reschedule("job_1", new Date("invalid"))).rejects.toThrow("newRunAt");
    });

    it("should validate cancel parameters", async () => {
      const snooze = new MongoSnooze({ db: mockDb });
      await expect(snooze.cancel("")).rejects.toThrow("Job ID must be a non-empty string");
    });
  });

  describe("Index Conflict & Replica Set Resilience", () => {
    it("should wrap index conflict in IndexConflictError", async () => {
      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      wakeups.shouldFailWithIndexConflict = true;

      const snooze = new MongoSnooze({ db: mockDb });
      await expect(snooze.initialize()).rejects.toThrow(IndexConflictError);
    });

    it("should throw ReplicaSetRequiredError on standalone MongoDB", async () => {
      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      wakeups.shouldFailWithReplicaSet = true;

      const snooze = new MongoSnooze({ db: mockDb });
      await expect(snooze.start()).rejects.toThrow(ReplicaSetRequiredError);
    });
  });

  describe("Safe Error Handling", () => {
    it("should not crash process when error is emitted without listeners", () => {
      const snooze = new MongoSnooze({ db: mockDb });
      const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

      // Call internal safeEmit without attaching snooze.on('error')
      (snooze as any).safeEmit("error", new Error("Simulated background error"));

      expect(consoleSpy).toHaveBeenCalledTimes(1);
      consoleSpy.mockRestore();
    });
  });

  describe("Queue Deduplication", () => {
    it("should deduplicate wakeups in memory queue", async () => {
      const snooze = new MongoSnooze({ db: mockDb, concurrency: 1 });
      let handlerRunning = true;

      snooze.define("slow-job", async () => {
        while (handlerRunning) {
          await new Promise((r) => setTimeout(r, 10));
        }
      });

      await snooze.start();

      // Trigger 1 job to saturate concurrency
      await snooze.schedule("slow-job", {}, { runAt: new Date(Date.now() - 1000) });
      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      await wakeups.deleteOne({ _id: "any" });

      // Enqueue the SAME jobId multiple times
      (snooze as any).enqueueWakeup("duplicate_id");
      (snooze as any).enqueueWakeup("duplicate_id");
      (snooze as any).enqueueWakeup("duplicate_id");

      // Verify internal queue only holds it once
      expect((snooze as any).pendingWakeupQueue).toHaveLength(1);
      expect((snooze as any).pendingWakeupSet.size).toBe(1);

      handlerRunning = false;
      await snooze.stop();
    });
  });

  describe("Heartbeat & Lease Safety", () => {
    it("should allow handler to manually touch and extend lease", async () => {
      const snooze = new MongoSnooze({ db: mockDb, leaseDurationMs: 10_000 });
      let leaseExtended = false;

      snooze.define("long-task", async (job) => {
        // Manually extend lease
        leaseExtended = await job.touch(30_000);
        return "done";
      });

      await snooze.start();
      const jobId = await snooze.schedule("long-task", {}, { runAt: new Date(Date.now() - 1000) });

      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      await wakeups.deleteOne({ _id: jobId });

      await new Promise((r) => setTimeout(r, 50));

      expect(leaseExtended).toBe(true);
      const job = await snooze.getJob(jobId);
      expect(job?.status).toBe("completed");

      await snooze.stop();
    });

    it("should abort completion and emit LeaseLostError if another worker took the lease", async () => {
      const snooze = new MongoSnooze({ db: mockDb });
      const errorListener = vi.fn();
      snooze.on("error", errorListener);

      snooze.define("stolen-task", async () => {
        // Simulate another worker stealing the job while this one is still executing
        const jobs = mockDb.collection("mongosnooze_jobs") as MockCollection;
        await jobs.updateOne(
          { name: "stolen-task" },
          { $set: { lockedBy: "another_rogue_worker" } }
        );
        return "done";
      });

      await snooze.start();
      const jobId = await snooze.schedule("stolen-task", {}, { runAt: new Date(Date.now() - 1000) });

      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      await wakeups.deleteOne({ _id: jobId });

      await new Promise((r) => setTimeout(r, 50));

      expect(errorListener).toHaveBeenCalled();
      const lastErr = errorListener.mock.calls[0][0];
      expect(lastErr).toBeInstanceOf(LeaseLostError);

      await snooze.stop();
    });
  });

  describe("Standard End-to-End Workflow", () => {
    it("should complete delayed job, retry on error, and recover expired leases", async () => {
      const snooze = new MongoSnooze({
        db: mockDb,
        defaultMaxAttempts: 2,
        defaultBackoffMs: 10,
      });

      let attemptsCount = 0;
      snooze.define("flaky-task", async () => {
        attemptsCount++;
        if (attemptsCount === 1) {
          throw new Error("Temporary network glitch");
        }
        return "recovered";
      });

      await snooze.start();
      const jobId = await snooze.schedule("flaky-task", {}, { runAt: new Date(Date.now() - 1000) });

      const wakeups = mockDb.collection("mongosnooze_wakeups") as MockCollection;
      // Trigger attempt 1
      await wakeups.deleteOne({ _id: jobId });
      await new Promise((r) => setTimeout(r, 60));

      expect(attemptsCount).toBe(1);
      let job = await snooze.getJob(jobId);
      expect(job?.status).toBe("pending");
      expect(job?.attempts).toBe(1);

      // Wait for backoff delay (10ms) to elapse so runAt <= now
      await new Promise((r) => setTimeout(r, 30));

      // Trigger retry (attempt 2)
      await wakeups.deleteOne({ _id: jobId });
      await new Promise((r) => setTimeout(r, 80));

      expect(attemptsCount).toBe(2);
      job = await snooze.getJob(jobId);
      expect(job?.status).toBe("completed");
      expect(job?.result).toBe("recovered");

      await snooze.stop();
    });
  });
});
