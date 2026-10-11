import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import type {
  ChangeStream,
  Collection,
  Db,
  ChangeStreamDocument,
} from "mongodb";
import type {
  JobDocument,
  JobHandler,
  JobInfo,
  MongoSnoozeEvents,
  MongoSnoozeOptions,
  ScheduleOptions,
  WakeupDocument,
  MetaDocument,
} from "./types.js";
import {
  HandlerNotRegisteredError,
  JobNotFoundError,
  MongoSnoozeError,
  ReplicaSetRequiredError,
} from "./errors.js";

export class MongoSnooze extends EventEmitter {
  private readonly db: Db;
  private readonly prefix: string;
  public readonly workerId: string;
  private readonly leaseDurationMs: number;
  private readonly concurrency: number;
  private readonly recoveryIntervalMs: number;
  private readonly enableRecovery: boolean;
  private readonly shutdownTimeoutMs: number;
  private readonly defaultMaxAttempts: number;
  private readonly defaultBackoffMs: number;
  private readonly defaultBackoffFactor: number;

  private jobsCollection!: Collection<JobDocument>;
  private wakeupsCollection!: Collection<WakeupDocument>;
  private metaCollection!: Collection<MetaDocument>;

  private readonly handlers = new Map<string, JobHandler>();
  private changeStream: ChangeStream | null = null;
  private recoveryTimer: NodeJS.Timeout | null = null;
  private isRunning = false;
  private isStopping = false;
  private activeJobsCount = 0;
  private readonly inFlightPromises = new Set<Promise<void>>();
  private readonly pendingWakeupQueue: string[] = [];

  constructor(options: MongoSnoozeOptions) {
    super();

    if (!options?.db) {
      throw new MongoSnoozeError("MongoDB 'db' instance is required.");
    }

    this.db = options.db;
    this.prefix = options.collectionPrefix || "mongosnooze";
    this.workerId = options.workerId || `worker_${randomUUID()}`;
    this.leaseDurationMs = options.leaseDurationMs ?? 30_000;
    this.concurrency = options.concurrency ?? 10;
    this.recoveryIntervalMs = options.recoveryIntervalMs ?? 30_000;
    this.enableRecovery = options.enableRecovery ?? true;
    this.shutdownTimeoutMs = options.shutdownTimeoutMs ?? 10_000;
    this.defaultMaxAttempts = options.defaultMaxAttempts ?? 3;
    this.defaultBackoffMs = options.defaultBackoffMs ?? 5_000;
    this.defaultBackoffFactor = options.defaultBackoffFactor ?? 2;

    this.initCollections();
  }

  private initCollections(): void {
    this.jobsCollection = this.db.collection<JobDocument>(`${this.prefix}_jobs`);
    this.wakeupsCollection = this.db.collection<WakeupDocument>(
      `${this.prefix}_wakeups`
    );
    this.metaCollection = this.db.collection<MetaDocument>(`${this.prefix}_meta`);
  }

  /**
   * Initializes MongoDB collections and necessary indexes.
   * Creates:
   * - TTL index on `{ expireAt: 1 }` with `expireAfterSeconds: 0` on wakeups collection.
   * - Compound index on `{ status: 1, runAt: 1 }` on jobs collection.
   * - Compound index on `{ status: 1, leaseExpiresAt: 1 }` on jobs collection.
   */
  public async initialize(): Promise<void> {
    // 1. Wakeups TTL index
    await this.wakeupsCollection.createIndex(
      { expireAt: 1 },
      { expireAfterSeconds: 0, name: "mongosnooze_ttl_index" }
    );

    // 2. Jobs recovery & polling safety indexes
    await this.jobsCollection.createIndex(
      { status: 1, runAt: 1 },
      { name: "mongosnooze_status_runAt" }
    );
    await this.jobsCollection.createIndex(
      { status: 1, leaseExpiresAt: 1 },
      { name: "mongosnooze_status_lease" }
    );
    await this.jobsCollection.createIndex(
      { name: 1 },
      { name: "mongosnooze_job_name" }
    );
  }

  /**
   * Registers a job handler for a specific job name.
   */
  public define<T = any>(name: string, handler: JobHandler<T>): this {
    if (typeof handler !== "function") {
      throw new MongoSnoozeError(`Handler for job "${name}" must be a function.`);
    }
    this.handlers.set(name, handler);
    return this;
  }

  /**
   * Schedules a delayed job.
   */
  public async schedule<T = any>(
    name: string,
    data: T,
    options: ScheduleOptions = {}
  ): Promise<string> {
    let runAt: Date;
    if (options.runAt instanceof Date) {
      runAt = options.runAt;
    } else if (typeof options.delayMs === "number" && options.delayMs >= 0) {
      runAt = new Date(Date.now() + options.delayMs);
    } else {
      runAt = new Date(); // Due immediately if no timing specified
    }

    if (isNaN(runAt.getTime())) {
      throw new MongoSnoozeError("Invalid 'runAt' date provided for job.");
    }

    const jobId = options.jobId || randomUUID();
    const now = new Date();

    const jobDoc: JobDocument<T> = {
      _id: jobId,
      name,
      data,
      status: "pending",
      runAt,
      attempts: 0,
      maxAttempts: options.maxAttempts ?? this.defaultMaxAttempts,
      backoffMs: options.backoffMs ?? this.defaultBackoffMs,
      backoffFactor: options.backoffFactor ?? this.defaultBackoffFactor,
      lockedBy: null,
      lockedAt: null,
      leaseExpiresAt: null,
      createdAt: now,
      updatedAt: now,
    };

    // 1. Insert durable job record
    await this.jobsCollection.insertOne(jobDoc as any);

    // 2. Insert disposable TTL wakeup document
    await this.wakeupsCollection.updateOne(
      { _id: jobId },
      { $set: { _id: jobId, expireAt: runAt } },
      { upsert: true }
    );

    this.emit("job:scheduled", jobDoc);
    return jobId;
  }

  /**
   * Cancels a pending job. Stale wakeup events will safely ignore cancelled jobs.
   */
  public async cancel(jobId: string): Promise<boolean> {
    const now = new Date();
    const result = await this.jobsCollection.updateOne(
      { _id: jobId, status: { $in: ["pending", "failed"] } },
      { $set: { status: "cancelled", updatedAt: now } }
    );

    await this.wakeupsCollection.deleteOne({ _id: jobId });

    if (result.matchedCount > 0) {
      this.emit("job:cancelled", jobId);
      return true;
    }

    return false;
  }

  /**
   * Reschedules an existing job to a new time.
   */
  public async reschedule(jobId: string, newRunAt: Date): Promise<boolean> {
    if (!(newRunAt instanceof Date) || isNaN(newRunAt.getTime())) {
      throw new MongoSnoozeError("Invalid 'newRunAt' date provided.");
    }

    const now = new Date();
    const result = await this.jobsCollection.updateOne(
      { _id: jobId, status: { $in: ["pending", "failed", "cancelled"] } },
      {
        $set: {
          runAt: newRunAt,
          status: "pending",
          updatedAt: now,
        },
      }
    );

    if (result.matchedCount === 0) {
      return false;
    }

    await this.wakeupsCollection.updateOne(
      { _id: jobId },
      { $set: { _id: jobId, expireAt: newRunAt } },
      { upsert: true }
    );

    this.emit("job:rescheduled", jobId, newRunAt);
    return true;
  }

  /**
   * Retrieves a job document by ID.
   */
  public async getJob<T = any>(jobId: string): Promise<JobDocument<T> | null> {
    return this.jobsCollection.findOne({ _id: jobId }) as Promise<JobDocument<T> | null>;
  }

  /**
   * Starts the MongoSnooze worker:
   * 1. Ensures indexes are initialized.
   * 2. Starts the Change Stream on the wakeups collection.
   * 3. Starts the safety recovery sweeper.
   */
  public async start(): Promise<void> {
    if (this.isRunning) {
      return;
    }

    this.isRunning = true;
    this.isStopping = false;

    await this.initialize();
    await this.startChangeStream();

    if (this.enableRecovery) {
      // Run an immediate recovery sweep on startup to catch jobs missed during downtime
      await this.runRecoverySweep().catch((err) => this.emit("error", err));

      // Schedule periodic recovery sweeps
      this.recoveryTimer = setInterval(() => {
        this.runRecoverySweep().catch((err) => this.emit("error", err));
      }, this.recoveryIntervalMs);

      // Prevent the timer from holding Node.js event loop open if stopped
      if (this.recoveryTimer.unref) {
        this.recoveryTimer.unref();
      }
    }
  }

  /**
   * Gracefully stops the worker:
   * 1. Stops accepting new wakeups.
   * 2. Stops the recovery sweeper.
   * 3. Closes Change Stream.
   * 4. Awaits active in-flight jobs up to `shutdownTimeoutMs`.
   * 5. Releases any remaining locked jobs back to pending.
   */
  public async stop(): Promise<void> {
    if (!this.isRunning || this.isStopping) {
      return;
    }

    this.isStopping = true;
    this.isRunning = false;

    if (this.recoveryTimer) {
      clearInterval(this.recoveryTimer);
      this.recoveryTimer = null;
    }

    if (this.changeStream) {
      await this.changeStream.close().catch(() => {});
      this.changeStream = null;
    }

    // Wait for in-flight jobs
    if (this.inFlightPromises.size > 0) {
      const waitAll = Promise.all(Array.from(this.inFlightPromises));
      const timeoutPromise = new Promise((resolve) =>
        setTimeout(resolve, this.shutdownTimeoutMs)
      );

      await Promise.race([waitAll, timeoutPromise]);
    }

    // Release any lingering leases held by this worker
    const now = new Date();
    await this.jobsCollection.updateMany(
      { lockedBy: this.workerId, status: "running" },
      {
        $set: {
          status: "pending",
          lockedBy: null,
          lockedAt: null,
          leaseExpiresAt: null,
          updatedAt: now,
        },
      }
    ).catch(() => {});

    this.isStopping = false;
  }

  private async startChangeStream(): Promise<void> {
    let resumeToken: any = null;

    try {
      const meta = await this.metaCollection.findOne({ _id: "resume_token" });
      if (meta?.resumeToken) {
        resumeToken = meta.resumeToken;
      }
    } catch {
      // Ignore meta lookup error on startup
    }

    const pipeline = [{ $match: { operationType: "delete" } }];
    const streamOptions: any = {};
    if (resumeToken) {
      streamOptions.resumeAfter = resumeToken;
    }

    try {
      this.changeStream = this.wakeupsCollection.watch(pipeline, streamOptions);
    } catch (err: any) {
      if (
        err?.message?.includes("replica set") ||
        err?.codeName === "Location40573" ||
        err?.code === 40573
      ) {
        throw new ReplicaSetRequiredError();
      }
      throw err;
    }

    this.changeStream.on("change", async (change: ChangeStreamDocument<WakeupDocument>) => {
      if (!this.isRunning || this.isStopping) return;

      if (change.operationType === "delete") {
        const deletedId = (change.documentKey as any)?._id;
        if (deletedId) {
          this.enqueueWakeup(deletedId);
        }
      }

      // Persist latest resume token
      if (change._id) {
        this.saveResumeToken(change._id).catch(() => {});
      }
    });

    this.changeStream.on("error", async (err: any) => {
      if (this.isStopping || !this.isRunning) return;

      this.emit("error", err);

      // If the resume token is stale/lost (oplog history rolled over), restart without it
      const isOplogLost =
        err?.code === 286 || // ChangeStreamHistoryLost
        err?.codeName === "ChangeStreamHistoryLost" ||
        err?.message?.includes("resume token");

      if (isOplogLost) {
        await this.metaCollection.deleteOne({ _id: "resume_token" }).catch(() => {});
      }

      // Reconnect change stream after short backoff
      setTimeout(async () => {
        if (this.isRunning && !this.isStopping) {
          try {
            await this.startChangeStream();
            await this.runRecoverySweep();
          } catch (reconnectErr: any) {
            this.emit("error", reconnectErr);
          }
        }
      }, 2000);
    });
  }

  private async saveResumeToken(token: any): Promise<void> {
    try {
      await this.metaCollection.updateOne(
        { _id: "resume_token" },
        { $set: { resumeToken: token, updatedAt: new Date() } },
        { upsert: true }
      );
    } catch {
      // Non-critical background save
    }
  }

  private enqueueWakeup(jobId: string): void {
    if (this.activeJobsCount >= this.concurrency) {
      this.pendingWakeupQueue.push(jobId);
      return;
    }

    this.processJob(jobId);
  }

  private processNextInQueue(): void {
    if (this.isStopping || !this.isRunning) return;
    if (this.activeJobsCount >= this.concurrency) return;

    const nextId = this.pendingWakeupQueue.shift();
    if (nextId) {
      this.processJob(nextId);
    }
  }

  private processJob(jobId: string): void {
    this.activeJobsCount++;
    const jobPromise = this.runJob(jobId);
    this.inFlightPromises.add(jobPromise);

    jobPromise.finally(() => {
      this.activeJobsCount--;
      this.inFlightPromises.delete(jobPromise);
      this.processNextInQueue();
    });
  }

  private async runJob(jobId: string): Promise<void> {
    try {
      const claimed = await this.claimJob(jobId);
      if (claimed) {
        await this.executeJob(claimed);
      }
    } catch (err: any) {
      this.emit("error", err);
    }
  }

  /**
   * Atomically claims an eligible job for this worker.
   * Multi-worker safe: uses findOneAndUpdate with lease and status criteria.
   */
  private async claimJob(jobId: string): Promise<JobDocument | null> {
    const now = new Date();
    const leaseExpiresAt = new Date(now.getTime() + this.leaseDurationMs);

    const result = await this.jobsCollection.findOneAndUpdate(
      {
        _id: jobId,
        status: { $in: ["pending", "failed"] },
        runAt: { $lte: now },
      },
      {
        $set: {
          status: "running",
          lockedBy: this.workerId,
          lockedAt: now,
          leaseExpiresAt,
          updatedAt: now,
        },
        $inc: { attempts: 1 },
      },
      { returnDocument: "after" }
    );

    return result as JobDocument | null;
  }

  private async executeJob(jobDoc: JobDocument): Promise<void> {
    const handler = this.handlers.get(jobDoc.name);
    const jobInfo: JobInfo = {
      id: jobDoc._id,
      name: jobDoc.name,
      data: jobDoc.data,
      attempts: jobDoc.attempts,
      maxAttempts: jobDoc.maxAttempts,
      runAt: jobDoc.runAt,
      createdAt: jobDoc.createdAt,
    };

    if (!handler) {
      const err = new HandlerNotRegisteredError(jobDoc.name);
      await this.handleJobFailure(jobDoc, err);
      return;
    }

    this.emit("job:started", jobInfo);

    try {
      const result = await handler(jobInfo);
      await this.handleJobSuccess(jobDoc, result);
    } catch (err: any) {
      await this.handleJobFailure(jobDoc, err);
    }
  }

  private async handleJobSuccess(jobDoc: JobDocument, result: any): Promise<void> {
    const now = new Date();
    await this.jobsCollection.updateOne(
      { _id: jobDoc._id },
      {
        $set: {
          status: "completed",
          result: result !== undefined ? result : null,
          lockedBy: null,
          lockedAt: null,
          leaseExpiresAt: null,
          updatedAt: now,
        },
      }
    );

    // Clean up wakeup doc if it somehow still exists
    await this.wakeupsCollection.deleteOne({ _id: jobDoc._id }).catch(() => {});

    const jobInfo: JobInfo = {
      id: jobDoc._id,
      name: jobDoc.name,
      data: jobDoc.data,
      attempts: jobDoc.attempts,
      maxAttempts: jobDoc.maxAttempts,
      runAt: jobDoc.runAt,
      createdAt: jobDoc.createdAt,
    };

    this.emit("job:completed", jobInfo, result);
  }

  private async handleJobFailure(jobDoc: JobDocument, error: any): Promise<void> {
    const now = new Date();
    const errorMessage = error instanceof Error ? error.message : String(error);
    const errorStack = error instanceof Error ? error.stack : undefined;
    const attempts = jobDoc.attempts;

    const jobInfo: JobInfo = {
      id: jobDoc._id,
      name: jobDoc.name,
      data: jobDoc.data,
      attempts,
      maxAttempts: jobDoc.maxAttempts,
      runAt: jobDoc.runAt,
      createdAt: jobDoc.createdAt,
    };

    if (attempts < jobDoc.maxAttempts) {
      // Calculate exponential backoff
      const backoffMs =
        jobDoc.backoffMs * Math.pow(jobDoc.backoffFactor, attempts - 1);
      const nextRunAt = new Date(now.getTime() + backoffMs);

      await this.jobsCollection.updateOne(
        { _id: jobDoc._id },
        {
          $set: {
            status: "pending",
            runAt: nextRunAt,
            error: { message: errorMessage, stack: errorStack },
            lockedBy: null,
            lockedAt: null,
            leaseExpiresAt: null,
            updatedAt: now,
          },
        }
      );

      // Re-create TTL trigger in wakeups collection for the retry time
      await this.wakeupsCollection.updateOne(
        { _id: jobDoc._id },
        { $set: { _id: jobDoc._id, expireAt: nextRunAt } },
        { upsert: true }
      );

      this.emit("job:retrying", jobInfo, error, nextRunAt);
    } else {
      // Max attempts exceeded: mark permanently failed
      await this.jobsCollection.updateOne(
        { _id: jobDoc._id },
        {
          $set: {
            status: "failed",
            error: { message: errorMessage, stack: errorStack },
            lockedBy: null,
            lockedAt: null,
            leaseExpiresAt: null,
            updatedAt: now,
          },
        }
      );

      await this.wakeupsCollection.deleteOne({ _id: jobDoc._id }).catch(() => {});
      this.emit("job:failed", jobInfo, error);
    }
  }

  /**
   * Safety Net: Reconciles jobs that missed TTL delete events or whose workers crashed.
   */
  public async runRecoverySweep(): Promise<{
    recoveredLeases: number;
    recoveredPending: number;
  }> {
    const now = new Date();
    let recoveredLeases = 0;
    let recoveredPending = 0;

    // 1. Recover expired leases (worker crashed mid-execution)
    const expiredLeaseCursor = this.jobsCollection.find({
      status: "running",
      leaseExpiresAt: { $lt: now },
    });

    for await (const job of expiredLeaseCursor) {
      if (job.attempts < job.maxAttempts) {
        // Reset to pending so it can be re-run
        await this.jobsCollection.updateOne(
          { _id: job._id, status: "running" },
          {
            $set: {
              status: "pending",
              lockedBy: null,
              lockedAt: null,
              leaseExpiresAt: null,
              updatedAt: now,
            },
          }
        );
        this.enqueueWakeup(job._id);
        recoveredLeases++;
      } else {
        // Mark failed
        await this.jobsCollection.updateOne(
          { _id: job._id, status: "running" },
          {
            $set: {
              status: "failed",
              error: { message: "Job lease expired and max attempts reached." },
              lockedBy: null,
              lockedAt: null,
              leaseExpiresAt: null,
              updatedAt: now,
            },
          }
        );
      }
    }

    // 2. Recover pending jobs that are due but missed trigger (e.g. worker was offline)
    const missedPendingCursor = this.jobsCollection
      .find({
        status: "pending",
        runAt: { $lte: now },
      })
      .limit(100);

    for await (const job of missedPendingCursor) {
      this.enqueueWakeup(job._id);
      recoveredPending++;
    }

    const stats = { recoveredLeases, recoveredPending };
    if (recoveredLeases > 0 || recoveredPending > 0) {
      this.emit("recovery:sweep", stats);
    }

    return stats;
  }
}
