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
  MongoSnoozeOptions,
  ScheduleOptions,
  WakeupDocument,
  MetaDocument,
} from "./types.js";
import {
  HandlerNotRegisteredError,
  IndexConflictError,
  JobNotFoundError,
  LeaseLostError,
  MongoSnoozeError,
  ReplicaSetRequiredError,
} from "./errors.js";

export class MongoSnooze extends EventEmitter {
  private readonly db: Db;
  private readonly prefix: string;
  public readonly workerId: string;
  private readonly leaseDurationMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly concurrency: number;
  private readonly recoveryIntervalMs: number;
  private readonly recoveryBatchSize: number;
  private readonly enableRecovery: boolean;
  private readonly shutdownTimeoutMs: number;
  private readonly tokenSaveIntervalMs: number;
  private readonly defaultMaxAttempts: number;
  private readonly defaultBackoffMs: number;
  private readonly defaultBackoffFactor: number;

  private jobsCollection!: Collection<JobDocument>;
  private wakeupsCollection!: Collection<WakeupDocument>;
  private metaCollection!: Collection<MetaDocument>;

  private readonly handlers = new Map<string, JobHandler>();
  private changeStream: ChangeStream | null = null;
  private recoveryTimer: NodeJS.Timeout | null = null;
  private tokenSaveTimer: NodeJS.Timeout | null = null;
  private latestResumeToken: any = null;

  private isRunning = false;
  private isStopping = false;
  private activeJobsCount = 0;
  private readonly inFlightPromises = new Set<Promise<void>>();

  // Deduplicated queue for wakeups
  private readonly pendingWakeupQueue: string[] = [];
  private readonly pendingWakeupSet = new Set<string>();

  constructor(options: MongoSnoozeOptions) {
    super();

    if (!options?.db) {
      throw new MongoSnoozeError("MongoDB 'db' instance is required.");
    }

    this.db = options.db;
    this.prefix = options.collectionPrefix || "mongosnooze";
    this.workerId = options.workerId || `worker_${randomUUID()}`;
    this.leaseDurationMs = options.leaseDurationMs ?? 30_000;
    this.heartbeatIntervalMs =
      options.heartbeatIntervalMs ?? Math.max(1000, Math.floor(this.leaseDurationMs / 2));
    this.concurrency = options.concurrency ?? 10;
    this.recoveryIntervalMs = options.recoveryIntervalMs ?? 30_000;
    this.recoveryBatchSize = options.recoveryBatchSize ?? 100;
    this.enableRecovery = options.enableRecovery ?? true;
    this.shutdownTimeoutMs = options.shutdownTimeoutMs ?? 10_000;
    this.tokenSaveIntervalMs = options.tokenSaveIntervalMs ?? 1_000;
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
   * Emits an event safely, preventing process crashes if an 'error' event has no listeners.
   */
  private safeEmit(event: string, ...args: any[]): boolean {
    if (event === "error" && this.listenerCount("error") === 0) {
      const err = args[0];
      const message = err instanceof Error ? err.stack || err.message : String(err);
      console.error(`[MongoSnooze Unhandled Error] ${message}`);
      return false;
    }
    return this.emit(event, ...args);
  }

  /**
   * Initializes MongoDB collections and necessary indexes.
   * Creates:
   * - TTL index on `{ expireAt: 1 }` with `expireAfterSeconds: 0` on wakeups collection.
   * - Compound index on `{ status: 1, runAt: 1 }` on jobs collection.
   * - Compound index on `{ status: 1, leaseExpiresAt: 1 }` on jobs collection.
   */
  public async initialize(): Promise<void> {
    try {
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
    } catch (err: any) {
      if (
        err?.code === 85 || // IndexOptionsConflict
        err?.code === 86 || // IndexKeySpecsConflict
        err?.codeName === "IndexOptionsConflict" ||
        err?.codeName === "IndexKeySpecsConflict"
      ) {
        throw new IndexConflictError(err);
      }
      throw err;
    }
  }

  /**
   * Registers a job handler for a specific job name.
   */
  public define<T = any>(name: string, handler: JobHandler<T>): this {
    if (!name || typeof name !== "string") {
      throw new MongoSnoozeError("Job name must be a non-empty string.");
    }
    if (typeof handler !== "function") {
      throw new MongoSnoozeError(`Handler for job "${name}" must be a function.`);
    }
    this.handlers.set(name, handler);
    return this;
  }

  /**
   * Schedules a delayed job with validation.
   */
  public async schedule<T = any>(
    name: string,
    data: T,
    options: ScheduleOptions = {}
  ): Promise<string> {
    if (!name || typeof name !== "string") {
      throw new MongoSnoozeError("Job name must be a non-empty string.");
    }

    let runAt: Date;
    if (options.runAt instanceof Date) {
      runAt = options.runAt;
    } else if (typeof options.delayMs === "number") {
      if (options.delayMs < 0 || !Number.isFinite(options.delayMs)) {
        throw new MongoSnoozeError("'delayMs' must be a non-negative finite number.");
      }
      runAt = new Date(Date.now() + options.delayMs);
    } else {
      runAt = new Date();
    }

    if (isNaN(runAt.getTime())) {
      throw new MongoSnoozeError("Invalid 'runAt' date provided for job.");
    }

    if (options.maxAttempts !== undefined) {
      if (!Number.isInteger(options.maxAttempts) || options.maxAttempts < 1) {
        throw new MongoSnoozeError("'maxAttempts' must be a positive integer.");
      }
    }

    if (options.backoffMs !== undefined) {
      if (!Number.isFinite(options.backoffMs) || options.backoffMs < 0) {
        throw new MongoSnoozeError("'backoffMs' must be a non-negative number.");
      }
    }

    if (options.backoffFactor !== undefined) {
      if (!Number.isFinite(options.backoffFactor) || options.backoffFactor < 1) {
        throw new MongoSnoozeError("'backoffFactor' must be a number >= 1.");
      }
    }

    if (options.jobId !== undefined && (!options.jobId || typeof options.jobId !== "string")) {
      throw new MongoSnoozeError("'jobId' must be a non-empty string if provided.");
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

    this.safeEmit("job:scheduled", jobDoc);
    return jobId;
  }

  /**
   * Cancels a pending job. Stale wakeup events will safely ignore cancelled jobs.
   */
  public async cancel(jobId: string): Promise<boolean> {
    if (!jobId || typeof jobId !== "string") {
      throw new MongoSnoozeError("Job ID must be a non-empty string.");
    }

    const now = new Date();
    const result = await this.jobsCollection.updateOne(
      { _id: jobId, status: { $in: ["pending", "failed"] } },
      { $set: { status: "cancelled", updatedAt: now } }
    );

    await this.wakeupsCollection.deleteOne({ _id: jobId }).catch(() => {});

    // Remove from in-memory queue if present
    this.pendingWakeupSet.delete(jobId);

    if (result.matchedCount > 0) {
      this.safeEmit("job:cancelled", jobId);
      return true;
    }

    return false;
  }

  /**
   * Reschedules an existing job to a new time.
   */
  public async reschedule(jobId: string, newRunAt: Date): Promise<boolean> {
    if (!jobId || typeof jobId !== "string") {
      throw new MongoSnoozeError("Job ID must be a non-empty string.");
    }
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

    this.safeEmit("job:rescheduled", jobId, newRunAt);
    return true;
  }

  /**
   * Retrieves a job document by ID.
   */
  public async getJob<T = any>(jobId: string): Promise<JobDocument<T> | null> {
    if (!jobId || typeof jobId !== "string") {
      throw new MongoSnoozeError("Job ID must be a non-empty string.");
    }
    return this.jobsCollection.findOne({ _id: jobId }) as Promise<JobDocument<T> | null>;
  }

  /**
   * Starts the MongoSnooze worker.
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
      // Immediate startup sweep
      await this.runRecoverySweep().catch((err) => this.safeEmit("error", err));

      // Periodic recovery sweeper
      this.recoveryTimer = setInterval(() => {
        this.runRecoverySweep().catch((err) => this.safeEmit("error", err));
      }, this.recoveryIntervalMs);

      if (this.recoveryTimer.unref) {
        this.recoveryTimer.unref();
      }
    }

    // Debounced token save sweeper
    this.tokenSaveTimer = setInterval(() => {
      this.flushResumeToken().catch(() => {});
    }, this.tokenSaveIntervalMs);

    if (this.tokenSaveTimer.unref) {
      this.tokenSaveTimer.unref();
    }
  }

  /**
   * Gracefully stops the worker.
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

    if (this.tokenSaveTimer) {
      clearInterval(this.tokenSaveTimer);
      this.tokenSaveTimer = null;
    }

    await this.flushResumeToken().catch(() => {});

    if (this.changeStream) {
      await this.changeStream.close().catch(() => {});
      this.changeStream = null;
    }

    // Await in-flight jobs up to shutdown timeout
    if (this.inFlightPromises.size > 0) {
      const waitAll = Promise.all(Array.from(this.inFlightPromises));
      const timeoutPromise = new Promise((resolve) =>
        setTimeout(resolve, this.shutdownTimeoutMs)
      );

      await Promise.race([waitAll, timeoutPromise]);
    }

    // Release any lingering uncompleted leases
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

    this.pendingWakeupQueue.length = 0;
    this.pendingWakeupSet.clear();
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
      if (this.isReplicaSetError(err)) {
        this.isRunning = false;
        throw new ReplicaSetRequiredError();
      }
      throw err;
    }

    this.changeStream.on("change", (change: ChangeStreamDocument<WakeupDocument>) => {
      if (!this.isRunning || this.isStopping) return;

      if (change.operationType === "delete") {
        const deletedId = (change.documentKey as any)?._id;
        if (deletedId) {
          this.enqueueWakeup(deletedId);
        }
      }

      if (change._id) {
        this.latestResumeToken = change._id;
      }
    });

    this.changeStream.on("error", async (err: any) => {
      if (this.isStopping || !this.isRunning) return;

      if (this.isReplicaSetError(err)) {
        this.isRunning = false;
        this.safeEmit("error", new ReplicaSetRequiredError());
        return; // Do not reconnect on non-replica set
      }

      this.safeEmit("error", err);

      const isOplogLost =
        err?.code === 286 ||
        err?.codeName === "ChangeStreamHistoryLost" ||
        err?.message?.includes("resume token");

      if (isOplogLost) {
        this.latestResumeToken = null;
        await this.metaCollection.deleteOne({ _id: "resume_token" }).catch(() => {});
      }

      // Reconnect after short backoff
      setTimeout(async () => {
        if (this.isRunning && !this.isStopping) {
          try {
            await this.startChangeStream();
            await this.runRecoverySweep();
          } catch (reconnectErr: any) {
            this.safeEmit("error", reconnectErr);
          }
        }
      }, 2000);
    });
  }

  private isReplicaSetError(err: any): boolean {
    return (
      err?.message?.includes("replica set") ||
      err?.message?.includes("$changeStream stage is only supported on replica sets") ||
      err?.codeName === "Location40573" ||
      err?.code === 40573
    );
  }

  private async flushResumeToken(): Promise<void> {
    if (!this.latestResumeToken) return;
    const tokenToSave = this.latestResumeToken;
    try {
      await this.metaCollection.updateOne(
        { _id: "resume_token" },
        { $set: { resumeToken: tokenToSave, updatedAt: new Date() } },
        { upsert: true }
      );
    } catch {
      // Non-critical background save
    }
  }

  private enqueueWakeup(jobId: string): void {
    if (this.pendingWakeupSet.has(jobId)) {
      return; // Deduplicate
    }

    if (this.activeJobsCount >= this.concurrency) {
      this.pendingWakeupSet.add(jobId);
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
      this.pendingWakeupSet.delete(nextId);
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
    // Add small random jitter (0-20ms) to reduce contention across multiple workers
    const jitter = Math.floor(Math.random() * 20);
    if (jitter > 0) {
      await new Promise((r) => setTimeout(r, jitter));
    }

    try {
      const claimed = await this.claimJob(jobId);
      if (claimed) {
        await this.executeJob(claimed);
      }
    } catch (err: any) {
      this.safeEmit("error", err);
    }
  }

  /**
   * Atomically claims an eligible job for this worker.
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

    // Job touch helper allowing developer to manually extend the lease
    const touch = async (extraMs = this.leaseDurationMs): Promise<boolean> => {
      const newLease = new Date(Date.now() + extraMs);
      const res = await this.jobsCollection.updateOne(
        { _id: jobDoc._id, lockedBy: this.workerId, status: "running" },
        { $set: { leaseExpiresAt: newLease, updatedAt: new Date() } }
      );
      return res.matchedCount > 0;
    };

    const jobInfo: JobInfo = {
      id: jobDoc._id,
      name: jobDoc.name,
      data: jobDoc.data,
      attempts: jobDoc.attempts,
      maxAttempts: jobDoc.maxAttempts,
      runAt: jobDoc.runAt,
      createdAt: jobDoc.createdAt,
      touch,
    };

    if (!handler) {
      const err = new HandlerNotRegisteredError(jobDoc.name);
      await this.handleJobFailure(jobDoc, err);
      return;
    }

    this.safeEmit("job:started", jobInfo);

    // Auto-heartbeat: periodically extend lease during execution to protect long tasks
    const heartbeat = setInterval(async () => {
      try {
        const renewed = await touch(this.leaseDurationMs);
        if (!renewed) {
          clearInterval(heartbeat);
        }
      } catch {
        clearInterval(heartbeat);
      }
    }, this.heartbeatIntervalMs);

    if (heartbeat.unref) {
      heartbeat.unref();
    }

    try {
      const result = await handler(jobInfo);
      clearInterval(heartbeat);
      await this.handleJobSuccess(jobDoc, result);
    } catch (err: any) {
      clearInterval(heartbeat);
      await this.handleJobFailure(jobDoc, err);
    }
  }

  private async handleJobSuccess(jobDoc: JobDocument, result: any): Promise<void> {
    const now = new Date();

    // Enforce atomic ownership: only mark completed if WE still hold the lock!
    const updateResult = await this.jobsCollection.updateOne(
      { _id: jobDoc._id, lockedBy: this.workerId, status: "running" },
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

    if (updateResult.matchedCount === 0) {
      // Lease was stolen by another worker or recovery sweep!
      this.safeEmit("error", new LeaseLostError(jobDoc._id));
      return;
    }

    // Clean up wakeup doc if it still exists
    await this.wakeupsCollection.deleteOne({ _id: jobDoc._id }).catch(() => {});

    const jobInfo: JobInfo = {
      id: jobDoc._id,
      name: jobDoc.name,
      data: jobDoc.data,
      attempts: jobDoc.attempts,
      maxAttempts: jobDoc.maxAttempts,
      runAt: jobDoc.runAt,
      createdAt: jobDoc.createdAt,
      touch: async () => false,
    };

    this.safeEmit("job:completed", jobInfo, result);
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
      touch: async () => false,
    };

    if (attempts < jobDoc.maxAttempts) {
      const backoffMs =
        jobDoc.backoffMs * Math.pow(jobDoc.backoffFactor, attempts - 1);
      const nextRunAt = new Date(now.getTime() + backoffMs);

      const updateResult = await this.jobsCollection.updateOne(
        { _id: jobDoc._id, lockedBy: this.workerId, status: "running" },
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

      if (updateResult.matchedCount === 0) {
        this.safeEmit("error", new LeaseLostError(jobDoc._id));
        return;
      }

      await this.wakeupsCollection.updateOne(
        { _id: jobDoc._id },
        { $set: { _id: jobDoc._id, expireAt: nextRunAt } },
        { upsert: true }
      );

      this.safeEmit("job:retrying", jobInfo, error, nextRunAt);
    } else {
      const updateResult = await this.jobsCollection.updateOne(
        { _id: jobDoc._id, lockedBy: this.workerId, status: "running" },
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

      if (updateResult.matchedCount === 0) {
        this.safeEmit("error", new LeaseLostError(jobDoc._id));
        return;
      }

      await this.wakeupsCollection.deleteOne({ _id: jobDoc._id }).catch(() => {});
      this.safeEmit("job:failed", jobInfo, error);
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

    // 2. Recover pending jobs that are due but missed trigger
    const missedPendingCursor = this.jobsCollection
      .find({
        status: "pending",
        runAt: { $lte: now },
      })
      .limit(this.recoveryBatchSize);

    for await (const job of missedPendingCursor) {
      this.enqueueWakeup(job._id);
      recoveredPending++;
    }

    const stats = { recoveredLeases, recoveredPending };
    if (recoveredLeases > 0 || recoveredPending > 0) {
      this.safeEmit("recovery:sweep", stats);
    }

    return stats;
  }
}
