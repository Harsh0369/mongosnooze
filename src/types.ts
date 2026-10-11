import type { Db } from "mongodb";

export type JobStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

export interface JobDocument<T = any> {
  _id: string;
  name: string;
  data: T;
  status: JobStatus;
  runAt: Date;
  attempts: number;
  maxAttempts: number;
  backoffMs: number;
  backoffFactor: number;
  lockedBy: string | null;
  lockedAt: Date | null;
  leaseExpiresAt: Date | null;
  result?: any;
  error?: {
    message: string;
    stack?: string;
  };
  createdAt: Date;
  updatedAt: Date;
}

export interface WakeupDocument {
  _id: string;
  expireAt: Date;
}

export interface MetaDocument {
  _id: string;
  resumeToken?: any;
  updatedAt: Date;
}

export interface ScheduleOptions {
  /**
   * The exact Date when the job should run.
   */
  runAt?: Date;
  /**
   * Delay in milliseconds from now when the job should run.
   * Mutually exclusive with runAt (or used if runAt is not provided).
   */
  delayMs?: number;
  /**
   * Maximum number of attempts for this job.
   * Defaults to the scheduler's defaultMaxAttempts (default: 3).
   */
  maxAttempts?: number;
  /**
   * Base backoff delay in milliseconds for retries.
   * Defaults to scheduler's defaultBackoffMs (default: 5000).
   */
  backoffMs?: number;
  /**
   * Exponential backoff factor (multiplier on each retry attempt).
   * Default: 2.
   */
  backoffFactor?: number;
  /**
   * Custom unique ID for the job. If not provided, a UUID will be generated.
   */
  jobId?: string;
}

export interface MongoSnoozeOptions {
  /**
   * The MongoDB database instance.
   */
  db: Db;
  /**
   * Prefix for MongoSnooze collections:
   * - `${prefix}_jobs`
   * - `${prefix}_wakeups`
   * - `${prefix}_meta`
   * Default: 'mongosnooze'
   */
  collectionPrefix?: string;
  /**
   * Unique identifier for this worker instance.
   * Defaults to a randomly generated UUID.
   */
  workerId?: string;
  /**
   * Duration in milliseconds for job leases before considering the worker dead.
   * Default: 30000 (30 seconds).
   */
  leaseDurationMs?: number;
  /**
   * Maximum number of concurrent jobs this worker instance will process simultaneously.
   * Default: 10.
   */
  concurrency?: number;
  /**
   * Interval in milliseconds for running the safety recovery sweep.
   * Default: 30000 (30 seconds).
   */
  recoveryIntervalMs?: number;
  /**
   * Whether to enable the safety recovery sweeper.
   * Default: true.
   */
  enableRecovery?: boolean;
  /**
   * Maximum time in milliseconds to wait for active jobs to finish on graceful shutdown.
   * Default: 10000 (10 seconds).
   */
  shutdownTimeoutMs?: number;
  /**
   * Default maximum retry attempts for registered jobs.
   * Default: 3.
   */
  defaultMaxAttempts?: number;
  /**
   * Default base backoff time in milliseconds for retries.
   * Default: 5000 (5 seconds).
   */
  defaultBackoffMs?: number;
  /**
   * Default exponential backoff multiplier for retries.
   * Default: 2.
   */
  defaultBackoffFactor?: number;
}

export type JobHandler<T = any> = (job: JobInfo<T>) => Promise<any> | any;

export interface JobInfo<T = any> {
  id: string;
  name: string;
  data: T;
  attempts: number;
  maxAttempts: number;
  runAt: Date;
  createdAt: Date;
}

export interface MongoSnoozeEvents {
  "job:scheduled": (job: JobDocument) => void;
  "job:started": (job: JobInfo) => void;
  "job:completed": (job: JobInfo, result: any) => void;
  "job:failed": (job: JobInfo, error: Error) => void;
  "job:retrying": (job: JobInfo, error: Error, nextRunAt: Date) => void;
  "job:cancelled": (jobId: string) => void;
  "job:rescheduled": (jobId: string, newRunAt: Date) => void;
  "recovery:sweep": (stats: { recoveredLeases: number; recoveredPending: number }) => void;
  error: (error: Error) => void;
}
