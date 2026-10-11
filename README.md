# 💤 MongoSnooze

> **Schedule it in MongoDB. Wake it when it's due.**  
> Lightweight, MongoDB-native delayed-job triggers using Change Streams and TTL wakeups. No Redis required.

[![npm version](https://img.shields.io/npm/v/mongosnooze.svg?style=flat-square)](https://www.npmjs.com/package/mongosnooze)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)
[![TypeScript](https://img.shields.io/badge/TypeScript-Ready-blue.svg?style=flat-square)](https://www.typescriptlang.org/)

---

## ⚡ The Problem

Node.js developers frequently need persistent delayed tasks (reminders, trial expiry emails, workflow timeouts, or cleanup jobs). However, existing solutions often force an architectural compromise:

- **Redis queues (e.g. BullMQ):** Powerful and rich, but requires running, maintaining, and paying for a separate Redis cluster.
- **Continuous polling schedulers (e.g. Agenda):** Can cause continuous query noise against your primary database even when idle.
- **In-memory timers (`setTimeout`):** Work disappears whenever your server restarts or scales.

**MongoSnooze** gives you durable, delayed-job execution using only your existing MongoDB database. It combines MongoDB TTL indexes for fast, event-driven Change Stream notifications with a durable job record and safety-net recovery.

---

## 🏗️ Architecture: The 4-Layer Engine

```
 [Application] ── schedule(job, runAt)
      │
      ├──> 1. {prefix}_jobs (Durable Source of Truth)
      │       Stores payload, attempts, lease status, and result history.
      │
      └──> 2. {prefix}_wakeups (Disposable TTL Trigger)
              MongoDB TTL index automatically expires & deletes document.
                   │
                   ▼ (Delete Event)
           3. Change Stream Worker (Fast Notification Path)
              Atomically claims job lease via findOneAndUpdate.
              Executes handler & marks job completed.
                   │
                   ▲
           4. Recovery Sweeper (Correctness Safety Net)
              Periodic lightweight sweep reconciles expired worker leases
              and catches jobs missed during prolonged worker downtime.
```

### Why this design?
Relying solely on a TTL delete event is dangerous: if workers go offline and MongoDB oplog rolls over, the payload is permanently lost. MongoSnooze separates the **durable job record** (`_jobs`) from the **disposable trigger** (`_wakeups`). Even if an oplog event is missed, the periodic recovery sweeper guarantees your job runs.

---

## 🥊 Where MongoSnooze Fits

| Tool | Best Fit | Infrastructure | Idle Polling |
| :--- | :--- | :--- | :--- |
| **MongoSnooze** | **Lightweight delayed triggers, reminders, state transitions** | **MongoDB only** | **No** (Change Stream triggered) |
| **BullMQ** | Heavyweight message queues, complex parent-child flows, high throughput | Redis required | Depends on configuration |
| **Agenda** | Established MongoDB-backed cron and recurring scheduling | MongoDB only | Periodic polling queries |
| `setTimeout` | Ephemeral, non-critical in-memory timers | None | None (lost on restart) |

---

## ⚠️ Important Requirements & Constraints

1. **MongoDB Replica Set or Atlas Required:**  
   MongoDB Change Streams require a replica set or sharded cluster. MongoSnooze will not work on a standalone `mongod` instance without replica set support.
2. **Timing Accuracy:**  
   MongoDB's background TTL monitor runs once every 60 seconds. Therefore, MongoSnooze is designed for delayed tasks, reminders, and timeouts, not sub-second real-time precision.
3. **At-Least-Once Delivery:**  
   If a worker crashes halfway through sending an email before marking the job complete, the recovery engine will re-claim the job. **Handlers should be designed to be idempotent.**

---

## 🚀 Installation

```bash
npm install mongosnooze mongodb
```

> `mongodb` (>= 5.0.0) is a peer dependency.

---

## 💻 Quickstart

```typescript
import { MongoClient } from "mongodb";
import { MongoSnooze } from "mongosnooze";

const client = new MongoClient("mongodb://localhost:27017/?replicaSet=rs0");
await client.connect();

const snooze = new MongoSnooze({
  db: client.db("myapp"),
  collectionPrefix: "snooze",
});

// 1. Define your job handlers
snooze.define<{ userId: string }>("send-welcome-email", async (job) => {
  console.log(`Processing email for user ${job.data.userId} (attempt ${job.attempts})`);
  // Do your async work here
  return { status: "sent" };
});

// 2. Start the worker
await snooze.start();

// 3. Schedule a delayed job
const jobId = await snooze.schedule(
  "send-welcome-email",
  { userId: "usr_123" },
  { delayMs: 60_000 } // Runs in 1 minute
);

// Or schedule for a specific Date:
// await snooze.schedule("send-welcome-email", { userId: "usr_123" }, { runAt: new Date("2026-10-15T10:00:00Z") });
```

---

## 📖 API Reference

### `new MongoSnooze(options)`

- `db: Db` *(Required)*: MongoDB database instance.
- `collectionPrefix?: string`: Prefix for collections (default: `'mongosnooze'`).
- `workerId?: string`: Identifier for the worker instance (default: random UUID).
- `concurrency?: number`: Max jobs processed simultaneously by this worker (default: `10`).
- `leaseDurationMs?: number`: Lock duration in ms before considering a worker crashed (default: `30000`).
- `recoveryIntervalMs?: number`: Interval in ms between safety sweeps (default: `30000`).
- `enableRecovery?: boolean`: Whether to run recovery sweeps (default: `true`).
- `shutdownTimeoutMs?: number`: Graceful shutdown timeout in ms (default: `10000`).
- `defaultMaxAttempts?: number`: Default max retry attempts (default: `3`).
- `defaultBackoffMs?: number`: Initial retry delay in ms (default: `5000`).
- `defaultBackoffFactor?: number`: Exponential backoff multiplier (default: `2`).

### `snooze.define<T>(name, handler)`
Registers a handler function for job `name`.
```typescript
snooze.define<{ orderId: string }>("order-timeout", async (job) => {
  console.log(job.id, job.name, job.data, job.attempts);
});
```

### `snooze.schedule<T>(name, data, options?)`
Schedules a job. Returns the generated or specified `jobId`.
Options:
- `runAt?: Date`: The exact time to execute.
- `delayMs?: number`: Milliseconds from now.
- `jobId?: string`: Custom unique job ID.
- `maxAttempts?: number`: Overrides default max retries.
- `backoffMs?: number`: Overrides retry base backoff.

### `snooze.cancel(jobId)`
Cancels a pending or failed job. If the TTL event later fires, it is safely ignored.

### `snooze.reschedule(jobId, newRunAt)`
Updates the schedule time for an existing job and updates the TTL wakeup trigger.

### `snooze.getJob(jobId)`
Fetches the durable job record from MongoDB, including execution status, attempts, errors, or results.

### `snooze.start()`
Initializes necessary MongoDB indexes, opens the Change Stream, and starts recovery sweeper.

### `snooze.stop()`
Gracefully stops the worker: closes change streams, awaits active jobs up to `shutdownTimeoutMs`, and releases remaining locks.

---

## 📡 Events

MongoSnooze extends `EventEmitter`:

```typescript
snooze.on("job:scheduled", (job) => { ... });
snooze.on("job:started", (job) => { ... });
snooze.on("job:completed", (job, result) => { ... });
snooze.on("job:failed", (job, error) => { ... });
snooze.on("job:retrying", (job, error, nextRunAt) => { ... });
snooze.on("job:cancelled", (jobId) => { ... });
snooze.on("recovery:sweep", (stats) => { ... });
snooze.on("error", (err) => { ... });
```

---

## 🛡️ License

MIT © [MongoSnooze Contributors](LICENSE)
