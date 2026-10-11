# MongoSnooze: Architecture & Design Decisions

This document details the architectural reasoning, tradeoffs, failure modes, and concurrency mechanics of **MongoSnooze**.

---

## 🎯 The Core Problem

Node.js web applications frequently require delayed execution:
- Sending a reminder email 24 hours after signup.
- Releasing an inventory lock if checkout isn't completed in 15 minutes.
- Retrying an external webhook with exponential backoff.
- Expiring subscription trials.

For these sparse-to-moderate workloads, developers face two unappealing extremes:
1. **Operating Redis (BullMQ / Celery):** Adds operational overhead, extra hosting costs, connection pool limits, and disaster recovery complexity when MongoDB is already the primary datastore.
2. **Continuous Polling (Agenda / custom loops):** Regularly queries the primary MongoDB cluster (`db.jobs.find({ runAt: { $lte: now } })`) every few seconds. On sparse workloads, this generates continuous read noise, cache churn, and IOPS waste while idle.

---

## 💡 The Naive TTL Idea & Why It Fails

A common developer intuition is:
> *"Why not insert a document with a TTL index on `runAt`, and listen for delete events using MongoDB Change Streams?"*

```
[Schedule Job] ──> Insert into TTL collection with expireAt
                       ↓
                   MongoDB TTL thread deletes document
                       ↓
                   Change Stream receives 'delete' event
                       ↓
                   Execute handler!
```

While elegant, this naive model has a **fatal failure mode**:
1. **Oplog Rolloff & Worker Downtime:** If your server or worker restarts or goes down for a few hours, MongoDB continues deleting expired documents. Once those events drop out of the MongoDB oplog window, **the job payload is permanently destroyed**. It can never be recovered because the document was deleted and MongoDB pre-images may not exist or be retained.
2. **TTL Timing Skew:** MongoDB's background TTL thread runs every 60 seconds. It is asynchronous and best-effort. If load is high, deletions can lag further.

---

## 🏗️ The MongoSnooze 4-Layer Solution

MongoSnooze solves this by decoupling the **durable source of truth** from the **disposable trigger**:

```
                          ┌───────────────────────────┐
                          │   snooze.schedule(...)    │
                          └─────────────┬─────────────┘
                                        │
                 ┌──────────────────────┴──────────────────────┐
                 ▼                                             ▼
     ┌───────────────────────┐                     ┌───────────────────────┐
     │   {prefix}_jobs       │                     │  {prefix}_wakeups     │
     │  (Durable Job Store)  │                     │ (Ephemeral TTL Trigger│
     │                       │                     │  with expireAfter: 0) │
     └───────────▲───────────┘                     └───────────┬───────────┘
                 │                                             │
                 │                                  MongoDB TTL background
                 │                                  deletes trigger document
                 │                                             │
                 │                                             ▼
                 │                                 ┌───────────────────────┐
                 │                                 │ Change Stream Worker  │
                 │                                 │ (Fast Notification)   │
                 │                                 └───────────┬───────────┘
                 │                                             │
                 │   1. Atomic Lease Claim                     │
                 │      (findOneAndUpdate status/lease)        │
                 └─────────────────────────────────────────────┘
                                        │
                                        ▼
                             ┌─────────────────────┐
                             │   Handler Runs      │
                             │ (with Auto-Heartbeat│
                             │   & job.touch)      │
                             └──────────┬──────────┘
                                        │
                         ┌──────────────┴──────────────┐
                         ▼                             ▼
                 [Success / Completed]       [Failure / Retry]
                   Updates _jobs;             Calculates backoff;
                   Deletes _wakeups           Upserts new trigger in _wakeups
```

### Layer 1: Durable Source of Truth (`_jobs`)
- Persists all state: `pending`, `running`, `completed`, `failed`, `cancelled`.
- Records attempts, max attempts, backoff parameters, lock status, lease expirations, worker ID, execution results, and errors.
- Never deleted by TTL; remains available for audit history and recovery.

### Layer 2: Disposable TTL Trigger (`_wakeups`)
- Minimalist document: `{ _id: jobId, expireAt: Date }`.
- TTL index `{ expireAt: 1 }` with `expireAfterSeconds: 0`.
- Acts purely as an alarm clock bell. If it is lost or delayed, the payload in `_jobs` remains safe.

### Layer 3: Change Stream Fast Path & Atomic Leases
- Listens to `delete` operations on `{prefix}_wakeups`.
- Multi-worker coordination is achieved using atomic `findOneAndUpdate`:
  ```typescript
  await jobs.findOneAndUpdate(
    {
      _id: jobId,
      status: { $in: ["pending", "failed"] },
      runAt: { $lte: new Date() }
    },
    {
      $set: {
        status: "running",
        lockedBy: this.workerId,
        lockedAt: new Date(),
        leaseExpiresAt: new Date(Date.now() + this.leaseDurationMs)
      },
      $inc: { attempts: 1 }
    },
    { returnDocument: "after" }
  );
  ```
- If multiple workers receive the delete event, only **one** successfully claims the job.

### Layer 4: Safety Net Recovery Sweeper
- Runs on worker startup and periodically (configurable, e.g. every 30 seconds).
- Reconciles two critical scenarios:
  1. **Abandoned / Dead Workers:** If a worker crashes mid-task, its lease expires (`leaseExpiresAt < now`). The sweeper detects this and resets the job to `pending` (or marks it failed if max attempts was reached).
  2. **Missed Oplog Events:** If workers were down for longer than the oplog retention window, the sweeper picks up due pending jobs (`runAt <= now`) and executes them.

---

## 🛡️ Concurrency & Distributed Correctness

### 1. Auto-Heartbeat for Long Tasks
If a job's execution takes longer than `leaseDurationMs`, the recovery sweeper must not assume the worker died. MongoSnooze runs an automated background heartbeat every `leaseDurationMs / 2` to renew `leaseExpiresAt`. Handlers can also call `await job.touch(extraMs)` manually.

### 2. Guarded Job Completion
When a job finishes or fails, the status update asserts that the current worker still holds the lease:
```typescript
await jobs.updateOne(
  { _id: jobDoc._id, lockedBy: this.workerId, status: "running" },
  { $set: { status: "completed", result: ... } }
);
```
If another worker claimed the job during an unexpected pause, the update safely aborts and emits a `LeaseLostError`.

### 3. Thundering Herd Mitigation
When thousands of jobs expire simultaneously, MongoSnooze introduces a 0–20ms randomized jitter prior to claiming and deduplicates in-memory wakeups via `Set<string>`.

---

## ⚖️ Tradeoffs & Design Constraints

| Property | MongoSnooze | BullMQ | Agenda |
| :--- | :--- | :--- | :--- |
| **Datastore** | MongoDB only | Redis required | MongoDB only |
| **Idle Overhead** | Zero polling queries | Minimal | Periodic polling queries |
| **Timing Precision** | ~60s TTL granularity | Sub-second | Polling interval |
| **Best For** | Delayed jobs & timeouts | High-throughput queues | Established cron schedules |
