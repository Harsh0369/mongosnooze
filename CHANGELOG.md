# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] - 2026-10-11

### Added
- **4-Layer Engine**:
  - Durable job store (`{prefix}_jobs`) preserving state, attempts, locks, errors, and results.
  - Disposable TTL trigger collection (`{prefix}_wakeups`) with `expireAfterSeconds: 0`.
  - Fast-path MongoDB Change Stream worker consuming `delete` events.
  - Periodic & startup safety-net recovery sweeper to catch missed wakeups and expired leases.
- **Atomic Concurrency & Lease Safety**:
  - Multi-worker safe lease acquisition via atomic `findOneAndUpdate`.
  - Automatic background lease heartbeating during job execution.
  - Manual `job.touch(extraMs)` API to extend leases for long-running tasks.
  - Strict atomic completion verification preventing rogue workers from overwriting active locks.
- **Robustness & Error Resilience**:
  - Exponential backoff retry policies with automatic trigger rescheduling.
  - In-memory wakeup deduplication (`pendingWakeupSet`).
  - Claim jitter (0-20ms) to mitigate multi-worker thundering herds.
  - Safe `EventEmitter` error emission preventing Node.js process crashes.
  - Standalone MongoDB detection and clear `ReplicaSetRequiredError`.
  - Index conflict detection wrapping in actionable `IndexConflictError`.
- **Developer Experience**:
  - Full TypeScript type safety with strict types and autocomplete.
  - Dual CommonJS (`dist/index.cjs`) and ES Module (`dist/index.js`) support.
  - Comprehensive unit test suite with standalone mock harness.
  - Detailed architecture guide and example runnable script.
