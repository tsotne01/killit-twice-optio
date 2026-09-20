# AGENTS.md: Autonomous Agent Operating Instructions

**System:** Kill It Twice — Fault-Tolerant Data Replication Pipeline  
**Target Platform:** Optio Platform Engineering  
**Standard:** Senior/Staff Engineer Architectural Guardrails  

---

## 1. Mission & System Overview

This repository houses a high-throughput, fault-tolerant replication pipeline that streams data from a primary PostgreSQL database into two distinct sinks:
1. **Elasticsearch** (Search & segmentation index)
2. **RabbitMQ** (Distributed event bus consumed by an independent audit worker)

Your task when modifying this codebase is to preserve resilience, high throughput, and strict fault tolerance across all crash scenarios.

---

## 2. Non-Negotiable Architectural Invariants

Whenever you propose, write, or refactor code in this repository, you **MUST strictly enforce** these invariants:

### 2.1. Memory Bounds (<= 512 MB RAM)
- **NEVER** buffer entire datasets into memory.
- **NEVER** issue unpaged queries like `SELECT * FROM customers`.
- All ingestion must flow through bounded batches (e.g., 500 records) or streaming backpressured cursors.

### 2.2. Keyset Pagination (NO OFFSET)
- `OFFSET / LIMIT` is **strictly prohibited** for backfill queries. On large datasets (1M+ rows), `OFFSET` degrades linearly ($O(N)$).
- Always use Keyset (Cursor) Pagination:
  ```sql
  SELECT * FROM customers WHERE id > :last_id ORDER BY id ASC LIMIT :batch_size;
  ```

### 2.3. Checkpoint Commit Semantics
- A checkpoint must **ONLY** be committed *after* the current batch is durably acknowledged by both Elasticsearch and RabbitMQ.
- If a crash occurs before checkpoint persistence, the batch will be re-processed upon restart. The sinks must remain idempotent.

### 2.4. Sink Idempotency & Conflict Resolution
- **Elasticsearch:** All documents must use `_id = customer.id` and `version_type=external_gte` using `customer.version`. If a stale backfill write attempts to overwrite an incremental update, the resulting HTTP `409` conflict must be caught, treated as benign, and discarded.
- **RabbitMQ:** Downstream consumers must be idempotent by keying messages on `event_id` or `record_id + version`.

### 2.5. Partial Batch Failure Isolation (G4)
- **NEVER** rollback or reject an entire 500-item batch if only a few items fail (e.g., mapping type errors in Elasticsearch).
- Inspect the Elasticsearch `_bulk` response (`errors: true`).
- Acknowledge and index all valid items.
- Route rejected items to the `replication_dlq` table with full error context (original payload, error message, timestamp, target sink).

### 2.6. Outage Resilience & Zero Busy-Looping (G3)
- If a sink (Elasticsearch or RabbitMQ) goes down, the pipeline must **NOT** crash and must **NOT** enter a CPU-saturating tight loop.
- Implement **Exponential Backoff with Jitter** ($1s, 2s, 4s, \dots, \max 30s$).
- Probe health with lightweight pings before resuming batch dispatch.

---

## 3. Directory Layout & File Conventions

Maintain strict separation of concerns across the codebase:

```
├── SPEC.md                  # The authoritative system design specification
├── AGENTS.md                # This agent guidelines document
├── README.md                # Project documentation, ADRs, and verification summary
├── Makefile                 # Standard CLI commands (make up, seed, verify)
├── docker-compose.yml       # Complete local infrastructure orchestration
├── docker/
│   ├── postgres/            # Schema DDL (01_init.sql) and seed configuration
│   └── elasticsearch/       # Index templates, mappings, and cluster settings
├── src/
│   ├── config/              # Environment and runtime configurations
│   ├── db/                  # PostgreSQL connection pool and query helpers
│   ├── checkpoint/          # Checkpoint persistence and resume state logic
│   ├── dlq/                 # Dead Letter Queue persistence and replay logic
│   ├── sinks/
│   │   ├── elasticsearch.ts # ES bulk ingestion and item-level error parser
│   │   └── rabbitmq.ts      # RabbitMQ topic publisher with publisher confirms
│   ├── pipeline/
│   │   ├── backfill.ts      # Keyset cursor backfill engine
│   │   ├── incremental.ts   # Watermark polling incremental engine
│   │   └── manager.ts       # Concurrent engine coordinator and circuit breaker
│   ├── consumer/            # Independent audit consumer listening to RabbitMQ
│   └── api/                 # Telemetry metrics (/api/metrics) and control server
├── ui/                      # Web dashboard (monitoring, data browser, chaos controls)
└── scripts/
    ├── seed.sh              # High-throughput data generation script (1M records)
    └── verify.sh            # Automated verification harness for Gates G1-G5
```

---

## 4. Protected Zones (Do Not Touch Without Approval)

Do **NOT** alter the following without explicit alignment:
1. **Schema of `replication_checkpoints` and `replication_dlq`**: Any changes break G1, G2, and G4 guarantees.
2. **Gate Criteria in `scripts/verify.sh`**: The verification assertions match the exact requirements of the Optio technical review.
3. **Delivery Semantics**: Do not attempt to replace At-Least-Once + Idempotency with Distributed 2-Phase Commits.

---

## 5. Coding & Observability Standards

- **TypeScript / Node.js:** Strict TypeScript typing (`strict: true`). No `any` types unless interfacing with raw untyped external payloads.
- **Structured Logging:** Log only structured JSON with standard fields: `timestamp`, `level`, `context` (e.g., `BackfillWorker`, `ESSink`), `message`, and relevant metadata (`batch_id`, `cursor`, `duration_ms`).
- **Error Handling:** Always distinguish between **transient errors** (network timeout, 503, connection refused) which require retry/backoff, and **fatal item errors** (400 mapping error) which route to DLQ.
- **Git Commit Messages:** Follow the Conventional Commits specification:
  - `feat(...)`: New features
  - `fix(...)`: Bug fixes
  - `test(...)`: Verification harness or tests
  - `docs(...)`: Documentation updates
  - `infra(...)`: Docker, Compose, and environment setups

---

## 6. Verification Workflow

Before considering any task complete, you must verify:
1. **Clean Lint and Build:** `npm run build` or equivalent without warnings.
2. **Docker Compose Health:** All services (`postgres`, `elasticsearch`, `rabbitmq`, `pipeline`) must report `healthy` in `docker compose ps`.
3. **Data Integrity:** `make seed` generates 1,000,000 rows without memory exhaustion.
4. **Gates Verification:** `make verify` runs all 5 gates and produces a clean `PASS` report.
