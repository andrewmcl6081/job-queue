# Distributed Job Queue + Workflow Engine

A Temporal-lite system: a reliable, fault-tolerant brain that manages and executes work across many machines without losing track of anything.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Core Concepts](#core-concepts)
3. [Architecture](#architecture)
4. [Tech Stack](#tech-stack)
5. [Database Schema](#database-schema)
6. [Redis Streams Design](#redis-streams-design)
7. [Service Communication](#service-communication)
8. [Implementation Roadmap](#implementation-roadmap)
9. [Deployment](#deployment)
10. [Project Structure](#project-structure)
11. [Key Algorithms & Patterns](#key-algorithms--patterns)
12. [Testing Strategy](#testing-strategy)
13. [Tradeoffs & Design Decisions](#tradeoffs--design-decisions)
14. [Stretch Goals](#stretch-goals)
15. [Interview Talking Points](#interview-talking-points)

---

## Project Overview

### What This System Does

Reliably runs background work across many machines, surviving crashes, failures, and scale.

**Example use case:**
1. User uploads a file
2. System processes it
3. Stores results
4. Sends a notification

That's a workflow — a sequence of steps. The challenge: server crashes halfway through, a step fails randomly, you need to retry safely, and thousands of these run at once. A normal backend falls apart here. This system doesn't.

### Why It Matters

Most developers build REST APIs and CRUD apps. Very few build resilient distributed systems. This project demonstrates:

- **Failure-first design** — systems WILL crash; we plan for it
- **State persistence** — nothing important lives only in memory
- **Idempotent operations** — retries don't break things
- **Horizontal scaling** — coordination across many workers

---

## Core Concepts

### 1. Task Durability
"If the system crashes, the task is not lost."

Every task is persisted to Postgres before it's enqueued in Redis. Postgres is the source of truth; Redis is a fast dispatch mechanism. If Redis loses data, we can replay from Postgres. Status transitions are persisted: `pending → queued → running → completed` (or `failed`).

### 2. Retry Policies
"If something fails, try again intelligently."

Define max attempts and a delay strategy. Use **exponential backoff**:
```
retry_count += 1
wait_time = base * 2^retry_count
```
Example: retry 3 times, waiting 1s → 5s → 30s. Prevents overwhelming a failing downstream system.

### 3. Idempotency (CRITICAL)
"Running the same task twice should not break things."

Retries can cause duplicates. Bad: charging a credit card twice. Good: use unique operation IDs and check if already processed before executing. With Redis Streams + at-least-once delivery, idempotency is non-negotiable.

### 4. Worker Orchestration
"Many workers process jobs in parallel safely."

Workers consume from a Redis Streams **consumer group**. Redis guarantees that each message is delivered to exactly one consumer in the group at a time, eliminating duplicate processing without any locking on our side.

### 5. Failure Recovery
"If a worker dies mid-task, someone else takes over."

Two layers of recovery:
- **Redis-level**: Pending Entries List (PEL) tracks messages a consumer claimed but didn't ACK. We use `XAUTOCLAIM` to reassign these.
- **Postgres-level**: Workers send heartbeats to Postgres. A sweeper detects stale heartbeats and re-enqueues tasks to Redis.

### 6. Workflows as DAGs
DAG = Directed Acyclic Graph. Steps with dependencies, no loops.

```
A → B → C
    ↓
    D
```

Define workflows declaratively. The engine tracks state in Postgres and dispatches ready steps to Redis as their dependencies complete.

### 7. Dead Letter Queue (DLQ)
"If a task fails too many times, move it aside."

Don't retry forever. After max retries, the task is marked `dead_letter` in Postgres and pushed to a separate Redis stream (`tasks:dlq`) for human inspection.

### 8. At-Least-Once + Idempotency
Exactly-once execution is extremely hard in distributed systems. Redis Streams provides at-least-once delivery via consumer group ACKs. We pair it with idempotent operations. This is what most production systems use.

---

## Architecture

### High-Level Component Diagram

```
                   ┌──────────────────┐
                   │  Client / App    │
                   │ Submits workflows│
                   └────────┬─────────┘
                            │ HTTP POST /workflows
                            ▼
                   ┌──────────────────┐
                   │   API Server     │
                   │ Validates,       │
                   │ persists workflow│
                   └────────┬─────────┘
                            │ INSERT (transactional)
                            ▼
                   ┌──────────────────────┐
                   │  PostgreSQL          │
                   │  SOURCE OF TRUTH     │
                   │ workflows, tasks,    │◄────────┐
                   │ events, retries      │         │
                   └────────────┬─────────┘         │ State updates
                                │                   │ + heartbeats
                                │ Read ready tasks  │
                                ▼                   │
                   ┌──────────────────────┐         │
                   │     Dispatcher       │         │
                   │ Pushes ready tasks   │         │
                   │ to Redis             │         │
                   └────────────┬─────────┘         │
                                │ XADD              │
                                ▼                   │
                   ┌──────────────────────┐         │
                   │   Redis Streams      │         │
                   │  tasks:stream        │         │
                   │  Consumer group      │         │
                   │  "workers"           │         │
                   └────────────┬─────────┘         │
                                │ XREADGROUP        │
                                ▼                   │
   ┌────────────────────────────────────────┐       │
   │  Worker Pool (stateless, scales out)   │       │
   │  ┌────────┐ ┌────────┐ ┌────────┐ ...  │───────┘
   │  │Worker 1│ │Worker 2│ │Worker 3│      │
   │  └────────┘ └────────┘ └────────┘      │
   │   XACK on success / XAUTOCLAIM stale   │
   └─────────────────────┬──────────────────┘
                         │
                         │ Sidecar systems
                         ▼
   ┌─────────────────┐ ┌──────────────────┐ ┌──────────────────┐
   │ Prometheus +    │ │ DLQ stream:      │ │ Heartbeat        │
   │ Grafana metrics │ │ tasks:dlq        │ │ Monitor / Sweeper│
   └─────────────────┘ └──────────────────┘ └──────────────────┘
```

### The Six Components

**1. Client / API Consumer**
Starts workflows. Hits `POST /workflows` with a workflow definition.

**2. API Server**
Validates input, persists workflow + initial tasks to Postgres in a single transaction, returns workflow ID immediately. Also serves status/inspection endpoints.

**3. Dispatcher**
A small service (or goroutine inside the API server) that watches Postgres for tasks ready to run and pushes them to the Redis stream via `XADD`. Handles task readiness based on DAG dependencies.

**4. Redis Streams (the queue)**
Holds pending work in `tasks:stream`. Workers consume via a consumer group named `workers`. Failed/exhausted tasks go to `tasks:dlq`.

**5. Worker Pool**
Stateless processes that consume from Redis, execute tasks, update Postgres state, and ACK Redis. Scale horizontally by adding more.

**6. Sweeper**
Background process that handles two recovery jobs:
- Reclaims stale Redis Streams messages via `XAUTOCLAIM`
- Detects tasks with stale Postgres heartbeats and re-enqueues them

### Critical Design Principle

**Postgres is the source of truth. Redis is the queue.** Every state transition is recorded in Postgres first. Redis only holds a pointer (the task ID) to work that needs doing. If Redis is wiped, we replay all `pending` and `running` tasks from Postgres back into the stream and the system recovers.

---

## Tech Stack

### Recommended: Go-based Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Language | **Go** | Built for concurrency, used by Temporal itself, goroutines map naturally to workers |
| API framework | **Chi** or **Gin** | Lightweight, idiomatic Go HTTP routing |
| Database | **PostgreSQL 15+** | Source of truth for state, transactions, and audit log |
| Queue | **Redis Streams (Redis 7+)** | Consumer groups, PEL-based recovery, `XAUTOCLAIM`, high throughput |
| Redis client | **go-redis/v9** | Mature, supports all stream commands |
| Postgres driver | **pgx/v5** | Best-in-class Go driver with native Postgres features |
| Migrations | **golang-migrate** | Standard Go migration tool |
| Observability | **Prometheus + Grafana** | Industry standard metrics |
| Logging | **zerolog** or **slog** | Structured JSON logs |
| Containerization | **Docker + docker-compose** | One command to spin up locally |
| Testing | **testify + testcontainers** | Integration tests against real Postgres + Redis |

### Alternative: Python Stack
If Go feels too unfamiliar, Python is acceptable but less impressive for this domain.

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| API framework | FastAPI |
| Async runtime | asyncio |
| Postgres driver | asyncpg or psycopg3 |
| Redis client | redis-py (with async support) |
| Migrations | Alembic |

**Recommendation: Choose Go.** It signals systems-engineering depth and is what real-world systems like Temporal, Kubernetes, and Docker are built in.

---

## Database Schema

The schema is the heart of the system. Get this right and everything else follows.

### Tables

#### `workflows`
Tracks high-level workflow instances.

```sql
CREATE TABLE workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    definition JSONB NOT NULL,
    input JSONB,
    result JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    version INT NOT NULL DEFAULT 1
);

CREATE INDEX idx_workflows_status ON workflows(status) WHERE status IN ('pending', 'running');
```

#### `tasks`
Individual steps within a workflow. The most important table.

```sql
CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'queued', 'running', 'completed', 'failed', 'dead_letter')),
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    worker_id TEXT,
    redis_message_id TEXT,        -- Redis stream entry ID for this task
    input JSONB,
    result JSONB,
    error TEXT,
    idempotency_key TEXT UNIQUE,
    depends_on UUID[] DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Critical index for the dispatcher's polling query
CREATE INDEX idx_tasks_pending ON tasks(scheduled_at)
    WHERE status = 'pending';

-- For finding stuck tasks
CREATE INDEX idx_tasks_running_heartbeat ON tasks(last_heartbeat)
    WHERE status = 'running';

-- For inspecting a workflow's tasks
CREATE INDEX idx_tasks_workflow_id ON tasks(workflow_id);
```

Note the `queued` status (between `pending` and `running`) — set by the dispatcher when the task is pushed to Redis but hasn't been claimed by a worker yet. Also note `redis_message_id` for correlation between Postgres and Redis.

#### `workflow_events`
Append-only audit log. Enables replay and debugging.

```sql
CREATE TABLE workflow_events (
    id BIGSERIAL PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_events_workflow_id ON workflow_events(workflow_id, id);
```

Event types: `workflow_started`, `task_scheduled`, `task_enqueued`, `task_started`, `task_completed`, `task_failed`, `task_retried`, `task_dead_lettered`, `workflow_completed`, `workflow_failed`.

#### `workers` (optional but useful)
Track active workers for diagnostics.

```sql
CREATE TABLE workers (
    id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'active'
);
```

---

## Redis Streams Design

### Streams

| Stream | Purpose |
|--------|---------|
| `tasks:stream` | Main work queue. Dispatcher writes here; workers read here. |
| `tasks:dlq` | Dead letter queue. Tasks that exhausted retries land here for inspection. |

### Consumer Groups

Single consumer group on `tasks:stream`:

```
XGROUP CREATE tasks:stream workers $ MKSTREAM
```

- Group name: `workers`
- Each worker process registers as a consumer with a unique ID (e.g., `worker-{hostname}-{pid}`)
- Redis distributes messages across consumers; each message goes to exactly one consumer

### Message Format

Each stream entry contains minimal data — just enough to look up the full task in Postgres:

```
XADD tasks:stream * task_id <uuid> workflow_id <uuid> step_name <name> attempt <n>
```

Why minimal? Postgres holds the canonical state. The Redis message is just a pointer. If we put the full payload in Redis and it diverged from Postgres (e.g., a retry with updated input), we'd have consistency bugs.

### Worker Consumption Pattern

```
XREADGROUP GROUP workers <consumer_id> COUNT 10 BLOCK 5000 STREAMS tasks:stream >
```

- `>` means "give me only new messages I haven't seen"
- `BLOCK 5000` means "wait up to 5 seconds if nothing is available" (long-polling)
- `COUNT 10` is the max messages per call (tune for throughput)

### Acknowledgment

On successful task completion, the worker ACKs:
```
XACK tasks:stream workers <message_id>
```

If the worker dies before ACKing, the message stays in the Pending Entries List (PEL).

### Recovery via XAUTOCLAIM

The sweeper periodically reclaims messages that have been pending too long (e.g., 60 seconds without progress):

```
XAUTOCLAIM tasks:stream workers sweeper 60000 0 COUNT 100
```

This atomically transfers ownership of stale messages to the sweeper consumer, which then either re-enqueues them (`XADD` fresh + `XACK` old) or hands them off to the heartbeat-based recovery in Postgres.

### Stream Trimming

Prevent unbounded growth with capped streams. When dispatching:

```
XADD tasks:stream MAXLEN ~ 100000 * task_id <uuid> ...
```

The `~` means approximate trimming for better performance. 100k entries is plenty given that completed entries are ACKed and trimmed naturally.

---

## Service Communication

### How Components Talk to Each Other

#### Client → API Server: HTTP/REST
```
POST   /workflows            → Submit a new workflow
GET    /workflows/:id        → Get workflow status
GET    /workflows/:id/tasks  → List tasks for a workflow
POST   /workflows/:id/cancel → Cancel a running workflow
GET    /workflows/dlq        → List dead-lettered tasks
POST   /tasks/:id/retry      → Manually retry a dead-lettered task
GET    /healthz              → Liveness check
GET    /metrics              → Prometheus metrics
```

#### API Server → Postgres: SQL transactions
Workflow submission is atomic:
```sql
BEGIN;
INSERT INTO workflows (...) VALUES (...);
INSERT INTO tasks (...) VALUES (...);  -- one row per initial step
INSERT INTO workflow_events (...) VALUES (...);
COMMIT;
```

The API server does NOT write directly to Redis. The dispatcher handles that, ensuring Postgres is always written first.

#### Dispatcher → Postgres: Find ready tasks
The dispatcher polls Postgres for tasks that are ready to run (dependencies satisfied, scheduled time reached):

```sql
SELECT id, workflow_id, step_name, input, retry_count
FROM tasks
WHERE status = 'pending'
  AND scheduled_at <= NOW()
  AND (depends_on = '{}' OR NOT EXISTS (
      SELECT 1 FROM tasks t2
      WHERE t2.id = ANY(tasks.depends_on)
        AND t2.status != 'completed'
  ))
ORDER BY scheduled_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 100;
```

The `SKIP LOCKED` clause lets multiple dispatcher instances run concurrently without contention.

#### Dispatcher → Redis: Push to stream
For each ready task:
```
XADD tasks:stream MAXLEN ~ 100000 * task_id <uuid> workflow_id <uuid> step_name <name> attempt <n>
```

Then update Postgres in the same logical operation:
```sql
UPDATE tasks SET status='queued', redis_message_id=$1 WHERE id=$2;
```

**Important:** Use a transactional outbox pattern (write to Postgres first, then Redis, with a status of `queued` so we can detect tasks that made it to Postgres but not to Redis on dispatcher restart).

#### Worker → Redis: Consume
```
XREADGROUP GROUP workers <consumer_id> COUNT 10 BLOCK 5000 STREAMS tasks:stream >
```

#### Worker → Postgres: Claim and update
On receiving a message, the worker reads the task from Postgres and atomically transitions its state:
```sql
UPDATE tasks
SET status='running', started_at=NOW(), worker_id=$1, last_heartbeat=NOW()
WHERE id=$2 AND status='queued'
RETURNING *;
```

If the UPDATE returns 0 rows, another worker beat us to it (rare race; ACK and move on) or the task was cancelled. The `WHERE status='queued'` guard is critical.

#### Worker → Postgres: Heartbeat (every 5s)
Background goroutine while task executes:
```sql
UPDATE tasks SET last_heartbeat=NOW() WHERE id=$1;
```

#### Worker → Postgres + Redis: Completion
On success:
```sql
-- Postgres: record completion + log event + maybe enqueue successor tasks
BEGIN;
UPDATE tasks SET status='completed', result=$1, completed_at=NOW() WHERE id=$2;
INSERT INTO workflow_events (...) VALUES (...);
-- (mark dependent tasks as ready, etc.)
COMMIT;
```
Then ACK Redis:
```
XACK tasks:stream workers <message_id>
```

On failure with retries left:
```sql
UPDATE tasks
SET status='pending',
    retry_count = retry_count + 1,
    scheduled_at = NOW() + interval '<computed backoff>',
    error = $1
WHERE id = $2;
```
Then ACK Redis (the dispatcher will re-enqueue when `scheduled_at` is reached).

On failure with no retries left:
```sql
UPDATE tasks SET status='dead_letter', error=$1 WHERE id=$2;
```
Push to DLQ stream:
```
XADD tasks:dlq * task_id <uuid> error <msg>
```
Then ACK the main stream.

#### Sweeper → Redis: XAUTOCLAIM stale messages
Every 30 seconds:
```
XAUTOCLAIM tasks:stream workers sweeper-1 60000 0 COUNT 100
```

For each reclaimed message, check Postgres:
- If the task is `completed`, just ACK (worker finished but didn't ACK before dying).
- If the task is `running` with stale heartbeat, transition it back to `pending`, ACK the old message, and let the dispatcher re-enqueue.

#### Sweeper → Postgres: Heartbeat-based recovery
A second safety net for tasks where Redis somehow lost the message:
```sql
UPDATE tasks
SET status = 'pending',
    scheduled_at = NOW(),
    worker_id = NULL,
    last_heartbeat = NULL,
    redis_message_id = NULL
WHERE status = 'running'
  AND last_heartbeat < NOW() - INTERVAL '60 seconds';
```

---

## Implementation Roadmap

Build in this order. Don't skip ahead — earlier phases create scaffolding for later ones.

### Week 1 — Skeleton (Prove the loop works)
- [ ] Set up Go project structure with go.mod
- [ ] Postgres schema + migrations
- [ ] Docker Compose: Postgres + Redis + app
- [ ] API server: `POST /workflows`, `GET /workflows/:id`
- [ ] Dispatcher: poll Postgres, `XADD` to Redis stream
- [ ] Single worker: `XREADGROUP`, execute hardcoded task, `XACK`
- [ ] Basic structured logging
- [ ] Manual end-to-end test: submit workflow, see it complete

**Goal:** Submit a workflow, watch the dispatcher push it to Redis, watch the worker consume and complete it. No retries. No failures handled. Just prove the loop works.

### Week 2 — Resilience (Survive failures)
- [ ] Retry logic with exponential backoff
- [ ] Heartbeat goroutine in workers
- [ ] Sweeper process: `XAUTOCLAIM` for Redis-level recovery
- [ ] Sweeper: heartbeat-timeout recovery for Postgres-level safety
- [ ] Dead Letter Queue: `tasks:dlq` stream + status + endpoint to list
- [ ] Run multiple workers, verify each message is processed exactly once (per-attempt)
- [ ] Integration tests using testcontainers (both Postgres and Redis)

**Goal:** Kill -9 a worker mid-task. Watch `XAUTOCLAIM` reassign it. Inject random failures. Watch retries succeed. Inject permanent failures. Watch tasks land in DLQ.

### Week 3 — Workflows as DAGs (Multi-step)
- [ ] Define workflows declaratively (JSON or Go code)
- [ ] Sequential steps (A → B → C)
- [ ] Parallel branches (A → [B, C] → D)
- [ ] Dispatcher logic: only enqueue tasks whose dependencies are satisfied
- [ ] Idempotency keys
- [ ] Pass results from one step as input to the next
- [ ] CLI tool to submit and inspect workflows

**Goal:** Define a 5-step workflow with branching, submit it, watch each step execute in correct order with correct dependencies.

### Week 4 — Observability + Polish (Production feel)
- [ ] Prometheus metrics: Redis stream length, consumer group lag, PEL size, task duration, retry counts, worker count
- [ ] Grafana dashboard with stream/queue panels
- [ ] Comprehensive README with architecture diagrams
- [ ] Load testing script (submit 10k workflows, measure throughput)
- [ ] Graceful shutdown for workers (drain in-flight tasks, then exit)
- [ ] Configuration via environment variables
- [ ] Stream trimming policy tuned

**Goal:** Recruiter clones the repo, runs `docker-compose up`, opens Grafana, and sees a live system processing thousands of workflows.

---

## Deployment

### Local Development

`docker-compose.yml` should bring up:
- Postgres
- Redis (with persistence enabled — `appendonly yes`)
- API server
- Dispatcher
- 3 worker instances
- Sweeper
- Prometheus
- Grafana

```bash
docker-compose up
# Visit http://localhost:8080 for API
# Visit http://localhost:3000 for Grafana
```

### Configuration via Environment Variables

```
DATABASE_URL=postgres://user:pass@localhost:5432/jobqueue
REDIS_URL=redis://localhost:6379
REDIS_STREAM_KEY=tasks:stream
REDIS_DLQ_KEY=tasks:dlq
REDIS_CONSUMER_GROUP=workers
REDIS_STREAM_MAXLEN=100000
LOG_LEVEL=info
WORKER_CONCURRENCY=5
WORKER_HEARTBEAT_INTERVAL=5s
WORKER_HEARTBEAT_TIMEOUT=60s
WORKER_BLOCK_MS=5000
WORKER_BATCH_SIZE=10
DISPATCHER_INTERVAL=1s
DISPATCHER_BATCH_SIZE=100
SWEEPER_INTERVAL=30s
SWEEPER_PEL_TIMEOUT_MS=60000
DEFAULT_MAX_RETRIES=3
RETRY_BASE_DELAY=1s
METRICS_PORT=9090
API_PORT=8080
```

### Redis Configuration Notes

Enable persistence so Redis survives restarts:
```
appendonly yes
appendfsync everysec
```

Even with persistence, treat Redis as ephemeral. The recovery design assumes Redis can be wiped and rebuilt from Postgres.

### Production Deployment (Conceptual)

For a real deployment, you'd want:
- Postgres with replication (or managed service like RDS)
- Redis with persistence + replication (or managed service like ElastiCache)
- Workers as Kubernetes deployments — scale by replica count
- Dispatcher as a Kubernetes deployment with `replicas: 2` (active/active is fine; `SKIP LOCKED` handles concurrency)
- Sweeper as a singleton (or use leader election if running multiple)
- API server behind a load balancer
- Prometheus scraping all components
- Logs shipped to centralized store (Loki, CloudWatch, etc.)

---

## Project Structure

```
job-queue/
├── cmd/
│   ├── api/          # API server entrypoint
│   │   └── main.go
│   ├── worker/       # Worker process entrypoint
│   │   └── main.go
│   ├── dispatcher/   # Dispatcher entrypoint
│   │   └── main.go
│   ├── sweeper/      # Sweeper entrypoint
│   │   └── main.go
│   └── cli/          # CLI tool for submitting/inspecting workflows
│       └── main.go
├── internal/
│   ├── api/          # HTTP handlers
│   ├── workflow/     # Workflow definition + DAG logic
│   ├── task/         # Task lifecycle, state transitions
│   ├── worker/       # Worker loop, heartbeat, executor
│   ├── dispatcher/   # Postgres → Redis push logic
│   ├── sweeper/      # XAUTOCLAIM + heartbeat recovery
│   ├── queue/        # Redis Streams abstraction
│   ├── store/        # Postgres data access layer
│   ├── retry/        # Backoff calculation
│   ├── metrics/      # Prometheus instrumentation
│   └── config/       # Environment variable parsing
├── migrations/       # SQL migrations (golang-migrate format)
│   ├── 001_create_workflows.up.sql
│   ├── 001_create_workflows.down.sql
│   └── ...
├── deployments/
│   ├── docker-compose.yml
│   ├── Dockerfile.api
│   ├── Dockerfile.worker
│   ├── prometheus.yml
│   └── grafana/
│       └── dashboards/
├── examples/         # Example workflow definitions
│   ├── order_processing.json
│   └── file_pipeline.json
├── scripts/
│   ├── load_test.go
│   └── inject_chaos.sh
├── docs/
│   └── ARCHITECTURE.md
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

---

## Key Algorithms & Patterns

### Exponential Backoff with Jitter

```go
func nextRetryDelay(attempt int, base time.Duration) time.Duration {
    // Exponential: base * 2^attempt
    delay := base * time.Duration(1<<uint(attempt))

    // Cap to prevent absurd waits
    maxDelay := 10 * time.Minute
    if delay > maxDelay {
        delay = maxDelay
    }

    // Add jitter (±25%) to prevent thundering herd
    jitter := time.Duration(rand.Int63n(int64(delay) / 2))
    return delay - delay/4 + jitter
}
```

### Dispatcher Loop

```go
func (d *Dispatcher) Run(ctx context.Context) error {
    ticker := time.NewTicker(d.interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return nil
        case <-ticker.C:
            if err := d.dispatchBatch(ctx); err != nil {
                log.Error().Err(err).Msg("dispatch batch failed")
            }
        }
    }
}

func (d *Dispatcher) dispatchBatch(ctx context.Context) error {
    tx, err := d.db.BeginTx(ctx, pgx.TxOptions{})
    if err != nil {
        return err
    }
    defer tx.Rollback(ctx)

    tasks, err := d.store.ClaimReadyTasks(ctx, tx, d.batchSize)
    if err != nil {
        return err
    }

    for _, task := range tasks {
        msgID, err := d.queue.Push(ctx, task)
        if err != nil {
            return err  // Tx rollback; tasks stay 'pending'
        }
        if err := d.store.MarkQueued(ctx, tx, task.ID, msgID); err != nil {
            return err
        }
    }

    return tx.Commit(ctx)
}
```

### Worker Consumption Loop

```go
func (w *Worker) Run(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return nil
        default:
        }

        msgs, err := w.queue.Read(ctx, w.consumerID, w.batchSize, w.blockTimeout)
        if err != nil {
            log.Error().Err(err).Msg("read failed")
            time.Sleep(1 * time.Second)
            continue
        }

        for _, msg := range msgs {
            w.execute(ctx, msg)
        }
    }
}
```

### Heartbeat Goroutine

```go
func (w *Worker) execute(ctx context.Context, msg *StreamMessage) {
    task, err := w.store.ClaimTask(ctx, msg.TaskID, w.ID)
    if err != nil {
        // Task already claimed/cancelled — ACK and move on
        w.queue.Ack(ctx, msg.ID)
        return
    }

    hbCtx, cancelHB := context.WithCancel(ctx)
    defer cancelHB()
    go w.heartbeat(hbCtx, task.ID)

    result, err := w.runStep(ctx, task)
    if err != nil {
        w.handleFailure(ctx, task, msg, err)
        return
    }
    w.handleSuccess(ctx, task, msg, result)
}

func (w *Worker) heartbeat(ctx context.Context, taskID uuid.UUID) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            w.store.UpdateHeartbeat(ctx, taskID)
        }
    }
}
```

### Idempotent Task Execution

```go
func (w *Worker) runStep(ctx context.Context, task *Task) (Result, error) {
    if task.IdempotencyKey != "" {
        if existing, err := w.store.FindCompletedByKey(ctx, task.IdempotencyKey); err == nil && existing != nil {
            log.Info().Str("key", task.IdempotencyKey).Msg("returning cached result")
            return existing.Result, nil
        }
    }
    return w.executor.Execute(ctx, task)
}
```

### Sweeper: XAUTOCLAIM Loop

```go
func (s *Sweeper) reclaimStale(ctx context.Context) error {
    // Reclaim messages pending > 60s
    msgs, _, err := s.queue.AutoClaim(ctx, s.consumerID, 60*time.Second, 100)
    if err != nil {
        return err
    }

    for _, msg := range msgs {
        task, err := s.store.GetTask(ctx, msg.TaskID)
        if err != nil {
            continue
        }

        switch task.Status {
        case "completed":
            // Worker finished but didn't ACK before dying
            s.queue.Ack(ctx, msg.ID)
        case "running":
            // Stale — reset to pending, dispatcher will re-enqueue
            s.store.ResetToPending(ctx, task.ID)
            s.queue.Ack(ctx, msg.ID)
        }
    }

    return nil
}
```

---

## Testing Strategy

### Unit Tests
- Backoff calculation
- Workflow DAG validation (cycle detection, dependency resolution)
- State machine transitions

### Integration Tests (testcontainers)
- Spin up real Postgres + Redis in containers
- Submit a workflow, verify it completes end-to-end
- Multiple workers + consumer group: verify each message is delivered exactly once
- Heartbeat timeout + `XAUTOCLAIM` reassignment
- Retry behavior with simulated failures
- DLQ behavior after max retries
- Dispatcher idempotency (don't double-enqueue if it crashes after `XADD` but before commit)

### Chaos Tests
- Kill -9 a worker mid-task → verify recovery via `XAUTOCLAIM`
- Wipe Redis entirely → verify recovery from Postgres heartbeat sweeper
- Disconnect Postgres briefly → verify reconnect
- Submit 10k workflows → measure throughput, latency p50/p99

### Load Test
A simple Go program that submits N workflows and measures:
- Submission throughput (workflows/sec)
- End-to-end latency
- Worker CPU/memory usage
- Redis stream length over time
- Postgres query latency

---

## Tradeoffs & Design Decisions

Be ready to defend these in an interview.

### Why Redis Streams over RabbitMQ/Kafka?
**Right-sized.** Streams give us consumer groups, ACK semantics, and PEL-based recovery — everything we need without the operational weight of Kafka. RabbitMQ would also work, but Streams' append-only log model maps cleanly to our task execution pattern, and Redis is faster to operate for small-to-medium scale.

### Why Postgres + Redis instead of just one?
**Different jobs.** Postgres is ACID-transactional storage with rich queries — perfect for state, audit logs, and DAG dependency resolution. Redis is a fast in-memory queue with consumer groups — perfect for low-latency dispatch. Forcing one to do both means either weak durability (Redis-only) or queue contention at scale (Postgres-only).

### Why a separate Dispatcher service?
**Separation of concerns.** The API server's job is to validate and persist. The dispatcher's job is to push work to the queue when it's ready (dependencies satisfied, scheduled time reached). Splitting them keeps the API responsive and lets us scale dispatch independently.

### At-least-once vs exactly-once?
**At-least-once + idempotency.** Redis Streams provides at-least-once via consumer group ACKs. Exactly-once across distributed components is essentially impossible without expensive coordination. Most production systems (Temporal, Sidekiq, Celery) use at-least-once and require user code to be idempotent.

### What if Redis loses data?
**Postgres is the source of truth.** If Redis is wiped, the heartbeat sweeper detects tasks stuck in `running` (no heartbeat) and `queued` (no Redis message), resets them to `pending`, and the dispatcher re-enqueues. We may double-execute a task, but idempotency keeps us safe.

### Why not use Redis as the source of truth?
Redis lacks transactional guarantees across multiple keys/operations and isn't built for complex queries (DAG dependency resolution needs joins). Mixing roles is how you get inconsistency bugs.

### Why not just use Temporal/Celery/Sidekiq?
For a real product, you absolutely should. Building this yourself is about *understanding what those systems do*. Be explicit about that in the README.

### Why Go over Python?
Go's concurrency primitives (goroutines, channels, contexts) map naturally to this problem. Single binary deployment. Better performance per worker. Industry standard for this domain.

### Stateless workers?
Yes. All state lives in Postgres. Redis only holds in-flight pointers. Workers are interchangeable — kill any one and another picks up its work via `XAUTOCLAIM`.

---

## Stretch Goals

These take the project from "solid" to "genuinely impressive."

### Workflow Versioning
What happens when you deploy v2 of a workflow definition while v1 instances are still running? Tag each workflow instance with a version, route to the correct executor.

### Workflow Replay
Using the `workflow_events` log, reconstruct exactly what happened in any past workflow. Essential for debugging.

### Scheduled / Cron Workflows
`POST /schedules` with a cron expression. A scheduler service inserts new workflow instances on the schedule.

### Per-Step Timeouts
Each step has a max execution time. If exceeded, the worker is killed and the task fails (or retries).

### Saga / Compensation
On workflow failure, run compensating actions for already-completed steps. (e.g., refund payment if shipment fails.)

### Workflow Cancellation Propagation
Cancel a running workflow → in-flight workers detect it via cooperative cancellation and stop.

### Rate Limiting per Task Type
Use Redis to limit concurrency per step type (e.g., max 10 concurrent `send_email` tasks). A natural fit for Redis since it's already in the stack.

### Task Priority
Use multiple Redis streams (`tasks:stream:high`, `tasks:stream:normal`, `tasks:stream:low`) and have workers read from them in priority order.

### Stream Multiplexing by Task Type
Route different task types to different streams (e.g., `tasks:stream:cpu`, `tasks:stream:io`) and run specialized worker pools.

---

## Interview Talking Points

Practice answering these:

**Q: What happens if a worker crashes mid-task?**
A: Two recovery layers. First, the worker stops sending heartbeats to Postgres and stops ACKing the Redis message. After 60 seconds, the sweeper runs `XAUTOCLAIM`, reclaims the stale Redis message, checks Postgres, sees the task is `running` with a stale heartbeat, resets it to `pending`, and ACKs the old Redis entry. The dispatcher then re-enqueues. Second safety net: if Redis somehow lost the message entirely, the heartbeat-timeout sweeper still finds the stuck task in Postgres and re-enqueues it. Idempotency keys ensure the partial work doesn't cause duplicates.

**Q: How do you avoid duplicate execution?**
A: Two layers. First, Redis Streams consumer groups guarantee that each message is delivered to exactly one consumer at a time. Second, idempotency keys ensure that if a task is delivered more than once (due to retries or recovery), the underlying operation isn't repeated.

**Q: What if Redis goes down?**
A: Postgres is the source of truth. Tasks in flight are still recorded as `running` in Postgres with heartbeats. When Redis comes back, the heartbeat sweeper detects stuck tasks and the dispatcher re-enqueues them. We may double-execute some tasks during the failover window, but idempotency keeps us correct.

**Q: Why not just use Postgres as the queue too?**
A: It works at small scale but degrades. `SELECT FOR UPDATE SKIP LOCKED` is great until you hit thousands of TPS, at which point lock contention and table bloat become real. Redis Streams handles 100k+ msg/sec on a single node with no contention. We use the right tool for each job.

**Q: Why not just use cron jobs?**
A: Cron has no durability — if the machine crashes, the job is lost. No orchestration — can't model multi-step workflows with dependencies. No retries with backoff. No scaling — single machine. No observability into past runs.

**Q: How does this scale?**
A: Workers are stateless and horizontally scalable — add more processes; the consumer group automatically distributes load. Dispatchers also scale horizontally (Postgres `SKIP LOCKED` handles concurrency). Postgres scales vertically until ~10k TPS, at which point you'd add read replicas or partition the tasks table. Redis scales via Cluster mode if a single node isn't enough.

**Q: What's the bottleneck?**
A: Initially, dispatcher throughput — how fast we can move tasks from Postgres to Redis. We mitigate with batching and multiple dispatcher instances. Beyond that, the bottleneck is whatever the tasks themselves are doing (downstream APIs, database calls).

**Q: How would you add exactly-once semantics?**
A: True exactly-once is impossible without coordination. The practical answer: at-least-once delivery + idempotent operations + transactional outbox pattern for side effects. We can guarantee that effects happen *at most once* by storing operation IDs in the same transaction as the side effect.

**Q: Why Redis Streams over Pub/Sub?**
A: Pub/Sub is fire-and-forget — if no consumer is listening, the message is lost. Streams persist messages until ACKed and support consumer groups, replay, and PEL-based recovery. Streams are the right primitive for a job queue; Pub/Sub is for ephemeral broadcasting.

---

## Resume Bullet

> Built a fault-tolerant distributed workflow engine handling DAG-based job orchestration with at-least-once execution semantics, exponential backoff retries, heartbeat-based worker recovery, and dead-letter queuing. Stack: Go, PostgreSQL (source of truth), Redis Streams (consumer groups + `XAUTOCLAIM` recovery), Prometheus.

---

## Notes for Claude Code

When using this document as context:

1. **Start with the Week 1 roadmap.** Don't try to build everything at once.
2. **The database schema is canonical** — implement migrations exactly as specified before writing application code.
3. **Postgres is the source of truth, Redis is the queue.** Every state transition lands in Postgres first; Redis only carries pointers (task IDs).
4. **The dispatcher is non-trivial** — it must atomically read ready tasks from Postgres, push to Redis, and update Postgres status. Use `SELECT FOR UPDATE SKIP LOCKED` and a single transaction.
5. **Always write integration tests** with testcontainers for both Postgres and Redis. Unit tests alone won't catch concurrency bugs.
6. **Heartbeat + XAUTOCLAIM is the dual recovery contract.** Don't skip either — they cover different failure modes.
7. **Idempotency is a worker-side responsibility.** Every task executor function must handle being called twice with the same input gracefully.
8. **Logs should be structured JSON** with consistent fields: `workflow_id`, `task_id`, `worker_id`, `redis_message_id`, `event`. Correlating across Postgres and Redis is critical for debugging.
9. **Prefer explicit state transitions** in code (e.g., `MarkTaskQueued`, `MarkTaskRunning`, `MarkTaskCompleted`) over generic `UpdateTask` to make the state machine readable.
10. **Use go-redis/v9** for Redis access. Stream commands are well-supported: `XAdd`, `XReadGroup`, `XAck`, `XAutoClaim`, `XGroupCreate`.
