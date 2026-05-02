# Distributed Job Queue + Workflow Engine

## Project mission
Build a Temporal-lite distributed job queue and workflow engine that demonstrates durable task execution, Redis Streams worker orchestration, Postgres-backed state persistence, and failure-first distributed-system design.

This is a portfolio-grade systems project. Prioritize correctness, clear architecture, testability, and explainable tradeoffs over adding features quickly.

## Core architecture rules
- PostgreSQL is the source of truth.
- Redis Streams is the dispatch queue, not the durable state store.
- Redis messages should carry pointers such as `task_id`, `workflow_id`, `step_name`, and `attempt`; full task state belongs in Postgres.
- Every important state transition must be persisted in Postgres.
- Workers should be stateless and horizontally scalable.
- Prefer at-least-once delivery plus idempotent task execution over pretending to provide exactly-once execution.
- Keep Week 1 intentionally simple: no DAG engine, no retry logic, no sweeper, no DLQ, no metrics dashboard yet.

## Recommended stack
- Language: Go
- HTTP router: chi
- Postgres driver: pgx/v5 with pgxpool
- Redis client: go-redis/v9
- IDs: github.com/google/uuid or Postgres-generated UUIDs
- Logging: standard library `log/slog`
- Local infrastructure: Docker Compose with Postgres and Redis
- Migrations: SQL files in `migrations/`; add golang-migrate later if not wired on day one

## Week 1 objective
Prove the complete happy-path loop:

1. Submit a workflow through `POST /workflows`.
2. API persists one workflow row and one initial task row in Postgres.
3. Dispatcher polls Postgres for `pending` tasks.
4. Dispatcher pushes the task pointer to Redis Streams with `XADD`.
5. Dispatcher marks the task as `queued` and stores the Redis message ID.
6. Worker consumes via `XREADGROUP`.
7. Worker claims the task by changing it from `queued` to `running`.
8. Worker executes a hardcoded fake task.
9. Worker marks the task `completed` in Postgres.
10. Worker ACKs the Redis message with `XACK`.
11. `GET /workflows/{id}` shows the workflow and task status.

Do not build resilience features until this happy path works end-to-end.

## Week 1 implementation boundaries
Implement:
- Go module and project structure
- Docker Compose for Postgres and Redis
- Basic config loading from environment variables
- SQL migrations for `workflows`, `tasks`, and `workflow_events`
- API endpoints:
  - `POST /workflows`
  - `GET /workflows/{id}`
  - `GET /healthz`
- Dispatcher loop
- Redis Streams consumer group initialization
- Single worker loop
- Hardcoded task executor, for example `echo` or `sleep_then_complete`
- Basic structured logs
- Manual curl-based end-to-end test

Do not implement yet:
- Retry/backoff
- Heartbeats
- Sweeper
- `XAUTOCLAIM`
- Dead letter queue
- DAG dependency scheduling beyond a single initial task
- Prometheus/Grafana
- Load tests
- Kubernetes or cloud deployment

## Expected repository shape
```text
job-queue/
├── cmd/
│   ├── api/main.go
│   ├── dispatcher/main.go
│   └── worker/main.go
├── internal/
│   ├── api/
│   ├── config/
│   ├── dispatcher/
│   ├── queue/
│   ├── store/
│   ├── task/
│   └── worker/
├── migrations/
├── deployments/
│   └── docker-compose.yml
├── examples/
├── docs/
├── scripts/
├── .claude/
│   ├── settings.json
│   └── skills/
├── Makefile
├── go.mod
└── README.md
```

## Data model rules
Use these statuses in Week 1:
- Workflow status: `pending`, `running`, `completed`, `failed`, `cancelled`
- Task status: `pending`, `queued`, `running`, `completed`, `failed`, `dead_letter`

Minimum Week 1 tables:
- `workflows`
- `tasks`
- `workflow_events`

Important task fields:
- `id`
- `workflow_id`
- `step_name`
- `status`
- `retry_count`
- `max_retries`
- `scheduled_at`
- `started_at`
- `completed_at`
- `last_heartbeat`
- `worker_id`
- `redis_message_id`
- `input`
- `result`
- `error`
- `idempotency_key`
- `depends_on`
- `created_at`

## Redis rules
- Main stream key: `tasks:stream`
- Consumer group: `workers`
- DLQ stream key reserved for later: `tasks:dlq`
- Create the consumer group idempotently at worker startup.
- Use `XREADGROUP GROUP workers <consumer_id> COUNT 10 BLOCK 5000 STREAMS tasks:stream >`.
- On success, call `XACK tasks:stream workers <message_id>` only after Postgres has recorded completion.

## Dispatcher rules
- Poll Postgres for ready tasks with `status = 'pending'` and `scheduled_at <= NOW()`.
- Use `FOR UPDATE SKIP LOCKED` when claiming ready tasks.
- Dispatch in small batches.
- Push to Redis with `XADD`.
- Mark the task `queued` and store `redis_message_id`.
- For Week 1, a simple transaction around selection and status update is enough. Later, harden this with a transactional outbox pattern.

## Worker rules
- Read from Redis Streams with a unique consumer ID.
- For each message, load the task from Postgres.
- Claim the task with an atomic update guarded by `WHERE status = 'queued'`.
- If the claim returns zero rows, ACK and move on because another process or state transition already handled it.
- Execute a hardcoded fake task in Week 1.
- On success, update task status to `completed`, store a JSON result, and insert a `task_completed` event.
- ACK Redis only after Postgres commit succeeds.

## Claude Code workflow rules
- Start with Sonnet for normal implementation.
- Use Plan Mode for schema changes, dispatcher logic, worker claim logic, or any multi-file refactor.
- Keep diffs small and checkpoint frequently.
- After each implementation step, run the narrowest verification command first.
- Always summarize:
  - what changed
  - what command was run
  - what passed or failed
  - what risk remains

## Safety rules
- Never read `.env`, `.env.*`, or `secrets/`.
- Do not print secrets in logs.
- Ask before adding major dependencies.
- Ask before changing project architecture beyond the current phase.
- Do not skip verification unless explicitly instructed.

## Useful commands
Prefer adding these to the `Makefile` and using them consistently:

```bash
make up          # start local Postgres and Redis
make down        # stop local services
make migrate-up  # apply SQL migrations
make api         # run API server
make dispatcher  # run dispatcher
make worker      # run worker
make test        # run Go tests
make fmt         # gofmt all files
make vet         # go vet ./...
make e2e         # run manual/local smoke test script later
```

## First implementation prompt for Claude Code
Use this prompt when starting the repo:

```text
Use Plan Mode first.

I am starting Week 1 of this distributed job queue + workflow engine project. Read CLAUDE.md and ARCHITECTURE.md if present.

Goal for Week 1: prove the happy-path loop only: POST /workflows -> Postgres workflow/task rows -> dispatcher XADD to Redis -> worker XREADGROUP -> hardcoded task execution -> Postgres completed -> Redis XACK -> GET /workflows/{id} shows completion.

Do not implement retries, heartbeats, sweeper, DLQ, DAG branching, Prometheus, Grafana, or load testing yet.

First, inspect the repo and propose the smallest file structure and implementation sequence. After I approve the plan, scaffold the Go module, Docker Compose, migrations, config, and Makefile.
```
