CREATE TABLE tasks(
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
	step_name TEXT NOT NULL,
	status TEXT NOT NULL CHECK(
		status IN ('pending', 'queued', 'running', 'completed', 'failed', 'dead_letter')
	),
	retry_count INT NOT NULL DEFAULT 0,
	max_retries INT NOT NULL DEFAULT 3,
	scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	started_at TIMESTAMPTZ,
	completed_at TIMESTAMPTZ,
	last_heartbeat TIMESTAMPTZ,
	worker_id TEXT,
	redis_message_id TEXT,
	input JSONB,
	result JSONB,
	error TEXT,
	idempotency_key TEXT UNIQUE,
	depends_on UUID[] NOT NULL DEFAULT '{}',
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tasks_pending
	ON tasks(scheduled_at)
	WHERE status = 'pending';

CREATE INDEX idx_tasks_running_heartbeat
	ON tasks(last_heartbeat)
	WHERE status = 'running';

CREATE INDEX idx_tasks_workflow_id
	ON tasks(workflow_id);
