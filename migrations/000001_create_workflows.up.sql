CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE workflows(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  status TEXT NOT NULL CHECK(
    status IN ('pending', 'running', 'completed', 'failed', 'cancelled')
  ),
  definition JSONB NOT NULL,
  input JSONB,
  result JSONB,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  version INT NOT NULL DEFAULT 1
);

CREATE INDEX idx_workflows_status
  ON workflows(status)
  WHERE status IN ('pending', 'running');