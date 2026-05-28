CREATE TABLE IF NOT EXISTS roost_work_items (
  work_id TEXT PRIMARY KEY,
  engine TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  priority INTEGER NOT NULL DEFAULT 0,
  resources TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deadline_at TIMESTAMPTZ,
  idempotency_key TEXT UNIQUE,
  raw JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS roost_work_items_engine_idx
  ON roost_work_items (engine);

CREATE INDEX IF NOT EXISTS roost_work_items_created_at_idx
  ON roost_work_items (created_at DESC);

CREATE TABLE IF NOT EXISTS roost_work_meta (
  work_id TEXT PRIMARY KEY REFERENCES roost_work_items (work_id) ON DELETE CASCADE,
  engine TEXT NOT NULL,
  state TEXT NOT NULL,
  step TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error JSONB
);

CREATE INDEX IF NOT EXISTS roost_work_meta_state_updated_idx
  ON roost_work_meta (state, updated_at DESC);

CREATE INDEX IF NOT EXISTS roost_work_meta_engine_state_idx
  ON roost_work_meta (engine, state);

CREATE TABLE IF NOT EXISTS roost_snapshots (
  work_id TEXT PRIMARY KEY REFERENCES roost_work_items (work_id) ON DELETE CASCADE,
  engine TEXT NOT NULL,
  version INTEGER NOT NULL,
  status TEXT NOT NULL,
  step TEXT NOT NULL,
  data JSONB NOT NULL DEFAULT '{}'::jsonb,
  history JSONB NOT NULL DEFAULT '[]'::jsonb,
  artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
  is_finished BOOLEAN NOT NULL DEFAULT false,
  next_step_delay_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  failed_at TIMESTAMPTZ,
  raw JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS roost_snapshots_engine_updated_idx
  ON roost_snapshots (engine, updated_at DESC);

CREATE TABLE IF NOT EXISTS roost_artifacts (
  artifact_id TEXT PRIMARY KEY,
  work_id TEXT NOT NULL REFERENCES roost_work_items (work_id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  uri TEXT,
  content_hash TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  raw JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS roost_artifacts_work_idx
  ON roost_artifacts (work_id, created_at DESC);

CREATE TABLE IF NOT EXISTS roost_leases (
  work_id TEXT PRIMARY KEY REFERENCES roost_work_items (work_id) ON DELETE CASCADE,
  holder_id TEXT NOT NULL,
  lease_id TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS roost_leases_expires_at_idx
  ON roost_leases (expires_at);

CREATE TABLE IF NOT EXISTS roost_resource_claims (
  resource_key TEXT PRIMARY KEY,
  owner_value TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS roost_resource_claims_owner_idx
  ON roost_resource_claims (owner_value);

CREATE INDEX IF NOT EXISTS roost_resource_claims_expires_at_idx
  ON roost_resource_claims (expires_at);

CREATE TABLE IF NOT EXISTS roost_events (
  id BIGSERIAL PRIMARY KEY,
  event_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  kind TEXT,
  work_id TEXT,
  engine TEXT,
  payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS roost_events_ts_idx
  ON roost_events (event_ts DESC);

CREATE INDEX IF NOT EXISTS roost_events_work_ts_idx
  ON roost_events (work_id, event_ts DESC);

CREATE INDEX IF NOT EXISTS roost_events_kind_ts_idx
  ON roost_events (kind, event_ts DESC);

CREATE TABLE IF NOT EXISTS roost_dlq (
  id BIGSERIAL PRIMARY KEY,
  work_id TEXT,
  engine TEXT,
  step TEXT,
  last_error JSONB,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  acked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS roost_dlq_active_idx
  ON roost_dlq (created_at DESC)
  WHERE acked_at IS NULL;

CREATE TABLE IF NOT EXISTS roost_worker_heartbeats (
  worker_id TEXT PRIMARY KEY,
  engine_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  queue_name TEXT,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS roost_operator_actions (
  id BIGSERIAL PRIMARY KEY,
  action_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  action TEXT NOT NULL,
  work_id TEXT,
  actor TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS roost_operator_actions_work_ts_idx
  ON roost_operator_actions (work_id, action_ts DESC);
