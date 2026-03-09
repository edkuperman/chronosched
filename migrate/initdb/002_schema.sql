CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS cron_fires CASCADE;
DROP TABLE IF EXISTS cron_state CASCADE;
DROP TABLE IF EXISTS job_queue CASCADE;
DROP TABLE IF EXISTS job_frontier CASCADE;
DROP TABLE IF EXISTS job_dependencies CASCADE;
DROP TABLE IF EXISTS jobs CASCADE;
DROP TABLE IF EXISTS dag_runs CASCADE;
DROP TABLE IF EXISTS dag_version_edges CASCADE;
DROP TABLE IF EXISTS dag_version_nodes CASCADE;
DROP TABLE IF EXISTS dag_versions CASCADE;
DROP TABLE IF EXISTS dags CASCADE;
DROP TABLE IF EXISTS job_definitions CASCADE;
DROP TABLE IF EXISTS namespaces CASCADE;
DROP TYPE IF EXISTS run_status CASCADE;
DROP TYPE IF EXISTS job_status CASCADE;

CREATE TYPE run_status AS ENUM ('waiting','running','succeeded','failed','missed','cancelled');
CREATE TYPE job_status AS ENUM ('waiting','queued','dispatching','dispatched','running','succeeded','failed','lost','missed','cancelled','skipped');

CREATE TABLE namespaces (
  namespace_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE job_definitions (
  definition_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace_id UUID NOT NULL REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL DEFAULT 'cmd',
  payload_template JSONB NOT NULL DEFAULT '{}'::jsonb,
  schedule_type TEXT,
  cron_spec TEXT,
  interval_seconds INT,
  interval_start_at TIMESTAMPTZ,
  timezone TEXT,
  on_failure_policy TEXT NOT NULL DEFAULT 'continue',
  is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  is_paused BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(namespace_id, name),
  CONSTRAINT chk_job_definitions_schedule CHECK (
    schedule_type IS NULL
    OR (schedule_type = 'cron' AND cron_spec IS NOT NULL AND btrim(cron_spec) <> '' AND interval_seconds IS NULL AND interval_start_at IS NULL)
    OR (schedule_type = 'interval' AND interval_seconds IS NOT NULL AND interval_seconds > 0 AND interval_start_at IS NOT NULL AND (cron_spec IS NULL OR btrim(cron_spec) = ''))
  )
);

CREATE TABLE dags (
  dag_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace_id UUID NOT NULL REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  active_version_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(namespace_id, name)
);

CREATE TABLE dag_versions (
  dag_version_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dag_id UUID NOT NULL REFERENCES dags(dag_id) ON DELETE CASCADE,
  version_number INT NOT NULL,
  version_note TEXT NOT NULL DEFAULT '',
  based_on_version_id UUID REFERENCES dag_versions(dag_version_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(dag_id, version_number)
);

ALTER TABLE dags
ADD CONSTRAINT fk_dags_active_version
FOREIGN KEY (active_version_id) REFERENCES dag_versions(dag_version_id);

CREATE TABLE dag_version_nodes (
  node_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dag_version_id UUID NOT NULL REFERENCES dag_versions(dag_version_id) ON DELETE CASCADE,
  node_key TEXT NOT NULL,
  display_name TEXT NOT NULL,
  job_definition_id UUID NOT NULL REFERENCES job_definitions(definition_id),
  UNIQUE(dag_version_id, node_key)
);

CREATE TABLE dag_version_edges (
  edge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dag_version_id UUID NOT NULL REFERENCES dag_versions(dag_version_id) ON DELETE CASCADE,
  from_node_id UUID NOT NULL REFERENCES dag_version_nodes(node_id) ON DELETE CASCADE,
  to_node_id UUID NOT NULL REFERENCES dag_version_nodes(node_id) ON DELETE CASCADE,
  UNIQUE(dag_version_id, from_node_id, to_node_id)
);

CREATE TABLE dag_runs (
  run_id BIGSERIAL PRIMARY KEY,
  dag_id UUID NOT NULL REFERENCES dags(dag_id) ON DELETE CASCADE,
  dag_version_id UUID NOT NULL REFERENCES dag_versions(dag_version_id) ON DELETE RESTRICT,
  trigger_type TEXT NOT NULL,
  trigger_node_id UUID REFERENCES dag_version_nodes(node_id) ON DELETE RESTRICT,
  trigger_definition_id UUID REFERENCES job_definitions(definition_id),
  scheduled_at TIMESTAMPTZ NOT NULL,
  status run_status NOT NULL DEFAULT 'waiting',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);

CREATE TABLE jobs (
  job_id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES dag_runs(run_id) ON DELETE CASCADE,
  dag_version_node_id UUID NOT NULL REFERENCES dag_version_nodes(node_id) ON DELETE RESTRICT,
  job_definition_id UUID NOT NULL REFERENCES job_definitions(definition_id) ON DELETE RESTRICT,
  node_key TEXT NOT NULL,
  display_name TEXT NOT NULL,
  status job_status NOT NULL DEFAULT 'waiting',
  priority INT NOT NULL DEFAULT 0,
  due_at TIMESTAMPTZ NOT NULL,
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  lease_owner TEXT,
  lease_until TIMESTAMPTZ,
  dispatch_attempts INT NOT NULL DEFAULT 0,
  dispatched_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  last_heartbeat_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  external_execution_id TEXT,
  reason_code TEXT,
  reason_detail TEXT,
  last_error TEXT
);

CREATE TABLE job_dependencies (
  parent_job_id BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  child_job_id BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  PRIMARY KEY(parent_job_id, child_job_id)
);

CREATE TABLE job_frontier (
  job_id BIGINT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
  ready BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE job_queue (
  id BIGSERIAL PRIMARY KEY,
  job_id BIGINT NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE CASCADE,
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  priority INT NOT NULL DEFAULT 0,
  attempts INT NOT NULL DEFAULT 0,
  reserved_until TIMESTAMPTZ,
  consumer_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cron_state (
  node_id UUID PRIMARY KEY REFERENCES dag_version_nodes(node_id) ON DELETE CASCADE,
  next_run_at TIMESTAMPTZ
);

CREATE TABLE cron_fires (
  node_id UUID NOT NULL REFERENCES dag_version_nodes(node_id) ON DELETE CASCADE,
  scheduled_at TIMESTAMPTZ NOT NULL,
  run_id BIGINT NOT NULL REFERENCES dag_runs(run_id) ON DELETE CASCADE,
  PRIMARY KEY(node_id, scheduled_at)
);

CREATE INDEX idx_defs_namespace ON job_definitions(namespace_id, name);
CREATE INDEX idx_defs_cron ON job_definitions(cron_spec) WHERE COALESCE(schedule_type,'') IN ('', 'cron') AND cron_spec IS NOT NULL AND btrim(cron_spec) <> '';
CREATE INDEX idx_defs_interval ON job_definitions(interval_start_at, interval_seconds) WHERE schedule_type='interval' AND interval_seconds IS NOT NULL AND interval_start_at IS NOT NULL;
CREATE INDEX idx_dags_namespace ON dags(namespace_id, name);
CREATE INDEX idx_dag_versions_dag ON dag_versions(dag_id, version_number DESC);
CREATE INDEX idx_dag_nodes_version ON dag_version_nodes(dag_version_id, node_key);
CREATE INDEX idx_dag_edges_version ON dag_version_edges(dag_version_id);
CREATE INDEX idx_runs_dag ON dag_runs(dag_id, run_id DESC);
CREATE UNIQUE INDEX ux_dag_runs_scheduled_occurrence ON dag_runs(trigger_type, trigger_node_id, scheduled_at) WHERE trigger_type IN ('cron','interval');
CREATE INDEX idx_jobs_run ON jobs(run_id, job_id);
CREATE INDEX idx_jobs_status_due ON jobs(status, due_at);
CREATE INDEX idx_jobs_dispatched_at ON jobs(status, dispatched_at);
CREATE INDEX idx_jobs_heartbeat_at ON jobs(status, last_heartbeat_at);
CREATE INDEX idx_job_frontier_ready ON job_frontier(ready);
CREATE INDEX idx_job_deps_child ON job_dependencies(child_job_id);
CREATE INDEX idx_job_queue_available ON job_queue(available_at, priority DESC);
