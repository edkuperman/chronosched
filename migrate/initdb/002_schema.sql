-- ENUMS
CREATE TYPE job_status AS ENUM (
  'waiting',
  'queued',
  'running',
  'succeeded',
  'failed',
  'cancelled'
);

CREATE TYPE dependency_type AS ENUM (
  'order-only',
  'data'
);

-- NAMESPACES
CREATE TABLE IF NOT EXISTS namespaces (
  namespace_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name           TEXT NOT NULL UNIQUE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted        BOOLEAN NOT NULL DEFAULT FALSE
);

-- DAGs (Versioned)
CREATE TABLE IF NOT EXISTS dags (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace      UUID NOT NULL REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
  name           TEXT NOT NULL,
  version        INT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted        BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE(namespace, name, version)
);

-- JOB DEFINITIONS
CREATE TABLE IF NOT EXISTS job_definitions (
  def_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace        UUID NOT NULL REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
  name             TEXT NOT NULL,
  version          INT NOT NULL,
  kind             TEXT NOT NULL CHECK (kind IN ('cmd', 'http', 'binary')),
  payload_template JSONB NOT NULL DEFAULT '{}'::jsonb,
  cron_spec        TEXT,
  delay_interval   INTERVAL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted          BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE(namespace, name, version)
);

-- JOBS (Runtime Executions)
CREATE TABLE IF NOT EXISTS jobs (
  id             BIGSERIAL PRIMARY KEY,
  dag_id         UUID NOT NULL REFERENCES dags(id) ON DELETE CASCADE,
  def_id         UUID NOT NULL REFERENCES job_definitions(def_id),
  version        INT NOT NULL DEFAULT 1,
  deleted        BOOLEAN NOT NULL DEFAULT FALSE,
  status         job_status NOT NULL DEFAULT 'queued',
  priority       INT NOT NULL DEFAULT 0,
  due_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload_json   JSONB NOT NULL DEFAULT '{}'::jsonb,
  binary_data    BYTEA,
  lease_owner    TEXT,
  lease_until    TIMESTAMPTZ,
  enqueued_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at     TIMESTAMPTZ,
  finished_at    TIMESTAMPTZ,
  last_error     TEXT,
  last_scheduled_at TIMESTAMPTZ
);

-- JOB-LEVEL DEPENDENCIES (DAG Edges)
CREATE TABLE IF NOT EXISTS job_dependencies (
  dag_id         UUID NOT NULL REFERENCES dags(id) ON DELETE CASCADE,
  dag_version    INT NOT NULL DEFAULT 1,
  parent_job_id  BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  child_job_id   BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  dependency_type dependency_type NOT NULL DEFAULT 'order-only',
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted        BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (dag_id, dag_version, parent_job_id, child_job_id)
);

-- HISTORY TABLES

CREATE TABLE IF NOT EXISTS job_definitions_history (
  def_id           UUID,
  namespace        UUID,
  name             TEXT,
  version          INT,
  kind             TEXT,
  payload_template JSONB,
  cron_spec        TEXT,
  delay_interval   INTERVAL,
  created_at       TIMESTAMPTZ,
  deleted          BOOLEAN,
  archived_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dags_history (
  id           UUID,
  namespace    UUID,
  name         TEXT,
  version      INT,
  created_at   TIMESTAMPTZ,
  deleted      BOOLEAN,
  archived_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs_history (
  id             BIGINT,
  dag_id         UUID,
  def_id         UUID,
  version        INT,
  deleted        BOOLEAN,
  status         job_status,
  priority       INT,
  due_at         TIMESTAMPTZ,
  payload_json   JSONB,
  binary_data    BYTEA,
  lease_owner    TEXT,
  lease_until    TIMESTAMPTZ,
  enqueued_at    TIMESTAMPTZ,
  started_at     TIMESTAMPTZ,
  finished_at    TIMESTAMPTZ,
  last_error     TEXT,
  last_scheduled_at TIMESTAMPTZ,
  archived_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_dependencies_history (
  dag_id         UUID,
  dag_version    INT,
  parent_job_id  BIGINT,
  child_job_id   BIGINT,
  dependency_type dependency_type,
  created_at     TIMESTAMPTZ,
  deleted        BOOLEAN,
  archived_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- FRONTIER TABLE

CREATE TABLE IF NOT EXISTS job_frontier (
    job_id BIGINT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    ready  BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_job_frontier_ready ON job_frontier(ready);

-- Initialize frontier entry for a newly created job.
-- If the job has NO parents (for this DAG), it's ready immediately.
CREATE OR REPLACE FUNCTION init_frontier_for_job(job_id BIGINT, dag UUID)
RETURNS VOID AS $$
BEGIN
    INSERT INTO job_frontier(job_id, ready)
    VALUES (
        job_id,
        NOT EXISTS (
            SELECT 1
            FROM job_dependencies
            WHERE dag_id = dag
              AND child_job_id = job_id
              AND deleted = FALSE
        )
    )
    ON CONFLICT (job_id) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- When a parent job succeeds, mark its children as ready.
CREATE OR REPLACE FUNCTION frontier_mark_children_ready()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE job_frontier AS f
    SET ready = TRUE
    FROM job_dependencies AS deps
    WHERE deps.parent_job_id = NEW.id
      AND deps.child_job_id = f.job_id
      AND deps.deleted = FALSE;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_parent_success_promote ON jobs;

CREATE TRIGGER trg_parent_success_promote
AFTER UPDATE OF status ON jobs
FOR EACH ROW
WHEN (NEW.status = 'succeeded')
EXECUTE FUNCTION frontier_mark_children_ready();

-- job_queue for worker gateway

CREATE TABLE IF NOT EXISTS job_queue (
  id             BIGSERIAL PRIMARY KEY,
  job_id         BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  available_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  priority       INT NOT NULL DEFAULT 0,
  attempts       INT NOT NULL DEFAULT 0,
  reserved_until TIMESTAMPTZ,
  consumer_id    TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- INDEXES

CREATE INDEX idx_jobs_dag ON jobs(dag_id);
CREATE INDEX idx_jobs_def ON jobs(def_id);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_due ON jobs(due_at);

CREATE INDEX idx_definitions_ns ON job_definitions(namespace, name);

CREATE INDEX idx_definitions_cron ON job_definitions(cron_spec)
  WHERE cron_spec IS NOT NULL AND trim(cron_spec) <> '';

CREATE INDEX idx_job_deps_dag ON job_dependencies(dag_id);
CREATE INDEX idx_job_deps_parent ON job_dependencies(parent_job_id);
CREATE INDEX idx_job_deps_child ON job_dependencies(child_job_id);

CREATE INDEX IF NOT EXISTS idx_job_queue_available ON job_queue(available_at);
CREATE INDEX IF NOT EXISTS idx_job_queue_reserved ON job_queue(reserved_until);
CREATE INDEX IF NOT EXISTS idx_job_queue_priority ON job_queue(priority DESC, available_at);

