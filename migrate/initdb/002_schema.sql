-- 002_init.sql
-- Chronosched schema initialization

DO $$ BEGIN
  CREATE TYPE job_status AS ENUM ('queued','running','succeeded','failed','cancelled');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Namespaces
CREATE TABLE IF NOT EXISTS namespaces (
  namespace_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name         TEXT NOT NULL UNIQUE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Root DAG metadata
CREATE TABLE IF NOT EXISTS dags (
  id UUID PRIMARY KEY,
  namespace  TEXT NOT NULL,
  name       TEXT NOT NULL,
  version    INT  NOT NULL DEFAULT 1,
  deleted    BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(namespace, name, version)
);

-- Enforce uniqueness of active (non-deleted) DAG names within a namespace.
CREATE UNIQUE INDEX IF NOT EXISTS dags_unique_active_name
  ON dags(namespace, name)
  WHERE deleted = FALSE;


-- Immutable job definitions (templates)
CREATE TABLE IF NOT EXISTS job_definitions (
  def_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  namespace         TEXT NOT NULL,
  name              TEXT NOT NULL,
  version           INT  NOT NULL DEFAULT 1,
  kind              TEXT NOT NULL,
  payload_template  JSONB NOT NULL,

  -- Optional CRON expression for periodic scheduling (e.g. '0 * * * *' or '@every 10m')
  cron_spec         TEXT,

  -- Optional delay interval for dependency-triggered jobs (e.g. '5 minutes', '1 hour')
  -- This delay is applied after all parent jobs have succeeded.
  delay_interval    INTERVAL,

  deleted           BOOLEAN NOT NULL DEFAULT FALSE,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(namespace, name, version)
);

-- Enforce uniqueness of active (non-deleted) definition names within a namespace.
CREATE UNIQUE INDEX IF NOT EXISTS job_definitions_unique_active_name
  ON job_definitions(namespace, name)
  WHERE deleted = FALSE;


-- Prevent updates to definitions (immutability)
CREATE OR REPLACE FUNCTION prevent_jobdef_update()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'job_definitions are immutable; insert a new version';
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_jobdef_update ON job_definitions;
CREATE TRIGGER trg_prevent_jobdef_update
BEFORE UPDATE ON job_definitions
FOR EACH ROW EXECUTE FUNCTION prevent_jobdef_update();

-- Concrete job instances
CREATE TABLE IF NOT EXISTS jobs (
  id           BIGSERIAL PRIMARY KEY,
  dag_id       UUID NOT NULL REFERENCES dags(id) ON DELETE CASCADE,
  def_id       UUID NOT NULL REFERENCES job_definitions(def_id),
  version       INT NOT NULL DEFAULT 1,
  deleted       BOOLEAN NOT NULL DEFAULT FALSE,
  status       job_status NOT NULL DEFAULT 'queued',
  priority     INT NOT NULL DEFAULT 0,
  due_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  binary_data  BYTEA,
  lease_owner  TEXT,
  lease_until  TIMESTAMPTZ,
  enqueued_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at   TIMESTAMPTZ,
  finished_at  TIMESTAMPTZ,
  last_error   TEXT,
  UNIQUE(dag_id, def_id)
);

-- Directed edges between jobs
CREATE TABLE IF NOT EXISTS job_dependencies (
  dag_id        UUID NOT NULL,
  parent_job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  child_job_id  BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  dependency_type TEXT NOT NULL DEFAULT 'data',
  PRIMARY KEY (dag_id, parent_job_id, child_job_id),
  CHECK (parent_job_id <> child_job_id),
  CHECK (dependency_type IN ('order-only','data'))
);

-- Transitive closure for reachability (used for cycle detection)
CREATE TABLE IF NOT EXISTS job_closure (
  dag_id        UUID NOT NULL,
  ancestor_id   BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  descendant_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  depth         INT NOT NULL,
  PRIMARY KEY (dag_id, ancestor_id, descendant_id)
);

-- Frontier tracking for DAG scheduling (in-degree / readiness)
CREATE TABLE IF NOT EXISTS job_frontier (
  dag_id    UUID NOT NULL,
  job_id    BIGINT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
  in_degree INT NOT NULL DEFAULT 0,
  ready     BOOLEAN NOT NULL DEFAULT FALSE
);

-- Ensure new edges don't introduce cycles
CREATE OR REPLACE FUNCTION enforce_acyclic_closure()
RETURNS TRIGGER AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM job_closure
    WHERE dag_id = NEW.dag_id
      AND ancestor_id   = NEW.child_job_id
      AND descendant_id = NEW.parent_job_id
  ) THEN
    RAISE EXCEPTION 'cycle detected (% -> %)', NEW.parent_job_id, NEW.child_job_id;
  END IF;

  INSERT INTO job_closure(dag_id, ancestor_id, descendant_id, depth)
  VALUES (NEW.dag_id, NEW.parent_job_id, NEW.child_job_id, 1)
  ON CONFLICT DO NOTHING;

  INSERT INTO job_closure(dag_id, ancestor_id, descendant_id, depth)
  SELECT NEW.dag_id, p.ancestor_id, NEW.child_job_id, p.depth + 1
  FROM job_closure p
  WHERE p.dag_id = NEW.dag_id AND p.descendant_id = NEW.parent_job_id
  ON CONFLICT DO NOTHING;

  INSERT INTO job_closure(dag_id, ancestor_id, descendant_id, depth)
  SELECT NEW.dag_id, NEW.parent_job_id, c.descendant_id, c.depth + 1
  FROM job_closure c
  WHERE c.dag_id = NEW.dag_id AND c.ancestor_id = NEW.child_job_id
  ON CONFLICT DO NOTHING;

  INSERT INTO job_closure(dag_id, ancestor_id, descendant_id, depth)
  SELECT NEW.dag_id, p.ancestor_id, c.descendant_id, p.depth + c.depth + 1
  FROM job_closure p
  JOIN job_closure c
    ON p.dag_id = NEW.dag_id
   AND c.dag_id = NEW.dag_id
   AND p.descendant_id = NEW.parent_job_id
   AND c.ancestor_id   = NEW.child_job_id
  ON CONFLICT DO NOTHING;

  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dep_closure ON job_dependencies;
CREATE TRIGGER trg_dep_closure
BEFORE INSERT ON job_dependencies
FOR EACH ROW EXECUTE FUNCTION enforce_acyclic_closure();

-- Initialize frontier records for new jobs
CREATE OR REPLACE FUNCTION init_frontier_for_job(p_job_id BIGINT, p_dag UUID)
RETURNS VOID AS $$
BEGIN
  INSERT INTO job_frontier(dag_id, job_id, in_degree, ready)
  SELECT p_dag, p_job_id,
         COALESCE((SELECT COUNT(*) FROM job_dependencies WHERE dag_id = p_dag AND child_job_id = p_job_id),0),
         CASE WHEN (SELECT COUNT(*) FROM job_dependencies WHERE dag_id = p_dag AND child_job_id = p_job_id)=0 THEN TRUE ELSE FALSE END
  ON CONFLICT (job_id) DO NOTHING;
END; $$ LANGUAGE plpgsql;

-- Keep frontier in sync when dependencies change
CREATE OR REPLACE FUNCTION update_frontier_on_dependency()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    UPDATE job_frontier SET in_degree = in_degree + 1, ready = FALSE
    WHERE job_id = NEW.child_job_id;
  ELSIF TG_OP = 'DELETE' THEN
    UPDATE job_frontier SET in_degree = in_degree - 1,
           ready = (in_degree - 1) = 0
    WHERE job_id = OLD.child_job_id;
  END IF;
  RETURN NULL;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dep_frontier ON job_dependencies;
CREATE TRIGGER trg_dep_frontier
AFTER INSERT OR DELETE ON job_dependencies
FOR EACH ROW EXECUTE FUNCTION update_frontier_on_dependency();

-- When a job succeeds, propagate readiness to its children
CREATE OR REPLACE FUNCTION on_job_succeeded_make_children_ready()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.status = 'succeeded' AND OLD.status <> 'succeeded' THEN
    -- 1. Update frontier: mark children closer to readiness
    UPDATE job_frontier f
    SET in_degree = f.in_degree - 1,
        ready = (f.in_degree - 1) = 0
    WHERE f.job_id IN (
      SELECT child_job_id FROM job_dependencies
      WHERE dag_id = NEW.dag_id AND parent_job_id = NEW.id
    );

    -- 2. Update due_at for each child job, honoring its definition’s delay_interval
    UPDATE jobs j
    SET due_at = GREATEST(
      j.due_at,
      now() + COALESCE(d.delay_interval, INTERVAL '0 seconds')
    )
    FROM job_definitions d
    WHERE j.def_id = d.def_id
      AND j.id IN (
        SELECT child_job_id
        FROM job_dependencies
        WHERE dag_id = NEW.dag_id AND parent_job_id = NEW.id
      );
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_job_succeeded_children ON jobs;
CREATE TRIGGER trg_job_succeeded_children
AFTER UPDATE OF status ON jobs
FOR EACH ROW
EXECUTE FUNCTION on_job_succeeded_make_children_ready();

-- DAG deletion helper: cleans up only jobs unique to that DAG
CREATE OR REPLACE PROCEDURE delete_dag_preserve_shared(p_dag UUID)
LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM job_dependencies WHERE dag_id = p_dag;
  DELETE FROM job_closure     WHERE dag_id = p_dag;

  DELETE FROM jobs j
  WHERE j.dag_id = p_dag
    AND NOT EXISTS (
      SELECT 1 FROM jobs j2
      WHERE j2.def_id = j.def_id AND j2.dag_id <> p_dag
    );

  DELETE FROM dags WHERE id = p_dag;
END; $$;


-- History tables for pruned entities

CREATE TABLE IF NOT EXISTS dags_history (
  id         UUID NOT NULL,
  namespace  TEXT NOT NULL,
  name       TEXT NOT NULL,
  version    INT  NOT NULL,
  deleted    BOOLEAN NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_definitions_history (
  def_id           UUID NOT NULL,
  namespace        TEXT NOT NULL,
  name             TEXT NOT NULL,
  version          INT  NOT NULL,
  kind             TEXT NOT NULL,
  payload_template JSONB NOT NULL,
  cron_spec        TEXT,
  delay_interval   INTERVAL,
  deleted          BOOLEAN NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL,
  archived_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs_history (
  id           BIGINT NOT NULL,
  dag_id       UUID NOT NULL,
  def_id       UUID NOT NULL,
  status       job_status NOT NULL,
  priority     INT NOT NULL,
  due_at       TIMESTAMPTZ NOT NULL,
  payload_json JSONB NOT NULL,
  binary_data  BYTEA,
  lease_owner  TEXT,
  lease_until  TIMESTAMPTZ,
  enqueued_at  TIMESTAMPTZ NOT NULL,
  started_at   TIMESTAMPTZ,
  finished_at  TIMESTAMPTZ,
  last_error   TEXT,
  version      INT NOT NULL,
  deleted      BOOLEAN NOT NULL,
  archived_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_dependencies_history (
  dag_id          UUID NOT NULL,
  parent_job_id   BIGINT NOT NULL,
  child_job_id    BIGINT NOT NULL,
  dependency_type TEXT NOT NULL,
  archived_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
