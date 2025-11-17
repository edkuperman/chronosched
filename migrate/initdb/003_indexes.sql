-- 003_indexes.sql
-- Chronosched performance indexes
-- Safe to re-run: all use IF NOT EXISTS

--------------------------------------------------------------------
-- SECTION 1: JOB DEQUEUEING & SCHEDULING PERFORMANCE
--------------------------------------------------------------------

-- Filter queued and due jobs efficiently
CREATE INDEX IF NOT EXISTS idx_jobs_ready
  ON jobs (status, due_at);

-- Support ORDER BY priority DESC, due_at ASC for fair scheduling
CREATE INDEX IF NOT EXISTS idx_jobs_priority_due
  ON jobs (priority DESC, due_at ASC);

-- Accelerate lease-based dequeue (FOR UPDATE SKIP LOCKED pattern)
CREATE INDEX IF NOT EXISTS idx_jobs_lease_owner_until
  ON jobs (lease_owner, lease_until)
  WHERE status = 'queued';

-- Helpful for pruning: finished or deleted jobs
CREATE INDEX IF NOT EXISTS idx_jobs_status_finished_at
  ON jobs (status, finished_at)
  WHERE status IN ('succeeded', 'failed', 'cancelled') OR deleted = TRUE;

--------------------------------------------------------------------
-- SECTION 2: JOB HISTORY LOOKUPS
--------------------------------------------------------------------

-- Look up historical jobs by DAG quickly
CREATE INDEX IF NOT EXISTS idx_jobhist_dag
  ON jobs_history (dag_id, finished_at);

-- Look up historical jobs by definition
CREATE INDEX IF NOT EXISTS idx_jobhist_def
  ON jobs_history (def_id, finished_at);

--------------------------------------------------------------------
-- SECTION 3: JOB-LEVEL DAG DEPENDENCIES  (correct model)
--------------------------------------------------------------------

-- Find all children of a given parent inside a DAG
CREATE INDEX IF NOT EXISTS idx_jobdeps_parent
  ON job_dependencies (dag_id, parent_job_id)
  WHERE deleted = FALSE;

-- Find all parents of a given child inside a DAG
CREATE INDEX IF NOT EXISTS idx_jobdeps_child
  ON job_dependencies (dag_id, child_job_id)
  WHERE deleted = FALSE;

-- Uniqueness is enforced at DB level already:
--   UNIQUE (dag_id, parent_job_id, child_job_id)
-- But this index helps fast lookups:
CREATE INDEX IF NOT EXISTS idx_jobdeps_pair
  ON job_dependencies (dag_id, parent_job_id, child_job_id)
  WHERE deleted = FALSE;


--------------------------------------------------------------------
-- SECTION 4: PERIODIC JOB DEFINITIONS (CRON)
--------------------------------------------------------------------

-- Accelerate scheduler.LoadAndRegister() at startup
CREATE INDEX IF NOT EXISTS idx_jobdef_cron_spec
  ON job_definitions (cron_spec)
  WHERE cron_spec IS NOT NULL;
