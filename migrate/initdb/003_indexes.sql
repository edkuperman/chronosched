-- 003_indexes.sql
-- Chronosched performance indexes
-- These indexes accelerate queue operations, DAG traversal, and lease management
-- Safe to re-run: all use IF NOT EXISTS guards.

--------------------------------------------------------------------
-- SECTION 1: JOB DEQUEUEING & SCHEDULING PERFORMANCE
--------------------------------------------------------------------

-- Filter queued and due jobs efficiently
CREATE INDEX IF NOT EXISTS idx_jobs_ready
  ON jobs (status, due_at);

-- Support ORDER BY priority DESC, due_at ASC for fair scheduling
CREATE INDEX IF NOT EXISTS idx_jobs_priority_due
  ON jobs (priority DESC, due_at ASC);

-- Frontier readiness filter (topological readiness)
CREATE INDEX IF NOT EXISTS idx_frontier_ready
  ON job_frontier (ready)
  WHERE ready = TRUE;

--------------------------------------------------------------------
-- SECTION 2: DAG DEPENDENCIES & CYCLE DETECTION
--------------------------------------------------------------------

-- Speed up child lookups for dependency traversal
CREATE INDEX IF NOT EXISTS idx_jobdeps_dag_child
  ON job_dependencies (dag_id, child_job_id);

-- Speed up parent lookups for dependency traversal
CREATE INDEX IF NOT EXISTS idx_jobdeps_dag_parent
  ON job_dependencies (dag_id, parent_job_id);

-- Optimize ancestor lookups during acyclicity enforcement
CREATE INDEX IF NOT EXISTS idx_jobclosure_dag_ancestor
  ON job_closure (dag_id, ancestor_id);

-- Optimize descendant lookups during acyclicity enforcement
CREATE INDEX IF NOT EXISTS idx_jobclosure_dag_descendant
  ON job_closure (dag_id, descendant_id);

--------------------------------------------------------------------
-- SECTION 3: JOB LEASES & RECOVERY
--------------------------------------------------------------------

-- Speed up expired lease detection and requeueing
CREATE INDEX IF NOT EXISTS idx_jobs_status_lease
  ON jobs (status, lease_until);

-- Quickly locate running jobs by worker (for lease extension or heartbeats)
CREATE INDEX IF NOT EXISTS idx_jobs_lease_owner
  ON jobs (lease_owner)
  WHERE status = 'running';

--------------------------------------------------------------------
-- SECTION 4: PERIODIC JOB DEFINITIONS
--------------------------------------------------------------------

-- Accelerate scheduler.LoadAndRegister() at startup
CREATE INDEX IF NOT EXISTS idx_jobdef_cron_spec
  ON job_definitions (cron_spec)
  WHERE cron_spec IS NOT NULL;




