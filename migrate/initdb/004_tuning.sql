-- Run ANALYZE periodically to refresh planner statistics:
--   ANALYZE jobs;
--   ANALYZE job_frontier;
--   ANALYZE job_dependencies;
--   ANALYZE job_closure;

-- For heavy write loads, consider lowering autovacuum thresholds:
--   ALTER TABLE jobs SET (autovacuum_vacuum_scale_factor = 0.05);
--   ALTER TABLE job_frontier SET (autovacuum_vacuum_scale_factor = 0.05);