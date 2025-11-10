\connect chronosched

\i /docker-entrypoint-initdb.d/002_init.sql
\i /docker-entrypoint-initdb.d/003_seed_wait_jobs.sql
