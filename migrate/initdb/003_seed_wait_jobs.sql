
-- 002_seed_wait_jobs.sql

DELETE FROM job_definitions
WHERE namespace = 'core' AND name IN ('Wait','Wait5s','Wait10s','Wait5m','Wait1h');

INSERT INTO job_definitions(namespace, name, version, kind, payload_template)
VALUES
 ('core','Wait',1,'cmd', '{"cmd":"sleep 0"}'),
 ('core','Wait5s',1,'cmd', '{"cmd":"sleep 5"}'),
 ('core','Wait10s',1,'cmd', '{"cmd":"sleep 10"}')
ON CONFLICT DO NOTHING;
