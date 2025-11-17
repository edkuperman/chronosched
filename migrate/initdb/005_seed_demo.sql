
-- 004_seed_wait_jobs.sql

DELETE FROM job_definitions jd
USING namespaces ns
WHERE jd.namespace = ns. namespace_id
AND ns.name = 'core'
AND jd.name IN ('Wait','Wait5s','Wait10s','Wait5m','Wait1h');

WITH ns AS (
    SELECT namespace_id
    FROM namespaces
    WHERE name = 'core'
)
INSERT INTO job_definitions(namespace, name, version, kind, payload_template)
SELECT
    ns.namespace_id,
    v.name,
    v.version,
    v.kind,
    v.payload_template::jsonb
FROM ns
CROSS JOIN (
    VALUES
        ('Wait',    1, 'cmd', '{"cmd":"sleep 0"}'),
        ('Wait5s',  1, 'cmd', '{"cmd":"sleep 5"}'),
        ('Wait10s', 1, 'cmd', '{"cmd":"sleep 10"}')
) AS v(name, version, kind, payload_template)
ON CONFLICT DO NOTHING;
