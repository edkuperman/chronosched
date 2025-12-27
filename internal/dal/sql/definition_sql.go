package sql

import (
    "context"

    "github.com/edkuperman/chronosched/internal/repository"
)

type JobDefinitionSQL struct {
    dal *SQLDAL
}

func NewJobDefinitionSQL(dal *SQLDAL) *JobDefinitionSQL {
    return &JobDefinitionSQL{dal: dal}
}

func (d *JobDefinitionSQL) ListByNamespace(ctx context.Context, namespaceID string) ([]repository.JobDefinition, error) {
    const q = `
SELECT def_id, namespace, name, version, kind, payload_template::text, cron_spec, delay_interval::text, created_at, deleted
FROM job_definitions
WHERE namespace = $1
ORDER BY name, version;
`
    rows, err := d.dal.DB.Query(ctx, q, namespaceID)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var res []repository.JobDefinition
    for rows.Next() {
        var def repository.JobDefinition
        var payloadStr string
        var cronStr *string
        var delayStr *string
        if err := rows.Scan(&def.DefID, &def.Namespace, &def.Name, &def.Version, &def.Kind, &payloadStr, &cronStr, &delayStr, &def.CreatedAt, &def.Deleted); err != nil {
            return nil, err
        }
        def.PayloadTemplate = []byte(payloadStr)
        def.CronSpec = cronStr
        def.DelayInterval = delayStr
        res = append(res, def)
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }
    return res, nil
}

func (d *JobDefinitionSQL) Create(ctx context.Context, def repository.JobDefinition) (*repository.JobDefinition, error) {
    const q = `
INSERT INTO job_definitions (namespace, name, version, kind, payload_template, cron_spec, delay_interval)
VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::interval)
RETURNING def_id, namespace, name, version, kind, payload_template::text, cron_spec, delay_interval::text, created_at, deleted;
`
    var out repository.JobDefinition
    var payloadStr string
    var cronStr *string
    var delayStr *string
    err := d.dal.DB.QueryRow(ctx, q,
        def.Namespace, def.Name, def.Version, def.Kind, string(def.PayloadTemplate),
        def.CronSpec, def.DelayInterval,
    ).Scan(&out.DefID, &out.Namespace, &out.Name, &out.Version, &out.Kind,
        &payloadStr, &cronStr, &delayStr, &out.CreatedAt, &out.Deleted)
    if err != nil {
        return nil, err
    }
    out.PayloadTemplate = []byte(payloadStr)
    out.CronSpec = cronStr
    out.DelayInterval = delayStr
    return &out, nil
}

func (d *JobDefinitionSQL) Get(ctx context.Context, namespaceID, defID string) (*repository.JobDefinition, error) {
    const q = `
SELECT def_id, namespace, name, version, kind, payload_template::text, cron_spec, delay_interval::text, created_at, deleted
FROM job_definitions
WHERE namespace = $1 AND def_id = $2;
`
    var out repository.JobDefinition
    var payloadStr string
    var cronStr *string
    var delayStr *string
    err := d.dal.DB.QueryRow(ctx, q, namespaceID, defID).Scan(
        &out.DefID, &out.Namespace, &out.Name, &out.Version, &out.Kind,
        &payloadStr, &cronStr, &delayStr, &out.CreatedAt, &out.Deleted,
    )
    if err != nil {
        return nil, err
    }
    out.PayloadTemplate = []byte(payloadStr)
    out.CronSpec = cronStr
    out.DelayInterval = delayStr
    return &out, nil
}

func (d *JobDefinitionSQL) Update(ctx context.Context, def repository.JobDefinition) (*repository.JobDefinition, error) {
    const q = `
UPDATE job_definitions
SET name = $3,
    version = $4,
    kind = $5,
    payload_template = $6::jsonb,
    cron_spec = $7,
    delay_interval = $8::interval,
    deleted = $9
WHERE namespace = $1 AND def_id = $2
RETURNING def_id, namespace, name, version, kind, payload_template::text, cron_spec, delay_interval::text, created_at, deleted;
`
    var out repository.JobDefinition
    var payloadStr string
    var cronStr *string
    var delayStr *string
    err := d.dal.DB.QueryRow(ctx, q,
        def.Namespace, def.DefID, def.Name, def.Version, def.Kind,
        string(def.PayloadTemplate), def.CronSpec, def.DelayInterval, def.Deleted,
    ).Scan(&out.DefID, &out.Namespace, &out.Name, &out.Version, &out.Kind,
        &payloadStr, &cronStr, &delayStr, &out.CreatedAt, &out.Deleted)
    if err != nil {
        return nil, err
    }
    out.PayloadTemplate = []byte(payloadStr)
    out.CronSpec = cronStr
    out.DelayInterval = delayStr
    return &out, nil
}

func (d *JobDefinitionSQL) Delete(ctx context.Context, namespaceID, defID string) error {
    const q = `
UPDATE job_definitions
SET deleted = TRUE
WHERE namespace = $1 AND def_id = $2;
`
    _, err := d.dal.DB.Exec(ctx, q, namespaceID, defID)
    return err
}

func (d *JobDefinitionSQL) BulkUpsert(ctx context.Context, namespaceID string, defs []repository.JobDefinition) error {
    const q = `
INSERT INTO job_definitions (namespace, name, version, kind, payload_template, cron_spec, delay_interval, deleted)
VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::interval, COALESCE($8, FALSE))
ON CONFLICT (namespace, name, version)
DO UPDATE SET
  kind = EXCLUDED.kind,
  payload_template = EXCLUDED.payload_template,
  cron_spec = EXCLUDED.cron_spec,
  delay_interval = EXCLUDED.delay_interval,
  deleted = EXCLUDED.deleted;
`
    for _, def := range defs {
        deleted := def.Deleted
        _, err := d.dal.DB.Exec(ctx, q,
            namespaceID,
            def.Name,
            def.Version,
            def.Kind,
            string(def.PayloadTemplate),
            def.CronSpec,
            def.DelayInterval,
            &deleted,
        )
        if err != nil {
            return err
        }
    }
    return nil
}
