package sql

import (
    "context"

    "github.com/edkuperman/chronosched/internal/repository"
)

type DAGSQL struct {
    dal *SQLDAL
}

func NewDAGSQL(dal *SQLDAL) *DAGSQL {
    return &DAGSQL{dal: dal}
}

func (d *DAGSQL) ListByNamespace(ctx context.Context, namespaceID string) ([]repository.DAG, error) {
    const q = `
SELECT id, namespace, name, version, created_at, deleted
FROM dags
WHERE namespace = $1
ORDER BY name, version;
`
    rows, err := d.dal.DB.Query(ctx, q, namespaceID)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var res []repository.DAG
    for rows.Next() {
        var dag repository.DAG
        if err := rows.Scan(&dag.ID, &dag.Namespace, &dag.Name, &dag.Version, &dag.CreatedAt, &dag.Deleted); err != nil {
            return nil, err
        }
        res = append(res, dag)
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }
    return res, nil
}

func (d *DAGSQL) Create(ctx context.Context, namespaceID, name string, version int) (*repository.DAG, error) {
    const q = `
INSERT INTO dags (namespace, name, version)
VALUES ($1, $2, $3)
RETURNING id, namespace, name, version, created_at, deleted;
`
    var dag repository.DAG
    err := d.dal.DB.QueryRow(ctx, q, namespaceID, name, version).Scan(
        &dag.ID, &dag.Namespace, &dag.Name, &dag.Version, &dag.CreatedAt, &dag.Deleted,
    )
    if err != nil {
        return nil, err
    }
    return &dag, nil
}

func (d *DAGSQL) Upsert(ctx context.Context, dag repository.DAG) (*repository.DAG, error) {
    const q = `
INSERT INTO dags (namespace, name, version)
VALUES ($1, $2, $3)
ON CONFLICT (namespace, name, version)
DO UPDATE SET deleted = EXCLUDED.deleted
RETURNING id, namespace, name, version, created_at, deleted;
`
    var out repository.DAG
    err := d.dal.DB.QueryRow(ctx, q, dag.Namespace, dag.Name, dag.Version).Scan(
        &out.ID, &out.Namespace, &out.Name, &out.Version, &out.CreatedAt, &out.Deleted,
    )
    if err != nil {
        return nil, err
    }
    return &out, nil
}

func (d *DAGSQL) Get(ctx context.Context, namespaceID, id string) (*repository.DAG, error) {
    const q = `
SELECT id, namespace, name, version, created_at, deleted
FROM dags
WHERE namespace = $1 AND id = $2;
`
    var dag repository.DAG
    err := d.dal.DB.QueryRow(ctx, q, namespaceID, id).Scan(
        &dag.ID, &dag.Namespace, &dag.Name, &dag.Version, &dag.CreatedAt, &dag.Deleted,
    )
    if err != nil {
        return nil, err
    }
    return &dag, nil
}

func (d *DAGSQL) Update(ctx context.Context, dag repository.DAG) (*repository.DAG, error) {
    const q = `
UPDATE dags
SET name = $3, version = $4, deleted = $5
WHERE namespace = $1 AND id = $2
RETURNING id, namespace, name, version, created_at, deleted;
`
    var out repository.DAG
    err := d.dal.DB.QueryRow(ctx, q, dag.Namespace, dag.ID, dag.Name, dag.Version, dag.Deleted).Scan(
        &out.ID, &out.Namespace, &out.Name, &out.Version, &out.CreatedAt, &out.Deleted,
    )
    if err != nil {
        return nil, err
    }
    return &out, nil
}

func (d *DAGSQL) Delete(ctx context.Context, namespaceID, id string) error {
    const q = `
UPDATE dags
SET deleted = TRUE
WHERE namespace = $1 AND id = $2;
`
    _, err := d.dal.DB.Exec(ctx, q, namespaceID, id)
    return err
}
