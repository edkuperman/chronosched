package sql

import (
    "context"

    "github.com/edkuperman/chronosched/internal/repository"
)

type NamespaceSQL struct {
    dal *SQLDAL
}

func NewNamespaceSQL(dal *SQLDAL) *NamespaceSQL {
    return &NamespaceSQL{dal: dal}
}

func (n *NamespaceSQL) List(ctx context.Context) ([]repository.Namespace, error) {
    const q = `
SELECT namespace_id, name, created_at, deleted
FROM namespaces
WHERE deleted = FALSE
ORDER BY name;
`
    rows, err := n.dal.DB.Query(ctx, q)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var res []repository.Namespace
    for rows.Next() {
        var ns repository.Namespace
        if err := rows.Scan(&ns.ID, &ns.Name, &ns.CreatedAt, &ns.Deleted); err != nil {
            return nil, err
        }
        res = append(res, ns)
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }
    return res, nil
}

func (n *NamespaceSQL) Create(ctx context.Context, name string) (*repository.Namespace, error) {
    const q = `
INSERT INTO namespaces (name)
VALUES ($1)
RETURNING namespace_id, name, created_at, deleted;
`
    var ns repository.Namespace
    err := n.dal.DB.QueryRow(ctx, q, name).Scan(&ns.ID, &ns.Name, &ns.CreatedAt, &ns.Deleted)
    if err != nil {
        return nil, err
    }
    return &ns, nil
}

func (n *NamespaceSQL) GetByName(ctx context.Context, name string) (*repository.Namespace, error) {
    const q = `
SELECT namespace_id, name, created_at, deleted
FROM namespaces
WHERE name = $1;
`
    var ns repository.Namespace
    err := n.dal.DB.QueryRow(ctx, q, name).Scan(&ns.ID, &ns.Name, &ns.CreatedAt, &ns.Deleted)
    if err != nil {
        return nil, err
    }
    return &ns, nil
}

func (n *NamespaceSQL) Rename(ctx context.Context, oldName, newName string) (*repository.Namespace, error) {
    const q = `
UPDATE namespaces
SET name = $2
WHERE name = $1
RETURNING namespace_id, name, created_at, deleted;
`
    var ns repository.Namespace
    err := n.dal.DB.QueryRow(ctx, q, oldName, newName).Scan(&ns.ID, &ns.Name, &ns.CreatedAt, &ns.Deleted)
    if err != nil {
        return nil, err
    }
    return &ns, nil
}

func (n *NamespaceSQL) Delete(ctx context.Context, name string) error {
    const q = `
UPDATE namespaces
SET deleted = TRUE
WHERE name = $1;
`
    _, err := n.dal.DB.Exec(ctx, q, name)
    return err
}
