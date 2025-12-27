package sql

import (
    "context"

    "github.com/edkuperman/chronosched/internal/repository"
)

type DependencySQL struct {
    dal *SQLDAL
}

func NewDependencySQL(dal *SQLDAL) *DependencySQL {
    return &DependencySQL{dal: dal}
}

func (d *DependencySQL) ListByDAG(ctx context.Context, dagID string) ([]repository.Dependency, error) {
    const q = `
SELECT parent_job_id, child_job_id, dependency_type::text
FROM job_dependencies
WHERE dag_id = $1 AND deleted = FALSE
ORDER BY parent_job_id, child_job_id;
`
    rows, err := d.dal.DB.Query(ctx, q, dagID)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var res []repository.Dependency
    for rows.Next() {
        var dep repository.Dependency
        if err := rows.Scan(&dep.ParentJobID, &dep.ChildJobID, &dep.DependencyType); err != nil {
            return nil, err
        }
        res = append(res, dep)
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }
    return res, nil
}

func (d *DependencySQL) Create(ctx context.Context, dagID string, dep repository.Dependency) error {
    const q = `
INSERT INTO job_dependencies (dag_id, parent_job_id, child_job_id, dependency_type)
VALUES ($1, $2, $3, $4::dependency_type);
`
    _, err := d.dal.DB.Exec(ctx, q, dagID, dep.ParentJobID, dep.ChildJobID, dep.DependencyType)
    return err
}

func (d *DependencySQL) BulkUpsert(ctx context.Context, dagID string, deps []repository.Dependency) error {
    // Simple implementation: remove existing and reinsert.
    if err := d.DeleteAll(ctx, dagID); err != nil {
        return err
    }
    for _, dep := range deps {
        if err := d.Create(ctx, dagID, dep); err != nil {
            return err
        }
    }
    return nil
}

func (d *DependencySQL) Patch(ctx context.Context, dagID string, deps []repository.Dependency) error {
    // For now, treat patch as bulk upsert.
    return d.BulkUpsert(ctx, dagID, deps)
}

func (d *DependencySQL) DeleteAll(ctx context.Context, dagID string) error {
    const q = `
UPDATE job_dependencies
SET deleted = TRUE
WHERE dag_id = $1 AND deleted = FALSE;
`
    _, err := d.dal.DB.Exec(ctx, q, dagID)
    return err
}
