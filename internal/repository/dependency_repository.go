package repository

import "context"

type Dependency struct {
    ParentJobID    int64  `json:"parent_job_id"`
    ChildJobID     int64  `json:"child_job_id"`
    DependencyType string `json:"dependency_type"`
}

type DependencyRepository interface {
    ListByDAG(ctx context.Context, dagID string) ([]Dependency, error)
    Create(ctx context.Context, dagID string, dep Dependency) error
    BulkUpsert(ctx context.Context, dagID string, deps []Dependency) error
    Patch(ctx context.Context, dagID string, deps []Dependency) error
    DeleteAll(ctx context.Context, dagID string) error
}
