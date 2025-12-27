package repository

import (
    "context"
    "time"
)

type DAG struct {
    ID        string    `json:"id"`
    Namespace string    `json:"namespace"`
    Name      string    `json:"name"`
    Version   int       `json:"version"`
    CreatedAt time.Time `json:"created_at"`
    Deleted   bool      `json:"deleted"`
}

type DAGRepository interface {
    ListByNamespace(ctx context.Context, namespaceID string) ([]DAG, error)
    Create(ctx context.Context, namespaceID, name string, version int) (*DAG, error)
    Upsert(ctx context.Context, dag DAG) (*DAG, error)
    Get(ctx context.Context, namespaceID, id string) (*DAG, error)
    Update(ctx context.Context, dag DAG) (*DAG, error)
    Delete(ctx context.Context, namespaceID, id string) error
}
