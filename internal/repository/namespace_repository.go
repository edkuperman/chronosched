package repository

import (
    "context"
    "time"
)

type Namespace struct {
    ID        string    `json:"namespace_id"`
    Name      string    `json:"name"`
    CreatedAt time.Time `json:"created_at"`
    Deleted   bool      `json:"deleted"`
}

type NamespaceRepository interface {
    List(ctx context.Context) ([]Namespace, error)
    Create(ctx context.Context, name string) (*Namespace, error)
    GetByName(ctx context.Context, name string) (*Namespace, error)
    Rename(ctx context.Context, oldName, newName string) (*Namespace, error)
    Delete(ctx context.Context, name string) error
}
