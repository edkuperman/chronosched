package repository

import (
	"context"
	"time"
)

type Namespace struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type NamespaceRepository interface {
	List(ctx context.Context) ([]Namespace, error)
	Create(ctx context.Context, name string) (*Namespace, error)
	GetByName(ctx context.Context, name string) (*Namespace, error)
}
