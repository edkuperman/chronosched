package sql

import (
	"context"
	"github.com/edkuperman/chronosched/internal/repository"
)

type NamespaceSQL struct{ store *Store }

func NewNamespaceSQL(dal *SQLDAL) *NamespaceSQL { return &NamespaceSQL{store: NewStore(dal)} }
func (n *NamespaceSQL) List(ctx context.Context) ([]repository.Namespace, error) {
	return n.store.ListNamespaces(ctx)
}
func (n *NamespaceSQL) Create(ctx context.Context, name string) (*repository.Namespace, error) {
	return n.store.CreateNamespace(ctx, name)
}
func (n *NamespaceSQL) GetByName(ctx context.Context, name string) (*repository.Namespace, error) {
	return n.store.GetNamespaceByName(ctx, name)
}
