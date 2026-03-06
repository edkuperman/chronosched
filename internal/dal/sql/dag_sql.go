package sql

import (
	"context"
	"github.com/edkuperman/chronosched/internal/repository"
)

type DAGSQL struct{ store *Store }

func NewDAGSQL(dal *SQLDAL) *DAGSQL { return &DAGSQL{store: NewStore(dal)} }
func (d *DAGSQL) ListByNamespace(ctx context.Context, namespaceID string) ([]repository.DAG, error) {
	return d.store.ListByNamespaceDAG(ctx, namespaceID)
}
func (d *DAGSQL) Create(ctx context.Context, namespaceID, name, description string) (*repository.DAG, error) {
	return d.store.CreateDAG(ctx, namespaceID, name, description)
}
func (d *DAGSQL) Get(ctx context.Context, dagID string) (*repository.DAG, error) {
	return d.store.GetDAG(ctx, dagID)
}
func (d *DAGSQL) Delete(ctx context.Context, dagID string) error {
	return d.store.DeleteDAG(ctx, dagID)
}
func (d *DAGSQL) CreateVersion(ctx context.Context, dagID string, input repository.DAGVersionCreateInput) (*repository.DAGVersion, error) {
	return d.store.CreateVersion(ctx, dagID, input)
}
func (d *DAGSQL) ListVersions(ctx context.Context, dagID string) ([]repository.DAGVersion, error) {
	return d.store.ListVersions(ctx, dagID)
}
func (d *DAGSQL) GetVersion(ctx context.Context, dagVersionID string) (*repository.DAGVersion, error) {
	return d.store.GetVersion(ctx, dagVersionID)
}
func (d *DAGSQL) ActivateVersion(ctx context.Context, dagVersionID string) error {
	return d.store.ActivateVersion(ctx, dagVersionID)
}
func (d *DAGSQL) RevertVersion(ctx context.Context, dagVersionID string, activate bool, note string) (*repository.DAGVersion, error) {
	return d.store.RevertVersion(ctx, dagVersionID, activate, note)
}
func (d *DAGSQL) GetVersionGraph(ctx context.Context, dagVersionID string) (*repository.DAGVersionGraph, error) {
	return d.store.GetVersionGraph(ctx, dagVersionID)
}
