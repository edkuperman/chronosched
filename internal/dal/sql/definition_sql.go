package sql

import (
	"context"
	"github.com/edkuperman/chronosched/internal/repository"
	"time"
)

type JobDefinitionSQL struct{ store *Store }

func NewJobDefinitionSQL(dal *SQLDAL) *JobDefinitionSQL {
	return &JobDefinitionSQL{store: NewStore(dal)}
}
func (d *JobDefinitionSQL) ListByNamespace(ctx context.Context, namespaceID string) ([]repository.JobDefinition, error) {
	return d.store.ListByNamespace(ctx, namespaceID)
}
func (d *JobDefinitionSQL) Create(ctx context.Context, def repository.JobDefinition) (*repository.JobDefinition, error) {
	return d.store.CreateDefinition(ctx, def)
}
func (d *JobDefinitionSQL) Get(ctx context.Context, id string) (*repository.JobDefinition, error) {
	return d.store.GetDefinition(ctx, id)
}
func (d *JobDefinitionSQL) Update(ctx context.Context, def repository.JobDefinition) (*repository.JobDefinition, error) {
	return d.store.UpdateDefinition(ctx, def)
}
func (d *JobDefinitionSQL) Delete(ctx context.Context, id string) error {
	return d.store.DeleteDefinition(ctx, id)
}
func (d *JobDefinitionSQL) SetEnabled(ctx context.Context, id string, enabled bool) error {
	return d.store.SetEnabled(ctx, id, enabled)
}
func (d *JobDefinitionSQL) SetPaused(ctx context.Context, id string, paused bool) error {
	return d.store.SetPaused(ctx, id, paused)
}
func (d *JobDefinitionSQL) ApplyFailurePolicy(ctx context.Context, jobID int64) error {
	return d.store.ApplyFailurePolicy(ctx, jobID)
}
func (d *JobDefinitionSQL) ListUsages(ctx context.Context, definitionID string) ([]repository.DefinitionUsage, error) {
	return d.store.ListUsages(ctx, definitionID)
}
func (d *JobDefinitionSQL) ListScheduledUsages(ctx context.Context) ([]repository.ScheduledUsage, error) {
	return d.store.ListScheduledUsages(ctx)
}
func (d *JobDefinitionSQL) ListScheduledParents(ctx context.Context, dagVersionID, nodeID string) ([]repository.ScheduledParent, error) {
	return d.store.ListScheduledParents(ctx, dagVersionID, nodeID)
}
func (d *JobDefinitionSQL) GetCronFireStatus(ctx context.Context, nodeID string, scheduledAt time.Time) (*repository.CronFireStatus, error) {
	return d.store.GetCronFireStatus(ctx, nodeID, scheduledAt)
}
func (d *JobDefinitionSQL) GetCronNextRun(ctx context.Context, nodeID string) (*time.Time, error) {
	return d.store.GetCronNextRun(ctx, nodeID)
}
func (d *JobDefinitionSQL) SetCronNextRun(ctx context.Context, nodeID string, nextRunAt time.Time) error {
	return d.store.SetCronNextRun(ctx, nodeID, nextRunAt)
}
