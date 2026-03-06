package sql

import (
	"context"
	"github.com/edkuperman/chronosched/internal/repository"
	"time"
)

type RunSQL struct{ store *Store }

func NewRunSQL(dal *SQLDAL) *RunSQL { return &RunSQL{store: NewStore(dal)} }
func (r *RunSQL) CreateManualRun(ctx context.Context, dagID string, dagVersionID *string, scheduledAt time.Time) (*repository.DAGRun, error) {
	return r.store.CreateManualRun(ctx, dagID, dagVersionID, scheduledAt)
}
func (r *RunSQL) CreateScheduledRun(ctx context.Context, dagVersionID, triggerNodeID, definitionID, triggerType string, scheduledAt time.Time) (*repository.DAGRun, error) {
	return r.store.CreateScheduledRun(ctx, dagVersionID, triggerNodeID, definitionID, triggerType, scheduledAt)
}
func (r *RunSQL) ListByDAG(ctx context.Context, dagID string) ([]repository.DAGRun, error) {
	return r.store.ListByDAGRuns(ctx, dagID)
}
func (r *RunSQL) Get(ctx context.Context, runID int64) (*repository.DAGRun, error) {
	return r.store.GetRun(ctx, runID)
}
func (r *RunSQL) ListJobs(ctx context.Context, runID int64) ([]repository.RunJob, error) {
	return r.store.ListJobs(ctx, runID)
}
func (r *RunSQL) GetGraph(ctx context.Context, runID int64) (*repository.RunGraph, error) {
	return r.store.GetGraph(ctx, runID)
}
func (r *RunSQL) RefreshStatus(ctx context.Context, runID int64) error {
	return r.store.RefreshStatus(ctx, runID)
}
func (r *RunSQL) GetSchedulingMeta(ctx context.Context, runID int64) (*repository.RunSchedulingMeta, error) {
	return r.store.GetRunSchedulingMeta(ctx, runID)
}
