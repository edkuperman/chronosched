package sql

import (
	"context"
	"github.com/edkuperman/chronosched/internal/repository"
	"time"
)

type JobSQL struct{ store *Store }

func NewJobSQL(dal *SQLDAL) *JobSQL { return &JobSQL{store: NewStore(dal)} }
func (j *JobSQL) FindDueReadyWaiting(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
	return j.store.FindDueReadyWaiting(ctx, before, limit)
}
func (j *JobSQL) MarkQueued(ctx context.Context, id int64) error { return j.store.MarkQueued(ctx, id) }
func (j *JobSQL) MarkRunning(ctx context.Context, id int64) error {
	return j.store.MarkRunning(ctx, id)
}
func (j *JobSQL) MarkSucceeded(ctx context.Context, id int64) error {
	return j.store.MarkSucceeded(ctx, id)
}
func (j *JobSQL) MarkFailed(ctx context.Context, id int64, reason string) error {
	return j.store.MarkFailed(ctx, id, reason)
}
func (j *JobSQL) MarkMissed(ctx context.Context, id int64, reason string) error {
	return j.store.MarkMissed(ctx, id, reason)
}
func (j *JobSQL) GetReadiness(ctx context.Context, id int64) (*repository.JobReadiness, error) {
	return j.store.GetReadiness(ctx, id)
}
func (j *JobSQL) GetRunID(ctx context.Context, id int64) (int64, error) {
	return j.store.GetRunID(ctx, id)
}
func (j *JobSQL) GetExecution(ctx context.Context, id int64) (*repository.JobExecution, error) {
	return j.store.GetJobExecution(ctx, id)
}
