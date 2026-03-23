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
func (j *JobSQL) FindWaitingBlockedByFailedDependency(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
	return j.store.FindWaitingBlockedByFailedDependency(ctx, before, limit)
}
func (j *JobSQL) FindStaleDispatched(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
	return j.store.FindStaleDispatched(ctx, before, limit)
}
func (j *JobSQL) FindStaleRunning(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
	return j.store.FindStaleRunning(ctx, before, limit)
}
func (j *JobSQL) MarkQueued(ctx context.Context, id int64) error { return j.store.MarkQueued(ctx, id) }
func (j *JobSQL) MarkDispatching(ctx context.Context, id int64, workerID string, leaseFor time.Duration) error {
	return j.store.MarkDispatching(ctx, id, workerID, leaseFor)
}
func (j *JobSQL) RecordDispatchAccepted(ctx context.Context, id int64, externalExecutionID string) error {
	return j.store.RecordDispatchAccepted(ctx, id, externalExecutionID)
}
func (j *JobSQL) RecordDispatchRetry(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	return j.store.RecordDispatchRetry(ctx, id, reasonCode, reasonDetail)
}
func (j *JobSQL) RecordDispatchFailed(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	return j.store.RecordDispatchFailed(ctx, id, reasonCode, reasonDetail)
}
func (j *JobSQL) RecordStarted(ctx context.Context, id int64, externalExecutionID string) error {
	return j.store.RecordStarted(ctx, id, externalExecutionID)
}
func (j *JobSQL) RecordHeartbeat(ctx context.Context, id int64, heartbeatAt time.Time, detail string) error {
	return j.store.RecordHeartbeat(ctx, id, heartbeatAt, detail)
}
func (j *JobSQL) RecordCompletion(ctx context.Context, id int64, success bool, reasonCode, reasonDetail string) error {
	return j.store.RecordCompletion(ctx, id, success, reasonCode, reasonDetail)
}
func (j *JobSQL) MarkLost(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	return j.store.MarkLost(ctx, id, reasonCode, reasonDetail)
}
func (j *JobSQL) MarkMissed(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	return j.store.MarkMissed(ctx, id, reasonCode, reasonDetail)
}
func (j *JobSQL) MarkBlocked(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	return j.store.MarkBlocked(ctx, id, reasonCode, reasonDetail)
}
func (j *JobSQL) ListProblemJobs(ctx context.Context, namespaceID string, dagID *string, statuses []repository.JobStatus, limit int) ([]repository.ProblemJob, error) {
	return j.store.ListProblemJobs(ctx, namespaceID, dagID, statuses, limit)
}
func (j *JobSQL) RestartJob(ctx context.Context, namespaceID string, jobID int64, opts repository.RestartJobOptions) (*repository.RestartJobResult, error) {
	return j.store.RestartJob(ctx, namespaceID, jobID, opts)
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
