package sql

import (
	"context"
	"github.com/edkuperman/chronosched/internal/repository"
	"time"
)

type QueueSQL struct{ store *Store }

func NewQueueSQL(dal *SQLDAL) *QueueSQL { return &QueueSQL{store: NewStore(dal)} }
func (q *QueueSQL) Enqueue(ctx context.Context, jobID int64, runAt time.Time, priority int) error {
	return q.store.Enqueue(ctx, jobID, runAt, priority)
}
func (q *QueueSQL) Dequeue(ctx context.Context, workerID string, n int, vt time.Duration) ([]repository.QueueItem, error) {
	return q.store.Dequeue(ctx, workerID, n, vt)
}
func (q *QueueSQL) Ack(ctx context.Context, queueID int64, workerID string) error {
	return q.store.Ack(ctx, queueID, workerID)
}
func (q *QueueSQL) Fail(ctx context.Context, queueID int64, workerID string, delay time.Duration) error {
	return q.store.Fail(ctx, queueID, workerID, delay)
}
