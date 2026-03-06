package repository

import (
	"context"
	"encoding/json"
	"time"
)

type QueueItem struct {
	QueueID    int64           `json:"queue_id"`
	JobID      int64           `json:"job_id"`
	Attempts   int             `json:"attempts"`
	Priority   int             `json:"priority"`
	NodeKey    string          `json:"node_key"`
	Kind       string          `json:"kind"`
	Payload    json.RawMessage `json:"payload"`
	Definition string          `json:"definition_id"`
}

type QueueRepository interface {
	Enqueue(ctx context.Context, jobID int64, runAt time.Time, priority int) error
	Dequeue(ctx context.Context, workerID string, n int, vt time.Duration) ([]QueueItem, error)
	Ack(ctx context.Context, queueID int64, workerID string) error
	Fail(ctx context.Context, queueID int64, workerID string, delay time.Duration) error
}
