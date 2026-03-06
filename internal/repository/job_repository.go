package repository

import (
	"context"
	"encoding/json"
	"time"
)

type JobStatus string

const (
	JobStatusWaiting   JobStatus = "waiting"
	JobStatusQueued    JobStatus = "queued"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
	JobStatusMissed    JobStatus = "missed"
	JobStatusCancelled JobStatus = "cancelled"
	JobStatusSkipped   JobStatus = "skipped"
)

type Job struct {
	ID       int64     `json:"id"`
	RunID    int64     `json:"run_id"`
	Status   JobStatus `json:"status"`
	Priority int       `json:"priority"`
	DueAt    time.Time `json:"due_at"`
	NodeKey  string    `json:"node_key"`
	DefID    string    `json:"job_definition_id"`
}

type JobExecution struct {
	JobID      int64           `json:"job_id"`
	NodeKey    string          `json:"node_key"`
	Kind       string          `json:"kind"`
	Payload    json.RawMessage `json:"payload"`
	Definition string          `json:"definition_id"`
}

type JobReadiness struct {
	JobID             int64            `json:"job_id"`
	Status            JobStatus        `json:"status"`
	IsReady           bool             `json:"is_ready"`
	BlockingUpstreams []BlockingParent `json:"blocking_upstreams"`
}

type BlockingParent struct {
	JobID   int64     `json:"job_id"`
	NodeKey string    `json:"node_key"`
	Status  JobStatus `json:"status"`
}

type JobRepository interface {
	FindDueReadyWaiting(ctx context.Context, before time.Time, limit int) ([]*Job, error)
	MarkQueued(ctx context.Context, id int64) error
	MarkRunning(ctx context.Context, id int64) error
	MarkSucceeded(ctx context.Context, id int64) error
	MarkFailed(ctx context.Context, id int64, reason string) error
	MarkMissed(ctx context.Context, id int64, reason string) error
	GetReadiness(ctx context.Context, id int64) (*JobReadiness, error)
	GetRunID(ctx context.Context, id int64) (int64, error)
	GetExecution(ctx context.Context, id int64) (*JobExecution, error)
}
