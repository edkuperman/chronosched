package repository

import (
	"context"
	"encoding/json"
	"time"
)

type JobStatus string

const (
	JobStatusWaiting     JobStatus = "waiting"
	JobStatusQueued      JobStatus = "queued"
	JobStatusDispatching JobStatus = "dispatching"
	JobStatusDispatched  JobStatus = "dispatched"
	JobStatusRunning     JobStatus = "running"
	JobStatusSucceeded   JobStatus = "succeeded"
	JobStatusFailed      JobStatus = "failed"
	JobStatusLost        JobStatus = "lost"
	JobStatusMissed      JobStatus = "missed"
	JobStatusCancelled   JobStatus = "cancelled"
	JobStatusSkipped     JobStatus = "skipped"
)

type Job struct {
	ID                  int64      `json:"id"`
	RunID               int64      `json:"run_id"`
	Status              JobStatus  `json:"status"`
	Priority            int        `json:"priority"`
	DueAt               time.Time  `json:"due_at"`
	NodeKey             string     `json:"node_key"`
	DefID               string     `json:"job_definition_id"`
	DispatchedAt        *time.Time `json:"dispatched_at,omitempty"`
	LastHeartbeatAt     *time.Time `json:"last_heartbeat_at,omitempty"`
	StartedAt           *time.Time `json:"started_at,omitempty"`
	FinishedAt          *time.Time `json:"finished_at,omitempty"`
	ExternalExecutionID *string    `json:"external_execution_id,omitempty"`
	ReasonCode          *string    `json:"reason_code,omitempty"`
	ReasonDetail        *string    `json:"reason_detail,omitempty"`
}

type JobExecution struct {
	JobID      int64           `json:"job_id"`
	RunID      int64           `json:"run_id"`
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
	FindWaitingBlockedByFailedDependency(ctx context.Context, before time.Time, limit int) ([]*Job, error)
	FindStaleDispatched(ctx context.Context, before time.Time, limit int) ([]*Job, error)
	FindStaleRunning(ctx context.Context, before time.Time, limit int) ([]*Job, error)
	MarkQueued(ctx context.Context, id int64) error
	MarkDispatching(ctx context.Context, id int64, workerID string, leaseFor time.Duration) error
	RecordDispatchAccepted(ctx context.Context, id int64, externalExecutionID string) error
	RecordDispatchRetry(ctx context.Context, id int64, reasonCode, reasonDetail string) error
	RecordDispatchFailed(ctx context.Context, id int64, reasonCode, reasonDetail string) error
	RecordStarted(ctx context.Context, id int64, externalExecutionID string) error
	RecordHeartbeat(ctx context.Context, id int64, heartbeatAt time.Time, detail string) error
	RecordCompletion(ctx context.Context, id int64, success bool, reasonCode, reasonDetail string) error
	MarkLost(ctx context.Context, id int64, reasonCode, reasonDetail string) error
	MarkMissed(ctx context.Context, id int64, reasonCode, reasonDetail string) error
	GetReadiness(ctx context.Context, id int64) (*JobReadiness, error)
	GetRunID(ctx context.Context, id int64) (int64, error)
	GetExecution(ctx context.Context, id int64) (*JobExecution, error)
}
