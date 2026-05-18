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
	JobStatusBlocked     JobStatus = "blocked"
	JobStatusCancelled   JobStatus = "cancelled"
	JobStatusSkipped     JobStatus = "skipped"
)

type ProblemJob struct {
	JobID            int64      `json:"job_id"`
	RunID            int64      `json:"run_id"`
	NamespaceID      string     `json:"namespace_id"`
	DAGID            string     `json:"dag_id"`
	DAGName          string     `json:"dag_name,omitempty"`
	NodeKey          string     `json:"node_key"`
	DisplayName      string     `json:"display_name"`
	Status           JobStatus  `json:"status"`
	DispatchAttempts int        `json:"dispatch_attempts"`
	ReasonCode       *string    `json:"reason_code,omitempty"`
	ReasonDetail     *string    `json:"reason_detail,omitempty"`
	LastError        *string    `json:"last_error,omitempty"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
	IsReady          bool       `json:"is_ready"`
	IsRestartable    bool       `json:"is_restartable"`
}

type RestartJobOptions struct {
	Cascade bool `json:"cascade"`
}

type RestartJobResult struct {
	JobID       int64     `json:"job_id"`
	RunID       int64     `json:"run_id"`
	Cascade     bool      `json:"cascade"`
	ResetJobIDs []int64   `json:"reset_job_ids"`
	RestartedAt time.Time `json:"restarted_at"`
}

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
	FindStaleDispatching(ctx context.Context, before time.Time, limit int) ([]*Job, error)
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
	MarkBlocked(ctx context.Context, id int64, reasonCode, reasonDetail string) error
	ListProblemJobs(ctx context.Context, namespaceID string, dagID *string, statuses []JobStatus, limit int) ([]ProblemJob, error)
	RestartJob(ctx context.Context, namespaceID string, jobID int64, opts RestartJobOptions) (*RestartJobResult, error)
	GetReadiness(ctx context.Context, id int64) (*JobReadiness, error)
	GetRunID(ctx context.Context, id int64) (int64, error)
	GetExecution(ctx context.Context, id int64) (*JobExecution, error)
}
