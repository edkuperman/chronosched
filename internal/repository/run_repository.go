package repository

import (
	"context"
	"time"
)

type RunStatus string

const (
	RunStatusWaiting   RunStatus = "waiting"
	RunStatusRunning   RunStatus = "running"
	RunStatusSucceeded RunStatus = "succeeded"
	RunStatusFailed    RunStatus = "failed"
	RunStatusMissed    RunStatus = "missed"
	RunStatusCancelled RunStatus = "cancelled"
)

type RunTrigger struct {
	Type           string  `json:"type"`
	DefinitionID   *string `json:"definition_id,omitempty"`
	DefinitionName *string `json:"definition_name,omitempty"`
}

type DAGRun struct {
	ID            int64      `json:"id"`
	DAGID         string     `json:"dag_id"`
	DAGVersionID  string     `json:"dag_version_id"`
	DAGName       string     `json:"dag_name,omitempty"`
	VersionNumber int        `json:"version_number,omitempty"`
	Trigger       RunTrigger `json:"trigger"`
	Status        RunStatus  `json:"status"`
	ScheduledAt   time.Time  `json:"scheduled_at"`
	CreatedAt     time.Time  `json:"created_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}

type RunJob struct {
	JobID               int64      `json:"job_id"`
	RunID               int64      `json:"run_id"`
	NodeKey             string     `json:"node_key"`
	DisplayName         string     `json:"display_name"`
	JobDefinitionID     string     `json:"job_definition_id"`
	JobDefinitionName   string     `json:"job_definition_name,omitempty"`
	Status              JobStatus  `json:"status"`
	DueAt               time.Time  `json:"due_at"`
	DispatchedAt        *time.Time `json:"dispatched_at,omitempty"`
	StartedAt           *time.Time `json:"started_at,omitempty"`
	LastHeartbeatAt     *time.Time `json:"last_heartbeat_at,omitempty"`
	FinishedAt          *time.Time `json:"finished_at,omitempty"`
	ExternalExecutionID *string    `json:"external_execution_id,omitempty"`
	ReasonCode          *string    `json:"reason_code,omitempty"`
	LastError           *string    `json:"last_error,omitempty"`
	IsReady             *bool      `json:"is_ready,omitempty"`
}

type RunGraphEdge struct {
	FromJobID int64 `json:"from_job_id"`
	ToJobID   int64 `json:"to_job_id"`
}

type RunGraph struct {
	Run   DAGRun         `json:"run"`
	Nodes []RunJob       `json:"nodes"`
	Edges []RunGraphEdge `json:"edges"`
}

type RunSchedulingMeta struct {
	TriggerType   string
	DAGVersionID  string
	TriggerNodeID string
	ScheduledAt   time.Time
}

type RunRepository interface {
	CreateManualRun(ctx context.Context, dagID string, dagVersionID *string, scheduledAt time.Time) (*DAGRun, error)
	CreateScheduledRun(ctx context.Context, dagVersionID, triggerNodeID, definitionID, triggerType string, scheduledAt time.Time) (*DAGRun, error)
	ListByDAG(ctx context.Context, dagID string) ([]DAGRun, error)
	Get(ctx context.Context, runID int64) (*DAGRun, error)
	GetSchedulingMeta(ctx context.Context, runID int64) (*RunSchedulingMeta, error)
	ListJobs(ctx context.Context, runID int64) ([]RunJob, error)
	GetGraph(ctx context.Context, runID int64) (*RunGraph, error)
	RefreshStatus(ctx context.Context, runID int64) error
}
