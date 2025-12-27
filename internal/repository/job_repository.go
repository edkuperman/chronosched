package repository

import (
    "context"
    "time"
)

type JobStatus string

const (
    JobStatusWaiting   JobStatus = "waiting"
    JobStatusQueued    JobStatus = "queued"
    JobStatusRunning   JobStatus = "running"
    JobStatusSucceeded JobStatus = "succeeded"
    JobStatusFailed    JobStatus = "failed"
    JobStatusCancelled JobStatus = "cancelled"
)

type Job struct {
    ID       int64     `json:"id"`
    DagID    string    `json:"dag_id"`
    DefID    string    `json:"def_id"`
    Payload  []byte    `json:"payload"`
    Status   JobStatus `json:"status"`
    Priority int       `json:"priority"`
    DueAt    time.Time `json:"due_at"`
}

type JobListItem struct {
    ID     int64  `json:"id"`
    DefID  string `json:"def_id"`
    DagID  string `json:"dag_id"`
    Status string `json:"status"`
}

type JobRepository interface {
    Create(ctx context.Context, dagID, defID string, payload []byte, priority int) (*Job, error)
    Get(ctx context.Context, id int64) (*Job, error)
    ListByDAG(ctx context.Context, dagID string) ([]JobListItem, error)
    FindDueWaiting(ctx context.Context, before time.Time, limit int) ([]*Job, error)
    MarkQueued(ctx context.Context, id int64) error
    MarkRunning(ctx context.Context, id int64) error
    MarkSucceeded(ctx context.Context, id int64) error
    MarkFailed(ctx context.Context, id int64, reason string) error
    Delete(ctx context.Context, id int64) error
}
