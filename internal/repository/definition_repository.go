package repository

import (
	"context"
	"encoding/json"
	"time"
)

type Schedule struct {
	Type            string     `json:"type,omitempty"`
	Cron            string     `json:"cron,omitempty"`
	IntervalSeconds *int       `json:"interval_seconds,omitempty"`
	StartAt         *time.Time `json:"start_at,omitempty"`
	Timezone        string     `json:"timezone,omitempty"`
	OnFailure       string     `json:"on_failure,omitempty"`
}

type JobDefinition struct {
	ID              string          `json:"id"`
	NamespaceID     string          `json:"namespace_id"`
	Name            string          `json:"name"`
	Description     string          `json:"description,omitempty"`
	Kind            string          `json:"kind"`
	PayloadTemplate json.RawMessage `json:"payload_template"`
	Schedule        *Schedule       `json:"schedule,omitempty"`
	IsEnabled       bool            `json:"is_enabled"`
	IsPaused        bool            `json:"is_paused"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type DefinitionUsage struct {
	DAGID         string `json:"dag_id"`
	DAGName       string `json:"dag_name"`
	DAGVersionID  string `json:"dag_version_id"`
	VersionNumber int    `json:"version_number"`
	NodeKey       string `json:"node_key"`
	DisplayName   string `json:"display_name"`
	IsActive      bool   `json:"is_active"`
}

type ScheduledUsage struct {
	DAGID             string
	DAGVersionID      string
	DAGName           string
	VersionNumber     int
	NodeID            string
	NodeKey           string
	DisplayName       string
	DefinitionID      string
	DefinitionName    string
	ScheduleType      string
	CronSpec          string
	IntervalSeconds   *int
	StartAt           *time.Time
	Timezone          string
	DefinitionEnabled bool
	DefinitionPaused  bool
	OnFailurePolicy   string
	VersionCreatedAt  time.Time
}

type ScheduledParent struct {
	NodeID            string
	NodeKey           string
	DefinitionID      string
	DefinitionName    string
	ScheduleType      string
	CronSpec          string
	IntervalSeconds   *int
	StartAt           *time.Time
	Timezone          string
	DefinitionEnabled bool
	DefinitionPaused  bool
}

type CronFireStatus struct {
	Exists bool
	RunID  int64
	Status RunStatus
}

type JobDefinitionRepository interface {
	ListByNamespace(ctx context.Context, namespaceID string) ([]JobDefinition, error)
	Create(ctx context.Context, def JobDefinition) (*JobDefinition, error)
	Get(ctx context.Context, id string) (*JobDefinition, error)
	Update(ctx context.Context, def JobDefinition) (*JobDefinition, error)
	Delete(ctx context.Context, id string) error
	SetEnabled(ctx context.Context, id string, enabled bool) error
	SetPaused(ctx context.Context, id string, paused bool) error
	ApplyFailurePolicy(ctx context.Context, jobID int64) error
	ListUsages(ctx context.Context, definitionID string) ([]DefinitionUsage, error)
	ListScheduledUsages(ctx context.Context) ([]ScheduledUsage, error)
	ListScheduledParents(ctx context.Context, dagVersionID, nodeID string) ([]ScheduledParent, error)
	GetCronFireStatus(ctx context.Context, nodeID string, scheduledAt time.Time) (*CronFireStatus, error)

	GetCronNextRun(ctx context.Context, nodeID string) (*time.Time, error)
	SetCronNextRun(ctx context.Context, nodeID string, nextRunAt time.Time) error
}
