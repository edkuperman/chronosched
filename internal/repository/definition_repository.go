package repository

import (
    "context"
    "time"
)

type JobDefinition struct {
    DefID           string    `json:"def_id"`
    Namespace       string    `json:"namespace"`
    Name            string    `json:"name"`
    Version         int       `json:"version"`
    Kind            string    `json:"kind"`
    PayloadTemplate []byte    `json:"payload_template"`
    CronSpec        *string   `json:"cron_spec,omitempty"`
    DelayInterval   *string   `json:"delay_interval,omitempty"`
    CreatedAt       time.Time `json:"created_at"`
    Deleted         bool      `json:"deleted"`
}

type JobDefinitionRepository interface {
    ListByNamespace(ctx context.Context, namespaceID string) ([]JobDefinition, error)
    Create(ctx context.Context, def JobDefinition) (*JobDefinition, error)
    Get(ctx context.Context, namespaceID, defID string) (*JobDefinition, error)
    Update(ctx context.Context, def JobDefinition) (*JobDefinition, error)
    Delete(ctx context.Context, namespaceID, defID string) error
    BulkUpsert(ctx context.Context, namespaceID string, defs []JobDefinition) error
}
