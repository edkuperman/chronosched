package repository

import (
	"context"
	"time"
)

type DAG struct {
	ID                  string    `json:"id"`
	NamespaceID         string    `json:"namespace_id"`
	Name                string    `json:"name"`
	Description         string    `json:"description,omitempty"`
	ActiveVersionID     *string   `json:"active_version_id,omitempty"`
	LatestVersionNumber int       `json:"latest_version_number"`
	CreatedAt           time.Time `json:"created_at"`
}

type DAGVersion struct {
	ID               string    `json:"id"`
	DAGID            string    `json:"dag_id"`
	VersionNumber    int       `json:"version_number"`
	VersionNote      string    `json:"version_note,omitempty"`
	BasedOnVersionID *string   `json:"based_on_version_id,omitempty"`
	IsActive         bool      `json:"is_active"`
	CreatedAt        time.Time `json:"created_at"`
}

type DAGVersionNode struct {
	ID                string    `json:"id"`
	NodeKey           string    `json:"node_key"`
	DisplayName       string    `json:"display_name"`
	JobDefinitionID   string    `json:"job_definition_id"`
	JobDefinitionName string    `json:"job_definition_name"`
	Schedule          *Schedule `json:"schedule,omitempty"`
}

type DAGVersionEdge struct {
	FromNodeKey string `json:"from_node_key"`
	ToNodeKey   string `json:"to_node_key"`
}

type DAGVersionGraph struct {
	DAGVersionID  string           `json:"dag_version_id"`
	DAGID         string           `json:"dag_id"`
	DAGName       string           `json:"dag_name"`
	VersionNumber int              `json:"version_number"`
	IsActive      bool             `json:"is_active"`
	Nodes         []DAGVersionNode `json:"nodes"`
	Edges         []DAGVersionEdge `json:"edges"`
}

type DAGVersionInputNode struct {
	NodeKey         string `json:"node_key"`
	DisplayName     string `json:"display_name"`
	JobDefinitionID string `json:"job_definition_id"`
}

type DAGVersionInputEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type DAGVersionCreateInput struct {
	VersionNote      string                `json:"version_note"`
	BasedOnVersionID *string               `json:"based_on_version_id,omitempty"`
	Nodes            []DAGVersionInputNode `json:"nodes"`
	Edges            []DAGVersionInputEdge `json:"edges"`
}

type DAGRepository interface {
	ListByNamespace(ctx context.Context, namespaceID string) ([]DAG, error)
	Create(ctx context.Context, namespaceID, name, description string) (*DAG, error)
	Get(ctx context.Context, dagID string) (*DAG, error)
	Delete(ctx context.Context, dagID string) error
	CreateVersion(ctx context.Context, dagID string, input DAGVersionCreateInput) (*DAGVersion, error)
	ListVersions(ctx context.Context, dagID string) ([]DAGVersion, error)
	GetVersion(ctx context.Context, dagVersionID string) (*DAGVersion, error)
	ActivateVersion(ctx context.Context, dagVersionID string) error
	RevertVersion(ctx context.Context, dagVersionID string, activate bool, note string) (*DAGVersion, error)
	GetVersionGraph(ctx context.Context, dagVersionID string) (*DAGVersionGraph, error)
}
