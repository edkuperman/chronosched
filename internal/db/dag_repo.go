package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DAGRepo struct{ DB *pgxpool.Pool }

func NewDAGRepo(db *pgxpool.Pool) *DAGRepo { return &DAGRepo{DB: db} }

// insertVersionedDAG computes the next version for (namespace, name),
// inserts it if needed, and returns the latest (id, version).
func (r *DAGRepo) insertVersionedDAG(ctx context.Context, ns, name string) (string, int, error) {
	var id string
	var version int

	// Pattern is intentionally similar to JobRepo.AddDefinition:
	// - compute next version
	// - insert with ON CONFLICT(namespace, name, version) DO NOTHING
	// - if another writer won, read back the latest version
	err := r.DB.QueryRow(ctx, `
		WITH next_version AS (
			SELECT COALESCE(MAX(version), 0) + 1 AS v
			FROM dags
			WHERE namespace = $1 AND name = $2
		),
		ins AS (
			INSERT INTO dags(namespace, name, version)
			VALUES ($1, $2, COALESCE((SELECT v FROM next_version), 1))
			ON CONFLICT (namespace, name, version) DO NOTHING
			RETURNING id, version
		)
		SELECT id, version FROM ins
		UNION ALL
		SELECT id, version
		FROM dags
		WHERE namespace = $1 AND name = $2
		ORDER BY version DESC
		LIMIT 1;
	`, ns, name).Scan(&id, &version)

	if err != nil {
		return "", 0, err
	}

	return id, version, nil
}

// CreateDAG creates a new DAG version for (namespace, name).
// If another version is concurrently created, we return the latest one.
func (r *DAGRepo) CreateDAG(ctx context.Context, ns, name string) (string, error) {
	id, _, err := r.insertVersionedDAG(ctx, ns, name)
	return id, err
}

// UpsertDAG is used by the bulk upsert surface.
// We ignore dagID for versioning purposes and treat this exactly like
// bulkUpsertDefinitions: "make sure there's a DAG for this (namespace, name),
// creating a new version if needed".
func (r *DAGRepo) UpsertDAG(ctx context.Context, namespaceID, dagID, name string) error {
	_, _, err := r.insertVersionedDAG(ctx, namespaceID, name)
	return err
}

// UpdateDAG renames a DAG and bumps its version counter in-place.
// This is analogous to renameNamespace + "version++" for tracking edits.
func (r *DAGRepo) UpdateDAG(ctx context.Context, oldID, ns, newName string) (string, int, error) {
	var oldVersion int
	var oldName string

	err := r.DB.QueryRow(ctx, `
		SELECT name, version
		FROM dags
		WHERE id = $1 AND namespace = $2 AND deleted = FALSE
	`, oldID, ns).Scan(&oldName, &oldVersion)

	if err != nil {
		return "", 0, fmt.Errorf("dag not found: %w", err)
	}

	// Soft-delete old version
	_, err = r.DB.Exec(ctx, `
		UPDATE dags
		SET deleted = TRUE
		WHERE id = $1
	`, oldID)
	if err != nil {
		return "", 0, err
	}

	// Create new version
	newID, newVersion, err := r.insertVersionedDAG(ctx, ns, newName)
	if err != nil {
		return "", 0, err
	}

	// Copy job-level dependencies from old version → new version
	if err := r.CloneDependenciesToNewVersion(ctx, oldID, oldVersion, newVersion); err != nil {
		return "", 0, fmt.Errorf("failed to clone dependencies: %w", err)
	}

	return newID, newVersion, nil
}


func (r *DAGRepo) DeleteDAG(ctx context.Context, id string) error {
	var ns, name string

	// Locate namespace & name for the DAG being deleted
	err := r.DB.QueryRow(ctx, `
		SELECT namespace, name
		  FROM dags
		 WHERE id = $1;
	`, id).Scan(&ns, &name)
	if err != nil {
		return fmt.Errorf("dag not found: %w", err)
	}

	// Soft-delete all versions of this logical DAG
	_, err = r.DB.Exec(ctx, `
		UPDATE dags
		   SET deleted = TRUE
		 WHERE namespace = $1
		   AND name = $2;
	`, ns, name)

	return err
}

func (r *DAGRepo) CloneDependenciesToNewVersion(
	ctx context.Context,
	dagID string,
	oldVersion int,
	newVersion int,
) error {

	_, err := r.DB.Exec(ctx, `
		INSERT INTO job_dependencies (
			dag_id,
			dag_version,
			parent_job_id,
			child_job_id,
			dependency_type,
			deleted,
			created_at
		)
		SELECT
			dag_id,
			$3 AS dag_version,
			parent_job_id,
			child_job_id,
			dependency_type,
			FALSE,
			now()
		FROM job_dependencies
		WHERE dag_id = $1
		  AND dag_version = $2
		  AND deleted = FALSE
	`, dagID, oldVersion, newVersion)

	return err
}

