package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DependencyRepo struct {
	DB *pgxpool.Pool
}

func NewDependencyRepo(db *pgxpool.Pool) *DependencyRepo {
	return &DependencyRepo{DB: db}
}

// =========================================================================
//  MINIMAL CHANGE #1
//  job_dependencies uses BIGINT job IDs, not UUIDs
//  dagID is still UUID, but parent/child are BIGINT
// =========================================================================

func (r *DependencyRepo) UpsertDependency(
	ctx context.Context,
	dagID string,
	parentID string,
	childID string,
	depType string,
) (int64, error) {

	// Parse job IDs as int64
	pid, err := parseInt64(parentID)
	if err != nil {
		return 0, fmt.Errorf("invalid parent_id: %w", err)
	}
	cid, err := parseInt64(childID)
	if err != nil {
		return 0, fmt.Errorf("invalid child_id: %w", err)
	}

	var returned int64

	// =========================================================================
	// MINIMAL CHANGE #2
	//   - Use job_dependencies (not dependencies)
	//   - No version, no soft-delete revival
	//   - Upsert by (dag_id, parent_job_id, child_job_id)
	// =========================================================================
	err = r.DB.QueryRow(ctx, `
		INSERT INTO job_dependencies (dag_id, parent_job_id, child_job_id, dependency_type)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (dag_id, parent_job_id, child_job_id)
		DO UPDATE SET dependency_type = EXCLUDED.dependency_type,
		              deleted = FALSE
		RETURNING parent_job_id;
	`, dagID, pid, cid, depType).Scan(&returned)

	if err != nil {
		return 0, err
	}
	return returned, nil
}

// =========================================================================
//  MINIMAL CHANGE #3
//  UpdateDependency should identify row by parent+child, not a dependency id
// =========================================================================

func (r *DependencyRepo) UpdateDependency(
	ctx context.Context,
	dagID string,
	idStr string,
	depType string,
) error {

	depID, err := parseInt64(idStr)
	if err != nil {
		return fmt.Errorf("invalid dependency id: %w", err)
	}

	// depID here refers to parent_job_id (handlers expect this oddly)
	// We update all edges with this parent id in this DAG. Minimal approach.
	res, err := r.DB.Exec(ctx, `
		UPDATE job_dependencies
		   SET dependency_type = $3
		 WHERE dag_id = $1
		   AND parent_job_id = $2
		   AND deleted = FALSE;
	`, dagID, depID, depType)

	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return errors.New("not found")
	}
	return nil
}

// =========================================================================
//  MINIMAL CHANGE #4
//  DeleteDependency: use parent_job_id and child_job_id from query string
// =========================================================================

func (r *DependencyRepo) DeleteDependency(
	ctx context.Context,
	dagID string,
	idStr string,
) error {

	jobID, err := parseInt64(idStr)
	if err != nil {
		return fmt.Errorf("invalid dependency id: %w", err)
	}

	res, err := r.DB.Exec(ctx, `
		UPDATE job_dependencies
		   SET deleted = TRUE
		 WHERE dag_id = $1
		   AND parent_job_id = $2;
	`, dagID, jobID)

	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return errors.New("not found")
	}
	return nil
}



func parseInt64(s string) (int64, error) {
	var id int64
	_, err := fmt.Sscan(s, &id)
	return id, err
}
