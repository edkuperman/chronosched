package db

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/edkuperman/chronosched/internal/dag"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/google/uuid"
)

type JobRepo struct{ DB *pgxpool.Pool }

func NewJobRepo(db *pgxpool.Pool) *JobRepo { return &JobRepo{DB: db} }

// -----------------------------------------------------------------------------
// Data models
// -----------------------------------------------------------------------------

type Job struct {
	ID          int64
	DagID       string
	DefID       string
	Kind        string
	Priority    int
	PayloadJSON []byte
	DueAt       time.Time
	LeaseOwner  *string
	LeaseUntil  *time.Time
}

type JobDefinition struct {
    DefID           string `json:"def_id"`
    Namespace       string `json:"namespace"`
    Name            string `json:"name"`
    Version         int    `json:"version"`
    Kind            string `json:"kind"`
    PayloadTemplate string `json:"payload_template"`
}

type JobListItem struct {
    ID     int64   `json:"id"`
    DefID  string  `json:"def_id"`
    DagID  *string `json:"dag_id"`
    Status string  `json:"status"`
}

// -----------------------------------------------------------------------------
// Definitions
// -----------------------------------------------------------------------------
func (r *JobRepo) AddDefinition(
	ctx context.Context,
	ns, name string,
	kind string,
	payload string,
	cronSpec *string,
	delayInterval *string,
) (string, int, error) {

	// Compute next version
	var maxVersion int
	if err := r.DB.QueryRow(ctx, `
		SELECT COALESCE(MAX(version), 0)
		FROM job_definitions
		WHERE namespace = $1 AND name = $2;
	`, ns, name).Scan(&maxVersion); err != nil {
		return "", 0, err
	}
	nextVersion := maxVersion + 1

	// Insert new version
	var defID string
	var actualVersion int
	err := r.DB.QueryRow(ctx, `
		INSERT INTO job_definitions(
			namespace, name, version, kind,
			payload_template, cron_spec, delay_interval
		)
		VALUES($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (namespace, name, version) DO NOTHING
		RETURNING def_id, version;
	`, ns, name, nextVersion, kind, payload, cronSpec, delayInterval).Scan(&defID, &actualVersion)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Tried to add existion definition.
			// Get the latest version.
			if err := r.DB.QueryRow(ctx, `
				SELECT def_id, version
				FROM job_definitions
				WHERE namespace = $1 AND name = $2
				ORDER BY version DESC
				LIMIT 1;
			`, ns, name).Scan(&defID, &actualVersion); err != nil {
				return "", 0, errors.Wrap(err, "Failed to get latest version from job_definitions")
			}
			return defID, actualVersion, nil
		}		
		return "", 0, err
	}

	return defID, actualVersion, nil
}

// -----------------------------------------------------------------------------
// Jobs
// -----------------------------------------------------------------------------

// AddJob enqueues or updates a job row referencing a definition.
// Semantics:
//   - If (dag_id, def_id) does not exist -> insert new job (version = 1, queued)
//     and call init_frontier_for_job.
//   - If it already exists -> update payload / priority / due_at, reset status
//     to 'queued', clear deleted, and bump version = version + 1.
// This mirrors "upsert" behavior similar to bulkUpsertDefinitions.
func (r *JobRepo) AddJob(
	ctx context.Context,
	dagID string,
	defID string,
	priority int,
	dueAt *time.Time,
	payload string,
) (int64, error) {

	// Convert dagID (string) -> *uuid.UUID (nullable)
	var dagUUID *uuid.UUID
	if dagID != "" {
		parsed, err := uuid.Parse(dagID)
		if err != nil {
			return 0, fmt.Errorf("invalid dag_id: %w", err)
		}
		dagUUID = &parsed
	}

	// First try to insert a new job; if (dag_id, def_id) already exists,
	// ON CONFLICT DO NOTHING will return no rows.
	var id int64
	err := r.DB.QueryRow(ctx, `
		WITH ins AS (
		  INSERT INTO jobs(dag_id, def_id, priority, due_at, payload_json)
		  VALUES($1, $2, $3, COALESCE($4, now()), COALESCE($5::jsonb, '{}'::jsonb))
		  ON CONFLICT (dag_id, def_id) DO NOTHING
		  RETURNING id
		)
		SELECT id FROM ins;
	`, dagUUID, defID, priority, dueAt, payload).Scan(&id)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Existing job for (dag_id, def_id) – update it in-place, bump version.
			if err := r.DB.QueryRow(ctx, `
				UPDATE jobs
				   SET priority = $3,
				       due_at = COALESCE($4, now()),
				       payload_json = COALESCE($5::jsonb, '{}'::jsonb),
				       status = 'queued',
				       deleted = FALSE,
				       version = version + 1
				 WHERE dag_id = $1
				   AND def_id = $2
				RETURNING id;
			`, dagUUID, defID, priority, dueAt, payload).Scan(&id); err != nil {
				return 0, err
			}
			// No need to re-init frontier; job already exists in DAG frontier.
			return id, nil
		}
		return 0, err
	}

	// Newly inserted job: initialize its frontier.
	if _, err := r.DB.Exec(ctx,
		`SELECT init_frontier_for_job($1, $2);`,
		id, dagUUID,
	); err != nil {
		return 0, err
	}

	return id, nil
}

// LoadDefinition loads a job definition by its def_id.
func (r *JobRepo) LoadDefinition(ctx context.Context, defID string) (*JobDefinition, error) {
    row := r.DB.QueryRow(ctx, `
        SELECT def_id, namespace, name, version, kind, payload_template
          FROM job_definitions
         WHERE def_id = $1;
    `, defID)

    var def JobDefinition
    if err := row.Scan(
        &def.DefID,
        &def.Namespace,
        &def.Name,
        &def.Version,
        &def.Kind,
        &def.PayloadTemplate,
    ); err != nil {
        return nil, err
    }

    return &def, nil
}

// Load returns a job by id, looking first in live jobs and then in jobs_history
func (r *JobRepo) Load(ctx context.Context, jobID int64) (*Job, error) {
	row := r.DB.QueryRow(ctx, `
		SELECT id, kind, payload_json
		FROM (
			SELECT j.id, d.kind, j.payload_json
			  FROM jobs j
			  JOIN job_definitions d ON j.def_id = d.def_id
			 WHERE j.id = $1
			UNION ALL
			SELECT j.id, d.kind, j.payload_json
			  FROM jobs_history j
			  JOIN job_definitions d ON j.def_id = d.def_id
			 WHERE j.id = $1
		) s
		LIMIT 1;
	`, jobID)

	var j Job
	if err := row.Scan(&j.ID, &j.Kind, &j.PayloadJSON); err != nil {
		return nil, err
	}
	return &j, nil
}

// DequeueReady atomically claims up to limit ready jobs and returns them (with Kind).
func (r *JobRepo) DequeueReady(ctx context.Context, limit int, workerID string, leaseDuration time.Duration) ([]Job, error) {
	rows, err := r.DB.Query(ctx, `
		WITH next_jobs AS (
			SELECT j.id
			FROM jobs j
			JOIN job_frontier f ON j.id = f.job_id
			WHERE j.status = 'queued'
			  AND f.ready = TRUE
			  AND j.due_at <= now()
			ORDER BY j.priority DESC, j.due_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs AS j
		SET status='running',
		    lease_owner=$2,
		    lease_until=now() + ($3::interval),
		    started_at=now()
		FROM next_jobs nj
		JOIN job_definitions d ON d.def_id = j.def_id
		WHERE j.id = nj.id
		RETURNING
		  j.id, j.dag_id, j.def_id, d.kind,
		  j.priority, j.payload_json, j.due_at, j.lease_owner, j.lease_until;
	`, limit, workerID, leaseDuration.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(
			&j.ID, &j.DagID, &j.DefID, &j.Kind,
			&j.Priority, &j.PayloadJSON, &j.DueAt, &j.LeaseOwner, &j.LeaseUntil,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// LoadBinary returns binary bytes stored in the job record (optional column).
func (r *JobRepo) LoadBinary(ctx context.Context, jobID int64) ([]byte, error) {
	row := r.DB.QueryRow(ctx, `SELECT binary_data FROM jobs WHERE id=$1;`, jobID)
	var bin []byte
	if err := row.Scan(&bin); err != nil {
		return nil, err
	}
	if len(bin) == 0 {
		return nil, errors.New("no binary data")
	}
	return bin, nil
}

// -----------------------------------------------------------------------------
// Dependencies & Status transitions
// -----------------------------------------------------------------------------

func (r *JobRepo) AddDependency(ctx context.Context, dagID string, parentID, childID int64, depType string) error {
	if parentID == childID {
		return fmt.Errorf("cannot add self-dependency for job %d", parentID)
	}
	if depType == "" {
		depType = "data"
	}

	// Load existing dependencies for this DAG
	dagUUID, err := uuid.Parse(dagID)
	if err != nil {
		return fmt.Errorf("invalid dag_id: %w", err)
	}

	rows, err := r.DB.Query(ctx, `
		SELECT parent_job_id, child_job_id
		FROM job_dependencies
		WHERE dag_id = $1;
	`, dagUUID)
	if err != nil {
		return fmt.Errorf("failed to load existing dependencies: %w", err)
	}
	defer rows.Close()

	// Build in-memory edge list for a local cycle check (defensive; DB trigger also enforces)
	var edges []dag.Edge
	for rows.Next() {
		var p, c int64
		if err := rows.Scan(&p, &c); err != nil {
			return err
		}
		edges = append(edges, dag.Edge{Src: fmt.Sprint(p), Dst: fmt.Sprint(c)})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Add the proposed new edge
	edges = append(edges, dag.Edge{Src: fmt.Sprint(parentID), Dst: fmt.Sprint(childID)})

	detector := dag.DFSDetector{}
	cycles, err := detector.DetectCycles(ctx, dag.EdgeCache(edges), dagID)
	if err != nil {
		return fmt.Errorf("cycle check failed: %w", err)
	}
	if len(cycles) > 0 {
		return fmt.Errorf("cycle detected: %v", cycles)
	}

	// Insert edge (DB trigger enforce_acyclic_closure() is an additional guard)
	_, err = r.DB.Exec(ctx, `
		INSERT INTO job_dependencies(dag_id, parent_job_id, child_job_id, dependency_type)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING;
	`, dagID, parentID, childID, depType)
	if err != nil {
		return fmt.Errorf("failed to insert dependency: %w", err)
	}
	return nil
}

func (r *JobRepo) MarkComplete(ctx context.Context, jobID int64) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE jobs
		   SET status='succeeded',
		       finished_at=now(),
		       lease_owner=NULL,
		       lease_until=NULL
		 WHERE id=$1;
	`, jobID)
	return err
}

func (r *JobRepo) MarkFail(ctx context.Context, jobID int64, msg string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE jobs
		   SET status='failed',
		       finished_at=now(),
		       last_error=$2,
		       lease_owner=NULL,
		       lease_until=NULL
		 WHERE id=$1;
	`, jobID, msg)
	return err
}

func (r *JobRepo) ListByNamespace(ctx context.Context, namespace string) ([]*JobListItem, error) {
    rows, err := r.DB.Query(ctx, `
        SELECT j.id, j.def_id, j.dag_id, j.status
        FROM jobs j
        JOIN job_definitions d ON j.def_id = d.def_id
        WHERE d.namespace = $1
        ORDER BY j.id;
    `, namespace)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    out := []*JobListItem{}

    for rows.Next() {
        var it JobListItem
        if err := rows.Scan(&it.ID, &it.DefID, &it.DagID, &it.Status); err != nil {
            return nil, err
        }
        out = append(out, &it)
    }
    return out, rows.Err()
}

	
