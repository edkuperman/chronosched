package db

import (
	"context"
	"errors"
	"fmt"
	"time"

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


// -----------------------------------------------------------------------------
// Definitions
// -----------------------------------------------------------------------------

// AddDefinition inserts an immutable job definition (or returns the existing def_id).
// Handlers call this 6-arg form, so we keep it compatible. cron_spec / delay_interval
// remain NULL unless set by another path.
// AddDefinition inserts an immutable job definition. Versions are server-controlled.
// If version <= 0, the next version is computed from both live and history tables.
func (r *JobRepo) AddDefinition(
	ctx context.Context,
	ns, name string,
	version int,
	kind string,
	payload string,
	cronSpec *string,
	delayInterval *string,
) (string, error) {
	// If caller did not supply an explicit version, compute next version from both
	// job_definitions and job_definitions_history so we never reuse a version number.
	if version <= 0 {
		var current, archived int
		if err := r.DB.QueryRow(ctx, `
			SELECT COALESCE(MAX(version), 0)
			FROM job_definitions
			WHERE namespace = $1 AND name = $2;
		`, ns, name).Scan(&current); err != nil {
			return "", err
		}
		if err := r.DB.QueryRow(ctx, `
			SELECT COALESCE(MAX(version), 0)
			FROM job_definitions_history
			WHERE namespace = $1 AND name = $2;
		`, ns, name).Scan(&archived); err != nil {
			return "", err
		}
		if archived > current {
			current = archived
		}
		version = current + 1
	}

	var defID string
	err := r.DB.QueryRow(ctx, `
		INSERT INTO job_definitions(
			namespace,
			name,
			version,
			kind,
			payload_template,
			cron_spec,
			delay_interval
		)
		VALUES($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (namespace, name, version) DO NOTHING
		RETURNING def_id;
	`, ns, name, version, kind, payload, cronSpec, delayInterval).Scan(&defID)

	// If no row was inserted because (namespace,name,version) already exists,
	// return the existing immutable definition's ID instead.
	if errors.Is(err, pgx.ErrNoRows) {
		if err = r.DB.QueryRow(ctx, `
			SELECT def_id
			FROM job_definitions
			WHERE namespace=$1 AND name=$2 AND version=$3;
		`, ns, name, version).Scan(&defID); err != nil {
			return "", err
		}
	}

	return defID, err
}


// (Optional future extension, if/when handlers pass these):
// func (r *JobRepo) AddDefinitionWithSchedule(ctx context.Context, ns, name string, version int, kind, payload string, cronSpec *string, delay *string) (string, error) { ... }

// -----------------------------------------------------------------------------
// Jobs
// -----------------------------------------------------------------------------

// AddJob enqueues a new job row referencing a definition and initializes its frontier record.
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

    row := r.DB.QueryRow(ctx, `
        WITH ins AS (
          INSERT INTO jobs(dag_id, def_id, priority, due_at, payload_json)
          VALUES($1, $2, $3, COALESCE($4, now()), COALESCE($5::jsonb, '{}'::jsonb))
          RETURNING id
        )
        SELECT id FROM ins;
    `, dagUUID, defID, priority, dueAt, payload)

    var id int64
    if err := row.Scan(&id); err != nil {
        return 0, err
    }

    // Frontier update: handle NULL dag_id gracefully
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
