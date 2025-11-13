package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/edkuperman/chronosched/internal/dag"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// -----------------------------------------------------------------------------
// Definitions
// -----------------------------------------------------------------------------

// AddDefinition inserts an immutable job definition (or returns the existing def_id).
// Handlers call this 6-arg form, so we keep it compatible. cron_spec / delay_interval
// remain NULL unless set by another path.
func (r *JobRepo) AddDefinition(
	ctx context.Context,
	ns, name string,
	version int,
	kind string,
	payload string,
) (string, error) {
	var defID string

	// Try to insert a new immutable definition. On conflict, do nothing and fetch existing id.
	err := r.DB.QueryRow(ctx, `
		INSERT INTO job_definitions(namespace, name, version, kind, payload_template, cron_spec, delay_interval)
		VALUES($1, $2, $3, $4, $5::jsonb, NULL, NULL)
		ON CONFLICT (namespace, name, version) DO NOTHING
		RETURNING def_id;
	`, ns, name, version, kind, payload).Scan(&defID)

	if errors.Is(err, pgx.ErrNoRows) {
		err = r.DB.QueryRow(ctx, `
			SELECT def_id
			  FROM job_definitions
			 WHERE namespace=$1 AND name=$2 AND version=$3;
		`, ns, name, version).Scan(&defID)
	}

	return defID, err
}

// (Optional future extension, if/when handlers pass these):
// func (r *JobRepo) AddDefinitionWithSchedule(ctx context.Context, ns, name string, version int, kind, payload string, cronSpec *string, delay *string) (string, error) { ... }

// -----------------------------------------------------------------------------
// Jobs
// -----------------------------------------------------------------------------

// AddJob enqueues a new job row referencing a definition and initializes its frontier record.
func (r *JobRepo) AddJob(ctx context.Context, dagID string, defID string, priority int, dueAt *time.Time, payload string) (int64, error) {
	row := r.DB.QueryRow(ctx, `
		WITH ins AS (
		  INSERT INTO jobs(dag_id, def_id, priority, due_at, payload_json)
		  VALUES(NULLIF($1,''), $2, $3, COALESCE($4, now()), COALESCE($5::jsonb, '{}'::jsonb))
		  RETURNING id
		)
		SELECT id FROM ins;
	`, dagID, defID, priority, dueAt, payload)

	var id int64
	if err := row.Scan(&id); err != nil {
		return 0, err
	}

	// Initialize frontier (in-degree/ready) for this job
	if _, err := r.DB.Exec(ctx, `SELECT init_frontier_for_job($1, NULLIF($2,'')::uuid);`, id, dagID); err != nil {
		return 0, err
	}
	return id, nil
}

// Load fetches minimal job metadata needed for execution (compatible with existing handlers).
func (r *JobRepo) Load(ctx context.Context, jobID int64) (*Job, error) {
	row := r.DB.QueryRow(ctx, `
		SELECT j.id, d.kind, j.payload_json
		  FROM jobs j
		  JOIN job_definitions d ON j.def_id = d.def_id
		 WHERE j.id = $1;
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

func (r *JobRepo) AddDependency(ctx context.Context, dagID string, parentID, childID int64) error {
	if parentID == childID {
		return fmt.Errorf("cannot add self-dependency for job %d", parentID)
	}

	// Load existing dependencies for this DAG
	rows, err := r.DB.Query(ctx, `
		SELECT parent_job_id, child_job_id
		  FROM job_dependencies
		 WHERE dag_id = $1;
	`, dagID)
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
		INSERT INTO job_dependencies(dag_id, parent_job_id, child_job_id)
		VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING;
	`, dagID, parentID, childID)
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
