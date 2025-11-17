package scheduler

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"

	"github.com/edkuperman/chronosched/internal/db"
)

// Scheduler wires CRON-based scheduling of job definitions into DAG-scoped jobs.
//
// Model:
//   - Source of truth for schedules is job_definitions.cron_spec
//   - Every scheduled execution is enqueued as a job in an existing DAG
//   - Scheduler never creates special "cron DAGs"; it uses the DAG that matches
//     the definition's (namespace, name), picking the latest non-deleted version.
type Scheduler struct {
	dbPool *pgxpool.Pool
	cron   *cron.Cron
}

// New constructs a Scheduler. Typically used from cmd/server/main.go:
//
//   sched := scheduler.New(h.JobRepo(), pool)
//   if err := sched.LoadAndRegister(ctx); err != nil { ... }
//   go sched.Start()
//
func New(_ *db.JobRepo, pool *pgxpool.Pool) *Scheduler {
	c := cron.New(cron.WithSeconds())
	return &Scheduler{
		dbPool: pool,
		cron:   c,
	}
}

// scheduledDefinition is a view of a cron-enabled job definition.
type scheduledDefinition struct {
	DefID       string
	NamespaceID string
	Name        string
	PayloadJSON string
	CronSpec    string
}

// LoadAndRegister scans cron-enabled definitions and registers cron jobs
// with robfig/cron. Each cron callback enqueues a DAG-scoped job.
func (s *Scheduler) LoadAndRegister(ctx context.Context) error {
	const q = `
		SELECT
			def_id::text,
			namespace::text,
			name,
			payload_template::text,
			cron_spec
		FROM job_definitions
		WHERE deleted = FALSE
		  AND cron_spec IS NOT NULL
		  AND trim(cron_spec) <> ''
	`

	rows, err := s.dbPool.Query(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	registered := 0

	for rows.Next() {
		var d scheduledDefinition
		if err := rows.Scan(
			&d.DefID,
			&d.NamespaceID,
			&d.Name,
			&d.PayloadJSON,
			&d.CronSpec,
		); err != nil {
			return err
		}

		if d.PayloadJSON == "" || d.PayloadJSON == "null" {
			d.PayloadJSON = "{}"
		}

		defCopy := d // avoid closure capturing loop variable

		_, err := s.cron.AddFunc(d.CronSpec, func() {
			s.enqueueScheduledJob(context.Background(), defCopy)
		})
		if err != nil {
			log.Printf("scheduler: failed to register cron for def %s (%s): %v",
				d.Name, d.DefID, err)
			continue
		}

		registered++
	}

	if err := rows.Err(); err != nil {
		return err
	}

	log.Printf("scheduler: registered %d cron definition(s)", registered)
	return nil
}

// enqueueScheduledJob is invoked by robfig/cron callbacks.
//
// It finds the *existing* DAG in the definition's namespace with the same
// name as the definition (latest non-deleted version), and enqueues a job
// in that DAG using the definition's payload template.
func (s *Scheduler) enqueueScheduledJob(ctx context.Context, def scheduledDefinition) {
	// 1. Look up the DAG for this definition: (namespace, name), latest version, not deleted.
	const selectDAG = `
		SELECT id::text
		FROM dags
		WHERE namespace = $1
		  AND name = $2
		  AND deleted = FALSE
		ORDER BY version DESC
		LIMIT 1
	`

	var dagID string
	if err := s.dbPool.QueryRow(ctx, selectDAG, def.NamespaceID, def.Name).Scan(&dagID); err != nil {
		log.Printf("scheduler: no DAG found for cron def %s (%s) in namespace %s: %v",
			def.Name, def.DefID, def.NamespaceID, err)
		return
	}

	payload := def.PayloadJSON
	if payload == "" || payload == "null" {
		payload = "{}"
	}

	const status = "queued"
	const priority = 0

	const insertJob = `
		INSERT INTO jobs (
			dag_id,
			def_id,
			status,
			priority,
			due_at,
			payload_json
		)
		VALUES (
			$1,
			$2,
			$3,
			$4,
			now(),
			$5::jsonb
		)
		RETURNING id
	`

	var jobID int64
	if err := s.dbPool.QueryRow(ctx, insertJob, dagID, def.DefID, status, priority, payload).Scan(&jobID); err != nil {
		log.Printf("scheduler: insert job error for def %s: %v", def.DefID, err)
		return
	}

	// Initialize frontier: if the job has no parents, it becomes ready immediately.
	_, _ = s.dbPool.Exec(ctx, `SELECT init_frontier_for_job($1, $2)`, jobID, dagID)

	log.Printf("scheduler: enqueued scheduled job id=%d for def %s (%s) in DAG %s",
		jobID, def.Name, def.DefID, dagID)
}

// Start begins executing registered cron schedules.
func (s *Scheduler) Start() {
	s.cron.Start()
}

// Stop stops the scheduler and waits for any running jobs to finish.
func (s *Scheduler) Stop() {
	<-s.cron.Stop().Done()
}
