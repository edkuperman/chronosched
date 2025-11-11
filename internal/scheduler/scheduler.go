package scheduler

import (
	"context"
	"log"

	"github.com/edkuperman/chronosched/internal/db"
	"github.com/robfig/cron/v3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler struct {
	jobRepo *db.JobRepo
	dbPool  *pgxpool.Pool // replace with pgxpool.Pool or your wrapper
	cron    *cron.Cron
}

func New(jobRepo *db.JobRepo, pool *pgxpool.Pool) *Scheduler {
	return &Scheduler{
		jobRepo: jobRepo,
		dbPool:  pool,
		cron:    cron.New(cron.WithSeconds()),
	}
}

// Load all jobs that have cron_spec and register them
// Load all job definitions that have a cron_spec and register them
func (s *Scheduler) LoadAndRegister(ctx context.Context) error {
	rows, err := s.dbPool.Query(ctx, `
		SELECT def_id, cron_spec
		FROM job_definitions
		WHERE cron_spec IS NOT NULL;
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var defID, spec string
		if err := rows.Scan(&defID, &spec); err != nil {
			return err
		}

		def := defID
		log.Printf("[scheduler] registering definition %s (cron=%s)", def, spec)

		// Register CRON callback that enqueues a new job run from this definition
		s.cron.AddFunc(spec, func() {
			s.Tick(ctx, 0, def)
		})
	}

	return nil
}

// Tick() executes when a job's cron fires
func (s *Scheduler) Tick(ctx context.Context, jobID int64, defID string) {
	log.Printf("[scheduler] tick job=%d def=%s", jobID, defID)

	// Each tick creates a new run instance
	newJobID, err := s.jobRepo.AddJob(ctx, "", defID, 0, nil, "{}")
	if err != nil {
		log.Printf("[scheduler] failed to enqueue new run for job=%d: %v", jobID, err)
		return
	}

	log.Printf("[scheduler] spawned new run %d for job=%d def=%s", newJobID, jobID, defID)
}

// Start/Stop
func (s *Scheduler) Start() { s.cron.Start() }
func (s *Scheduler) Stop()  { <-s.cron.Stop().Done() }
