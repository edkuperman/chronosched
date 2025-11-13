package scheduler

import (
	"context"
	"log"

	"github.com/robfig/cron/v3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edkuperman/chronosched/internal/db"
)

type Scheduler struct {
	jobRepo *db.JobRepo
	dbPool  *pgxpool.Pool
	cron    *cron.Cron
}

func New(jobRepo *db.JobRepo, pool *pgxpool.Pool) *Scheduler {
	return &Scheduler{
		jobRepo: jobRepo,
		dbPool:  pool,
		cron:    cron.New(cron.WithSeconds()),
	}
}

// LoadAndRegister registers all job definitions that have a cron_spec set.
func (s *Scheduler) LoadAndRegister(ctx context.Context) error {
	rows, err := s.dbPool.Query(ctx, `
		SELECT def_id, cron_spec
		FROM job_definitions
		WHERE cron_spec IS NOT NULL`)
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
		_, err := s.cron.AddFunc(spec, func() {
			s.Tick(ctx, def)
		})
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

// Tick fires on cron tick for a definition and enqueues a new run.
func (s *Scheduler) Tick(ctx context.Context, defID string) {
	_, err := s.jobRepo.AddJob(ctx, "", defID, 0, nil, "{}") // dag-less periodic run
	if err != nil {
		log.Printf("[scheduler] failed to enqueue new run for def=%s: %v", defID, err)
		return
	}
	log.Printf("[scheduler] enqueued run for def=%s", defID)
}

func (s *Scheduler) Start() { s.cron.Start() }
func (s *Scheduler) Stop()  { <-s.cron.Stop().Done() }
