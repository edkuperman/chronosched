package scheduler

import (
	"context"
	"log"
	"time"

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

        // Capture loop variable safely
        defIDCopy := defID

        log.Printf("[scheduler] registering definition %s (cron=%s)", defIDCopy, spec)

        _, err := s.cron.AddFunc(spec, func() {
            // Create a fresh cancellable context for each cron fire
            tickCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
            defer cancel()

            s.Tick(tickCtx, defIDCopy)
        })
        if err != nil {
            return err
        }
    }
    return rows.Err()
}


// Tick fires on cron tick for a definition and enqueues a new run.
func (s *Scheduler) Tick(ctx context.Context, defID string) {
    // Load the definition so we can use its metadata
    def, err := s.jobRepo.LoadDefinition(ctx, defID)
    if err != nil {
        log.Printf("scheduler: invalid definition %s: %v", defID, err)
        return
    }

    // Use definition payload template
    payload := def.PayloadTemplate
    if payload == "" {
        payload = "{}"
    }

    // Priority (future use)
    priority := 0

    // Create a scheduled job with NO DAG
    _, err = s.jobRepo.AddJob(ctx, "", defID, priority, nil, payload)
    if err != nil {
        log.Printf("scheduler: AddJob error for def %s: %v", defID, err)
        return
    }

    log.Printf("scheduler: enqueued scheduled job for %s (%s)", def.Name, def.DefID)
}


func (s *Scheduler) Start() { s.cron.Start() }
func (s *Scheduler) Stop()  { <-s.cron.Stop().Done() }
