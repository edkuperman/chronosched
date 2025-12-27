package scheduler

import (
	"context"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/edkuperman/chronosched/internal/logger"
	"github.com/edkuperman/chronosched/internal/repository"
)

// Scheduler is a lightweight time-based scheduler that promotes
// ready jobs from 'waiting' to 'queued' and enqueues them onto
// the worker queue. It is intentionally simple and built entirely
// on repository interfaces so that the storage layer remains abstract.
type Scheduler struct {
	repos     *repository.Repos
	cron      *cron.Cron
	batchSize int
}

// NewScheduler constructs a Scheduler with sane defaults.
// It wires a cron entry that runs every 5 seconds and calls
// the internal tick() method.
func NewScheduler(repos *repository.Repos) *Scheduler {
	c := cron.New(cron.WithSeconds())

	s := &Scheduler{
		repos:     repos,
		cron:      c,
		batchSize: 128,
	}

	// Drive the scheduler tick every 5 seconds.
	// '@every' syntax is supported by robfig/cron.
	_, err := c.AddFunc("@every 5s", func() {
		ctx := context.Background()
		if err := s.tick(ctx); err != nil {
			logger.Error(err, "scheduler tick error")
		}
	})
	if err != nil {
		logger.Error(err, "failed to register scheduler cron entry")
	}

	return s
}

// Run starts the underlying cron scheduler and blocks until the
// provided context is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	s.cron.Start()
	defer s.cron.Stop()

	// Optional initial tick on startup so we don't wait for the first 5s interval.
	if err := s.tick(ctx); err != nil && ctx.Err() == nil {
		logger.Error(err, "scheduler initial tick error")
	}

	<-ctx.Done()
	return ctx.Err()
}

// tick performs a single scheduling pass:
//   - find up to batchSize jobs that are in 'waiting' status
//     and due_at <= now
//   - mark them 'queued'
//   - enqueue them into the job_queue via QueueRepository
func (s *Scheduler) tick(ctx context.Context) error {
	now := time.Now()

	// Find jobs that are ready to run.
	jobs, err := s.repos.Jobs.FindDueWaiting(ctx, now, s.batchSize)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}

	for _, j := range jobs {
		// Best-effort handling for each job; we log and continue
		// on errors so that one bad job does not block others.
		if err := s.repos.Jobs.MarkQueued(ctx, j.ID); err != nil {
			logger.Error(err, "scheduler failed to mark job queued", "jobID", j.ID)
			continue
		}
		if err := s.repos.Queue.Enqueue(ctx, j.ID, j.DueAt, j.Priority); err != nil {
			logger.Error(err, "scheduler failed to enqueue job", "jobID", j.ID)
			continue
		}
		logger.Info("scheduler enqueued job", "jobID", j.ID, "dagID", j.DagID, "defID", j.DefID)
	}

	return nil
}
