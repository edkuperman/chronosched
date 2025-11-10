package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edkuperman/chronosched/internal/db"
)

type Runner struct {
	// Dependencies
	DB     *pgxpool.Pool
	Sched  *db.SchedulerRepo
	Jobs   *db.JobRepo

	// Identity
	WorkerID string

	// Tuning
	PollEvery      time.Duration // how often to call FairDequeue
	MaxBatch       int           // max jobs per FairDequeue call
	Concurrency    int           // max parallel jobs in this process
	LeaseDuration  time.Duration // lease length on claim
	HeartbeatEvery time.Duration // how often to extend leases for running jobs

	// Internal
	muRunning sync.Mutex
	running   map[int64]struct{} // in-flight job ids, for heartbeat
}

func (r *Runner) Run(ctx context.Context) error {
	if r.Sched == nil {
		r.Sched = db.NewSchedulerRepo(r.DB)
	}
	if r.Jobs == nil {
		r.Jobs = db.NewJobRepo(r.DB)
	}
	if r.WorkerID == "" {
		r.WorkerID = defaultWorkerID()
	}
	if r.PollEvery == 0 {
		r.PollEvery = 800 * time.Millisecond
	}
	if r.MaxBatch <= 0 {
		r.MaxBatch = 8
	}
	if r.Concurrency <= 0 {
		r.Concurrency = 4
	}
	if r.LeaseDuration <= 0 {
		r.LeaseDuration = 10 * time.Minute
	}
	if r.HeartbeatEvery <= 0 {
		r.HeartbeatEvery = 30 * time.Second
	}

	log.Printf("[worker %s] starting (poll=%s, batch=%d, conc=%d, lease=%s)",
		r.WorkerID, r.PollEvery, r.MaxBatch, r.Concurrency, r.LeaseDuration)

	jobsCh := make(chan int64)
	resultsCh := make(chan result)
	sem := make(chan struct{}, r.Concurrency)

	r.running = make(map[int64]struct{})

	// 1) Poller: FairDequeue -> jobsCh
	pollCtx, pollCancel := context.WithCancel(ctx)
	go r.poller(pollCtx, jobsCh)

	// 2) Heartbeater: extends leases for in-flight jobs
	hbCtx, hbCancel := context.WithCancel(ctx)
	go r.heartbeater(hbCtx)

	// 3) Worker fan-out
	var wg sync.WaitGroup
	workerLoopDone := make(chan struct{})
	go func() {
		defer close(workerLoopDone)
		for {
			select {
			case <-ctx.Done():
				return
			case id, ok := <-jobsCh:
				if !ok {
					return
				}
				sem <- struct{}{}
				wg.Add(1)
				r.trackStart(id)
				go func(jobID int64) {
					defer wg.Done()
					defer func() { <-sem }()
					defer r.trackEnd(jobID)

					// Replace with actual execution for jobID (dispatch by kind if needed)
					if err := r.execute(ctx, jobID); err != nil {
						log.Printf("[worker %s] job %d FAILED: %v", r.WorkerID, jobID, err)
						resultsCh <- result{jobID: jobID, err: err}
						return
					}
					resultsCh <- result{jobID: jobID, err: nil}
				}(id)
			}
		}
	}()

	// 4) Results fan-in
	resultsDone := make(chan struct{})
	go func() {
		defer close(resultsDone)
		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-resultsCh:
				if !ok {
					return
				}
				if res.err != nil {
					// Mark fail + (optional) reschedule/backoff
					_ = r.Jobs.MarkFail(ctx, res.jobID, res.err.Error())
				} else {
					_ = r.Jobs.MarkComplete(ctx, res.jobID)
				}
			}
		}
	}()

	// === Shutdown path ===
	<-ctx.Done()
	log.Printf("[worker %s] shutting down...", r.WorkerID)

	// stop poller and heartbeater
	pollCancel()
	hbCancel()

	// stop accepting new jobs
	close(jobsCh)

	// wait for active goroutines to finish
	wg.Wait()
	close(resultsCh)

	<-resultsDone
	<-workerLoopDone

	log.Printf("[worker %s] stopped", r.WorkerID)
	return nil
}

type result struct {
	jobID int64
	err   error
}

// poller periodically calls FairDequeue and pushes job IDs to jobsCh.
func (r *Runner) poller(ctx context.Context, jobsCh chan<- int64) {
	t := time.NewTicker(r.PollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ids, err := r.Sched.FairDequeue(ctx, r.MaxBatch, r.WorkerID, r.LeaseDuration)
			if err != nil {
				log.Printf("[worker %s] FairDequeue error: %v", r.WorkerID, err)
				continue
			}
			for _, id := range ids {
				select {
				case jobsCh <- id:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// heartbeater extends leases for all currently running jobs.
func (r *Runner) heartbeater(ctx context.Context) {
	t := time.NewTicker(r.HeartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ids := r.snapshotRunning()
			if len(ids) == 0 {
				continue
			}
			// Direct SQL heartbeat to avoid extra repo surfacing; feel free to move into JobRepo.
			_, err := r.DB.Exec(ctx, `
				UPDATE jobs
				SET heartbeat_at = now(),
				    lease_until   = now() + $2::interval
				WHERE id = ANY($1)
				  AND status = 'running'
				  AND lease_owner = $3
			`, ids, r.LeaseDuration.String(), r.WorkerID)
			if err != nil {
				log.Printf("[worker %s] heartbeat error: %v", r.WorkerID, err)
			}
		}
	}
}

func (r *Runner) trackStart(id int64) {
	r.muRunning.Lock()
	r.running[id] = struct{}{}
	r.muRunning.Unlock()
}
func (r *Runner) trackEnd(id int64) {
	r.muRunning.Lock()
	delete(r.running, id)
	r.muRunning.Unlock()
}
func (r *Runner) snapshotRunning() []int64 {
	r.muRunning.Lock()
	defer r.muRunning.Unlock()
	out := make([]int64, 0, len(r.running))
	for id := range r.running {
		out = append(out, id)
	}
	return out
}

func (r *Runner) execute(ctx context.Context, jobID int64) error {
	j, err := r.Jobs.Load(ctx, jobID)
	if err != nil {
		return fmt.Errorf("load job %d: %w", jobID, err)
	}

	switch j.Kind {
	case "http":
		return r.execHTTP(ctx, j)
	case "cmd":
		return r.execCommand(ctx, j)
	case "binary":
		return r.execBinary(ctx, j)
	default:
		return fmt.Errorf("unknown job kind: %s", j.Kind)
	}
}

func defaultWorkerID() string {
    if id := os.Getenv("WORKER_ID"); id != "" {
        return id
    }
    h, _ := os.Hostname()
    return fmt.Sprintf("%s-%d", h, os.Getpid())
}

