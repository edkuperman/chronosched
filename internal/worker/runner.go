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

// Runner executes jobs in parallel, periodically polling the database for ready jobs.
type Runner struct {
	DB     *pgxpool.Pool
	Jobs   *db.JobRepo
	WorkerID string

	// tuning parameters
	PollEvery      time.Duration // how often to call DequeueReady
	MaxBatch       int           // max jobs dequeued per poll
	Concurrency    int           // max concurrent workers
	LeaseDuration  time.Duration // job lease time
	HeartbeatEvery time.Duration // lease renewal frequency

	// internal
	muRunning sync.Mutex
	running   map[int64]struct{}
}

// Run launches the polling loop and worker goroutines.
func (r *Runner) Run(ctx context.Context) error {
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

	jobsCh := make(chan db.Job)
	resultsCh := make(chan result)
	sem := make(chan struct{}, r.Concurrency)
	r.running = make(map[int64]struct{})

	// 1. Poller: periodically fetch ready jobs
	pollCtx, pollCancel := context.WithCancel(ctx)
	go r.poller(pollCtx, jobsCh)

	// 2. Heartbeater: periodically renew leases for running jobs
	hbCtx, hbCancel := context.WithCancel(ctx)
	go r.heartbeater(hbCtx)

	// 3. Worker fan-out
	var wg sync.WaitGroup
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case j, ok := <-jobsCh:
				if !ok {
					return
				}
				sem <- struct{}{}
				wg.Add(1)
				r.trackStart(j.ID)
				go func(job db.Job) {
					defer wg.Done()
					defer func() { <-sem }()
					defer r.trackEnd(job.ID)

					if err := r.execute(ctx, job); err != nil {
						log.Printf("[worker %s] job %d FAILED: %v", r.WorkerID, job.ID, err)
						resultsCh <- result{jobID: job.ID, err: err}
						return
					}
					resultsCh <- result{jobID: job.ID, err: nil}
				}(j)
			}
		}
	}()

	// 4. Results handler: update job status
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-resultsCh:
				if !ok {
					return
				}
				if res.err != nil {
					_ = r.Jobs.MarkFail(ctx, res.jobID, res.err.Error())
				} else {
					_ = r.Jobs.MarkComplete(ctx, res.jobID)
				}
			}
		}
	}()

	// === Shutdown ===
	<-ctx.Done()
	log.Printf("[worker %s] shutting down...", r.WorkerID)
	pollCancel()
	hbCancel()
	close(jobsCh)
	wg.Wait()
	close(resultsCh)
	log.Printf("[worker %s] stopped", r.WorkerID)
	return nil
}

// poller periodically fetches ready jobs.
func (r *Runner) poller(ctx context.Context, jobsCh chan<- db.Job) {
	ticker := time.NewTicker(r.PollEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			jobs, err := r.Jobs.DequeueReady(ctx, r.MaxBatch, r.WorkerID, r.LeaseDuration)
			if err != nil {
				log.Printf("[worker %s] DequeueReady error: %v", r.WorkerID, err)
				continue
			}
			for _, j := range jobs {
				select {
				case jobsCh <- j:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// heartbeater extends leases for in-flight jobs.
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
			_, err := r.DB.Exec(ctx, `
				UPDATE jobs
				   SET heartbeat_at = now(),
				       lease_until = now() + $2::interval
				 WHERE id = ANY($1)
				   AND status = 'running'
				   AND lease_owner = $3;
			`, ids, r.LeaseDuration.String(), r.WorkerID)
			if err != nil {
				log.Printf("[worker %s] heartbeat error: %v", r.WorkerID, err)
			}
		}
	}
}

// execute dispatches job execution based on its Kind.
func (r *Runner) execute(ctx context.Context, j db.Job) error {
	switch j.Kind {
	case "http":
		return r.execHTTP(ctx, j)
	case "binary":
		return r.execBinary(ctx, j)
	default:
		return fmt.Errorf("unknown job kind: %s", j.Kind)
	}
}

// internal tracking helpers
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

type result struct {
	jobID int64
	err   error
}

func defaultWorkerID() string {
	if id := os.Getenv("WORKER_ID"); id != "" {
		return id
	}
	h, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", h, os.Getpid())
}
