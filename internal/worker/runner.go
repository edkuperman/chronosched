package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/edkuperman/chronosched/internal/logger"

	"github.com/edkuperman/chronosched/internal/repository"
)

type Runner struct {
	BaseURL   string
	WorkerID  string
	Client    *http.Client
	PollEvery time.Duration
}

func NewRunner(baseURL, workerID string) *Runner {
	return &Runner{
		BaseURL:   baseURL,
		WorkerID:  workerID,
		Client:    &http.Client{Timeout: 10 * time.Second},
		PollEvery: 2 * time.Second,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.PollEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.pollOnce(ctx); err != nil {
				logger.Error(err, "worker poll error")
			}
		}
	}
}

type leaseRequest struct {
	WorkerID string `json:"worker_id"`
	MaxJobs  int    `json:"max_jobs"`
}

type leaseResponse struct {
	Items []repository.QueueItem `json:"items"`
}

func (r *Runner) pollOnce(ctx context.Context) error {
	reqBody, _ := json.Marshal(leaseRequest{
		WorkerID: r.WorkerID,
		MaxJobs:  4,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.BaseURL+"/internal/workers/lease", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.ReadAll(resp.Body)
		return nil // TODO: log and ignore
	}

	var lr leaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		return err
	}
	for _, item := range lr.Items {
		go r.executeJob(ctx, item)
	}
	return nil
}

type resultRequest struct {
	WorkerID string `json:"worker_id"`
	QueueID  int64  `json:"queue_id"`
	JobID    int64  `json:"job_id"`
	Success  bool   `json:"success"`
	Error    string `json:"error"`
}

func (r *Runner) executeJob(ctx context.Context, item repository.QueueItem) {
	// For now, just mark as succeeded immediately. In a real implementation,
	// this would fetch job details and execute the payload.
	reqBody, _ := json.Marshal(resultRequest{
		WorkerID: r.WorkerID,
		QueueID:  item.QueueID,
		JobID:    item.JobID,
		Success:  true,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.BaseURL+"/internal/workers/result", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.Client.Do(req)
	if err != nil {
		logger.Error(err, "report result error")
		return
	}
	resp.Body.Close()
}
