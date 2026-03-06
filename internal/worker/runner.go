package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

type restPayload struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    map[string]any    `json:"body,omitempty"`
}

type callbackResult struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

func NewRunner(baseURL, workerID string) *Runner {
	return &Runner{
		BaseURL:   baseURL,
		WorkerID:  workerID,
		Client:    &http.Client{Timeout: 10 * time.Second},
		PollEvery: 1 * time.Second,
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
	reqBody, _ := json.Marshal(leaseRequest{WorkerID: r.WorkerID, MaxJobs: 4})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.BaseURL+"/internal/workers/lease", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.ReadAll(resp.Body)
		return nil
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

func (r *Runner) reportResult(ctx context.Context, item repository.QueueItem, success bool, errMsg string) {
	reqBody, _ := json.Marshal(resultRequest{WorkerID: r.WorkerID, QueueID: item.QueueID, JobID: item.JobID, Success: success, Error: errMsg})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.BaseURL+"/internal/workers/result", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.Client.Do(req)
	if err != nil {
		logger.Error(err, "report result error")
		return
	}
	_ = resp.Body.Close()
}

func (r *Runner) executeREST(ctx context.Context, item repository.QueueItem) (bool, string) {
	var p restPayload
	if err := json.Unmarshal(item.Payload, &p); err != nil {
		return false, fmt.Sprintf("invalid rest payload: %v", err)
	}
	if p.URL == "" {
		return false, "rest payload url is required"
	}
	method := p.Method
	if method == "" {
		method = http.MethodPost
	}
	var body io.Reader
	if p.Body != nil {
		b, _ := json.Marshal(p.Body)
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.URL, body)
	if err != nil {
		return false, err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Chronosched-Job-ID", fmt.Sprintf("%d", item.JobID))
	req.Header.Set("X-Chronosched-Node-Key", item.NodeKey)
	req.Header.Set("X-Chronosched-Definition-ID", item.Definition)
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return false, fmt.Sprintf("callback returned %d: %s", resp.StatusCode, string(data))
	}
	var result callbackResult
	if len(bytes.TrimSpace(data)) == 0 {
		return true, ""
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return false, fmt.Sprintf("invalid callback response: %v", err)
	}
	if !result.Success {
		if result.Error != "" {
			return false, result.Error
		}
		if result.Message != "" {
			return false, result.Message
		}
		return false, "callback reported failure"
	}
	return true, result.Message
}

func (r *Runner) executeJob(ctx context.Context, item repository.QueueItem) {
	success := true
	errMsg := ""
	switch item.Kind {
	case "rest":
		success, errMsg = r.executeREST(ctx, item)
	default:
		success = false
		errMsg = fmt.Sprintf("unsupported job kind: %s", item.Kind)
	}
	r.reportResult(ctx, item, success, errMsg)
}
