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

type dispatchAck struct {
	Accepted            bool   `json:"accepted"`
	ExternalExecutionID string `json:"external_execution_id,omitempty"`
	Message             string `json:"message,omitempty"`
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

type dispatchResultRequest struct {
	WorkerID            string `json:"worker_id"`
	QueueID             int64  `json:"queue_id"`
	JobID               int64  `json:"job_id"`
	Success             bool   `json:"success"`
	Retryable           bool   `json:"retryable"`
	ReasonCode          string `json:"reason_code,omitempty"`
	ReasonDetail        string `json:"reason_detail,omitempty"`
	ExternalExecutionID string `json:"external_execution_id,omitempty"`
}

func (r *Runner) reportDispatchResult(ctx context.Context, item repository.QueueItem, success, retryable bool, reasonCode, reasonDetail, externalExecutionID string) {
	reqBody, _ := json.Marshal(dispatchResultRequest{WorkerID: r.WorkerID, QueueID: item.QueueID, JobID: item.JobID, Success: success, Retryable: retryable, ReasonCode: reasonCode, ReasonDetail: reasonDetail, ExternalExecutionID: externalExecutionID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.BaseURL+"/internal/workers/dispatch-result", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.Client.Do(req)
	if err != nil {
		logger.Error(err, "report dispatch result error")
		return
	}
	_ = resp.Body.Close()
}

func (r *Runner) executeREST(ctx context.Context, item repository.QueueItem) (bool, bool, string, string, string) {
	var p restPayload
	if err := json.Unmarshal(item.Payload, &p); err != nil {
		return false, false, "invalid_payload", fmt.Sprintf("invalid rest payload: %v", err), ""
	}
	if p.URL == "" {
		return false, false, "invalid_payload", "rest payload url is required", ""
	}
	method := p.Method
	if method == "" {
		method = http.MethodPost
	}
	payloadBody := map[string]any{}
	for k, v := range p.Body {
		payloadBody[k] = v
	}
	payloadBody["chronosched"] = map[string]any{
		"job_id":        item.JobID,
		"callback_base": fmt.Sprintf("%s/api/v1/jobs/%d/events", r.BaseURL, item.JobID),
		"node_key":      item.NodeKey,
	}
	b, _ := json.Marshal(payloadBody)
	req, err := http.NewRequestWithContext(ctx, method, p.URL, bytes.NewReader(b))
	if err != nil {
		return false, true, "dispatch_request_error", err.Error(), ""
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Chronosched-Job-ID", fmt.Sprintf("%d", item.JobID))
	req.Header.Set("X-Chronosched-Node-Key", item.NodeKey)
	req.Header.Set("X-Chronosched-Definition-ID", item.Definition)
	req.Header.Set("X-Chronosched-Callback-Base", fmt.Sprintf("%s/api/v1/jobs/%d/events", r.BaseURL, item.JobID))
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}
	resp, err := r.Client.Do(req)
	if err != nil {
		return false, true, "dispatch_http_error", err.Error(), ""
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 500 {
		return false, true, "dispatch_http_error", fmt.Sprintf("dispatch returned %d: %s", resp.StatusCode, string(data)), ""
	}
	if resp.StatusCode >= 400 {
		return false, false, "dispatch_rejected", fmt.Sprintf("dispatch returned %d: %s", resp.StatusCode, string(data)), ""
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return true, false, "", "", ""
	}
	var ack dispatchAck
	if err := json.Unmarshal(data, &ack); err != nil {
		return false, false, "dispatch_protocol_error", fmt.Sprintf("invalid dispatch response: %v", err), ""
	}
	if !ack.Accepted {
		reason := ack.Message
		if reason == "" {
			reason = "dispatch not accepted"
		}
		return false, false, "dispatch_rejected", reason, ack.ExternalExecutionID
	}
	return true, false, "", "", ack.ExternalExecutionID
}

func (r *Runner) executeJob(ctx context.Context, item repository.QueueItem) {
	success := true
	retryable := false
	reasonCode := ""
	reasonDetail := ""
	externalExecutionID := ""
	switch item.Kind {
	case "rest":
		success, retryable, reasonCode, reasonDetail, externalExecutionID = r.executeREST(ctx, item)
	default:
		success = false
		retryable = false
		reasonCode = "unsupported_job_kind"
		reasonDetail = fmt.Sprintf("unsupported job kind: %s", item.Kind)
	}
	r.reportDispatchResult(ctx, item, success, retryable, reasonCode, reasonDetail, externalExecutionID)
}
