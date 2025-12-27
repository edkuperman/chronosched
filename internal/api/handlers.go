package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/edkuperman/chronosched/internal/repository"
)

type Handler struct {
	Repos *repository.Repos
}

func NewHandler(repos *repository.Repos) *Handler {
	return &Handler{Repos: repos}
}

type createJobRequest struct {
	DagID    string          `json:"dag_id"`
	DefID    string          `json:"def_id"`
	Priority int             `json:"priority"`
	Payload  json.RawMessage `json:"payload"`
}

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Priority == 0 {
		req.Priority = 1
	}
	job, err := h.Repos.Jobs.Create(r.Context(), req.DagID, req.DefID, req.Payload, req.Priority)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := h.Repos.Jobs.MarkQueued(r.Context(), job.ID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := h.Repos.Queue.Enqueue(r.Context(), job.ID, time.Now(), job.Priority); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(job)
}

type leaseRequest struct {
	WorkerID string `json:"worker_id"`
	MaxJobs  int    `json:"max_jobs"`
}

type leaseResponse struct {
	Items []repository.QueueItem `json:"items"`
}

func (h *Handler) leaseJobs(w http.ResponseWriter, r *http.Request) {
	var req leaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.MaxJobs <= 0 {
		req.MaxJobs = 8
	}
	items, err := h.Repos.Queue.Dequeue(r.Context(), req.WorkerID, req.MaxJobs, 30*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(leaseResponse{Items: items})
}

type resultRequest struct {
	WorkerID string `json:"worker_id"`
	QueueID  int64  `json:"queue_id"`
	JobID    int64  `json:"job_id"`
	Success  bool   `json:"success"`
	Error    string `json:"error"`
}

func (h *Handler) reportResult(w http.ResponseWriter, r *http.Request) {
	var req resultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Success {
		if err := h.Repos.Jobs.MarkSucceeded(r.Context(), req.JobID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := h.Repos.Queue.Ack(r.Context(), req.QueueID, req.WorkerID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if err := h.Repos.Jobs.MarkFailed(r.Context(), req.JobID, req.Error); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := h.Repos.Queue.Fail(r.Context(), req.QueueID, req.WorkerID, 10*time.Second); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
