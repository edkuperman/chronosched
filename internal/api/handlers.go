package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/edkuperman/chronosched/internal/repository"
	"github.com/go-chi/chi/v5"
)

type Handler struct {
	Repos *repository.Repos
}

func NewHandler(repos *repository.Repos) *Handler { return &Handler{Repos: repos} }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func parseInt64Param(r *http.Request, key string) (int64, error) {
	return strconv.ParseInt(chi.URLParam(r, key), 10, 64)
}

func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ===== Namespaces =====
func (h *Handler) listNamespaces(w http.ResponseWriter, r *http.Request) {
	items, err := h.Repos.Namespaces.List(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

type createNamespaceRequest struct {
	Name string `json:"name"`
}

func (h *Handler) createNamespace(w http.ResponseWriter, r *http.Request) {
	var req createNamespaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	ns, err := h.Repos.Namespaces.Create(r.Context(), req.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, ns)
}

func (h *Handler) getNamespace(w http.ResponseWriter, r *http.Request) {
	ns, err := h.Repos.Namespaces.GetByName(r.Context(), chi.URLParam(r, "name"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, ns)
}

// ===== Job definitions =====
type definitionRequest struct {
	NamespaceID     string               `json:"namespace_id"`
	Name            string               `json:"name"`
	Description     string               `json:"description"`
	Kind            string               `json:"kind"`
	PayloadTemplate json.RawMessage      `json:"payload_template"`
	Schedule        *repository.Schedule `json:"schedule,omitempty"`
	IsEnabled       *bool                `json:"is_enabled,omitempty"`
}

func (h *Handler) listDefinitions(w http.ResponseWriter, r *http.Request) {
	items, err := h.Repos.Definitions.ListByNamespace(r.Context(), chi.URLParam(r, "namespace_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) createDefinition(w http.ResponseWriter, r *http.Request) {
	var req definitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.NamespaceID == "" {
		http.Error(w, "namespace_id and name are required", http.StatusBadRequest)
		return
	}
	enabled := true
	if req.IsEnabled != nil {
		enabled = *req.IsEnabled
	}
	if req.Kind == "" {
		req.Kind = "cmd"
	}
	def, err := h.Repos.Definitions.Create(r.Context(), repository.JobDefinition{
		NamespaceID: req.NamespaceID, Name: req.Name, Description: req.Description, Kind: req.Kind,
		PayloadTemplate: req.PayloadTemplate, Schedule: req.Schedule, IsEnabled: enabled,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, def)
}

func (h *Handler) getDefinition(w http.ResponseWriter, r *http.Request) {
	def, err := h.Repos.Definitions.Get(r.Context(), chi.URLParam(r, "definition_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, def)
}

func (h *Handler) deleteDefinition(w http.ResponseWriter, r *http.Request) {
	if err := h.Repos.Definitions.Delete(r.Context(), chi.URLParam(r, "definition_id")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) updateDefinition(w http.ResponseWriter, r *http.Request) {
	existing, err := h.Repos.Definitions.Get(r.Context(), chi.URLParam(r, "definition_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	var req definitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	existing.Name = req.Name
	existing.Description = req.Description
	if req.Kind != "" {
		existing.Kind = req.Kind
	}
	if len(req.PayloadTemplate) > 0 {
		existing.PayloadTemplate = req.PayloadTemplate
	}
	existing.Schedule = req.Schedule
	if req.IsEnabled != nil {
		existing.IsEnabled = *req.IsEnabled
	}
	def, err := h.Repos.Definitions.Update(r.Context(), *existing)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, def)
}

func (h *Handler) setDefinitionEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	if err := h.Repos.Definitions.SetEnabled(r.Context(), chi.URLParam(r, "definition_id"), enabled); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) enableDefinition(w http.ResponseWriter, r *http.Request) {
	h.setDefinitionEnabled(w, r, true)
}
func (h *Handler) disableDefinition(w http.ResponseWriter, r *http.Request) {
	h.setDefinitionEnabled(w, r, false)
}
func (h *Handler) pauseDefinition(w http.ResponseWriter, r *http.Request) {
	if err := h.Repos.Definitions.SetPaused(r.Context(), chi.URLParam(r, "definition_id"), true); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) resumeDefinition(w http.ResponseWriter, r *http.Request) {
	if err := h.Repos.Definitions.SetPaused(r.Context(), chi.URLParam(r, "definition_id"), false); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) definitionUsages(w http.ResponseWriter, r *http.Request) {
	items, err := h.Repos.Definitions.ListUsages(r.Context(), chi.URLParam(r, "definition_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"definition_id": chi.URLParam(r, "definition_id"), "usages": items})
}

// ===== DAGs =====
func (h *Handler) listDAGs(w http.ResponseWriter, r *http.Request) {
	items, err := h.Repos.DAGs.ListByNamespace(r.Context(), chi.URLParam(r, "namespace_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, items)
}
func (h *Handler) createDAG(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	dag, err := h.Repos.DAGs.Create(r.Context(), chi.URLParam(r, "namespace_id"), req.Name, req.Description)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, dag)
}
func (h *Handler) getDAG(w http.ResponseWriter, r *http.Request) {
	dag, err := h.Repos.DAGs.Get(r.Context(), chi.URLParam(r, "dag_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, dag)
}

func (h *Handler) deleteDAG(w http.ResponseWriter, r *http.Request) {
	if err := h.Repos.DAGs.Delete(r.Context(), chi.URLParam(r, "dag_id")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listDAGVersions(w http.ResponseWriter, r *http.Request) {
	items, err := h.Repos.DAGs.ListVersions(r.Context(), chi.URLParam(r, "dag_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (h *Handler) createDAGVersion(w http.ResponseWriter, r *http.Request) {
	var req repository.DAGVersionCreateInput
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	v, err := h.Repos.DAGs.CreateVersion(r.Context(), chi.URLParam(r, "dag_id"), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *Handler) getDAGVersion(w http.ResponseWriter, r *http.Request) {
	v, err := h.Repos.DAGs.GetVersion(r.Context(), chi.URLParam(r, "dag_version_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (h *Handler) activateDAGVersion(w http.ResponseWriter, r *http.Request) {
	if err := h.Repos.DAGs.ActivateVersion(r.Context(), chi.URLParam(r, "dag_version_id")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) revertDAGVersion(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Activate    bool   `json:"activate"`
		VersionNote string `json:"version_note"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.VersionNote == "" {
		req.VersionNote = "revert copy"
	}
	v, err := h.Repos.DAGs.RevertVersion(r.Context(), chi.URLParam(r, "dag_version_id"), req.Activate, req.VersionNote)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

func (h *Handler) getDAGGraph(w http.ResponseWriter, r *http.Request) {
	g, err := h.Repos.DAGs.GetVersionGraph(r.Context(), chi.URLParam(r, "dag_version_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, g)
}

// ===== Runs =====
func (h *Handler) createRun(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DAGVersionID *string `json:"dag_version_id,omitempty"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	run, err := h.Repos.Runs.CreateManualRun(r.Context(), chi.URLParam(r, "dag_id"), req.DAGVersionID, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, run)
}
func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := h.Repos.Runs.ListByDAG(r.Context(), chi.URLParam(r, "dag_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}
func (h *Handler) getRun(w http.ResponseWriter, r *http.Request) {
	runID, err := parseInt64Param(r, "run_id")
	if err != nil {
		http.Error(w, "invalid run_id", http.StatusBadRequest)
		return
	}
	run, err := h.Repos.Runs.Get(r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, run)
}
func (h *Handler) listRunJobs(w http.ResponseWriter, r *http.Request) {
	runID, err := parseInt64Param(r, "run_id")
	if err != nil {
		http.Error(w, "invalid run_id", http.StatusBadRequest)
		return
	}
	jobs, err := h.Repos.Runs.ListJobs(r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}
func (h *Handler) getRunGraph(w http.ResponseWriter, r *http.Request) {
	runID, err := parseInt64Param(r, "run_id")
	if err != nil {
		http.Error(w, "invalid run_id", http.StatusBadRequest)
		return
	}
	graph, err := h.Repos.Runs.GetGraph(r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, graph)
}

// ===== Jobs =====
type restartJobRequest struct {
	Cascade *bool `json:"cascade,omitempty"`
}

func parseJobStatusesCSV(raw string) []repository.JobStatus {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]repository.JobStatus, 0, len(parts))
	for _, part := range parts {
		v := repository.JobStatus(strings.TrimSpace(part))
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func (h *Handler) listNamespaceProblemJobs(w http.ResponseWriter, r *http.Request) {
	namespaceID := chi.URLParam(r, "namespace_id")
	var dagID *string
	if raw := strings.TrimSpace(r.URL.Query().Get("dag_id")); raw != "" {
		dagID = &raw
	}
	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	items, err := h.Repos.Jobs.ListProblemJobs(r.Context(), namespaceID, dagID, parseJobStatusesCSV(r.URL.Query().Get("status")), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h *Handler) restartNamespaceJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := parseInt64Param(r, "job_id")
	if err != nil {
		http.Error(w, "invalid job_id", http.StatusBadRequest)
		return
	}
	req := restartJobRequest{}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	cascade := true
	if raw := strings.TrimSpace(r.URL.Query().Get("cascade")); raw != "" {
		if parsed, err := strconv.ParseBool(raw); err == nil {
			cascade = parsed
		}
	}
	if req.Cascade != nil {
		cascade = *req.Cascade
	}
	result, err := h.Repos.Jobs.RestartJob(r.Context(), chi.URLParam(r, "namespace_id"), jobID, repository.RestartJobOptions{Cascade: cascade})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) getJobReadiness(w http.ResponseWriter, r *http.Request) {
	jobID, err := parseInt64Param(r, "job_id")
	if err != nil {
		http.Error(w, "invalid job_id", http.StatusBadRequest)
		return
	}
	ready, err := h.Repos.Jobs.GetReadiness(r.Context(), jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, ready)
}

// ===== Worker gateway =====
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
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.MaxJobs <= 0 {
		req.MaxJobs = 4
	}
	items, err := h.Repos.Queue.Dequeue(r.Context(), req.WorkerID, req.MaxJobs, 30*time.Second)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, leaseResponse{Items: items})
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

func (h *Handler) reportDispatchResult(w http.ResponseWriter, r *http.Request) {
	var req dispatchResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Success {
		if err := h.Repos.Jobs.RecordDispatchAccepted(r.Context(), req.JobID, req.ExternalExecutionID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := h.Repos.Queue.Ack(r.Context(), req.QueueID, req.WorkerID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else if req.Retryable {
		if err := h.Repos.Jobs.RecordDispatchRetry(r.Context(), req.JobID, req.ReasonCode, req.ReasonDetail); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := h.Repos.Queue.Fail(r.Context(), req.QueueID, req.WorkerID, 10*time.Second); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if err := h.Repos.Jobs.RecordDispatchFailed(r.Context(), req.JobID, req.ReasonCode, req.ReasonDetail); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = h.Repos.Queue.Ack(r.Context(), req.QueueID, req.WorkerID)
	}
	if runID, err := h.Repos.Jobs.GetRunID(r.Context(), req.JobID); err == nil {
		_ = h.Repos.Runs.RefreshStatus(r.Context(), runID)
	}
	w.WriteHeader(http.StatusNoContent)
}

type jobEventRequest struct {
	Status              string     `json:"status"`
	ExternalExecutionID string     `json:"external_execution_id,omitempty"`
	HeartbeatAt         *time.Time `json:"heartbeat_at,omitempty"`
	ReasonCode          string     `json:"reason_code,omitempty"`
	ReasonDetail        string     `json:"reason_detail,omitempty"`
}

func (h *Handler) postJobEvent(w http.ResponseWriter, r *http.Request) {
	jobID, err := parseInt64Param(r, "job_id")
	if err != nil {
		http.Error(w, "invalid job_id", http.StatusBadRequest)
		return
	}
	var req jobEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	switch req.Status {
	case "started":
		err = h.Repos.Jobs.RecordStarted(r.Context(), jobID, req.ExternalExecutionID)
	case "heartbeat":
		hb := time.Now().UTC()
		if req.HeartbeatAt != nil {
			hb = req.HeartbeatAt.UTC()
		}
		err = h.Repos.Jobs.RecordHeartbeat(r.Context(), jobID, hb, req.ReasonDetail)
	case "succeeded":
		err = h.Repos.Jobs.RecordCompletion(r.Context(), jobID, true, req.ReasonCode, req.ReasonDetail)
	case "failed":
		err = h.Repos.Jobs.RecordCompletion(r.Context(), jobID, false, req.ReasonCode, req.ReasonDetail)
	default:
		http.Error(w, "unsupported status", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if runID, err := h.Repos.Jobs.GetRunID(r.Context(), jobID); err == nil {
		_ = h.Repos.Runs.RefreshStatus(r.Context(), runID)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) checkGlobalCycles(w http.ResponseWriter, r *http.Request) {
	resp, err := h.Repos.Admin.CheckGlobalCycles(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}
