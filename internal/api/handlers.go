package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/edkuperman/chronosched/internal/db"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handlers wires up all API endpoints.
type Handlers struct {
	DB  *pgxpool.Pool
	dag *db.DAGRepo
	job *db.JobRepo
	sch *db.SchedulerRepo
}

func NewHandlers(pool *pgxpool.Pool) *Handlers {
	return &Handlers{
		DB:  pool,
		dag: db.NewDAGRepo(pool),
		job: db.NewJobRepo(pool),
		sch: db.NewSchedulerRepo(pool),
	}
}

// Utility functions
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// Endpoints

// Health check (wired inline in router)
func (h *Handlers) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// POST /dags
func (h *Handlers) createDAG(w http.ResponseWriter, r *http.Request) {
	type req struct {
		ID        string `json:"id"`
		Namespace string `json:"namespace"`
		Name      string `json:"name"`
	}

	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, fmt.Errorf("invalid request: %w", err))
		return
	}

	if body.ID == "" {
		writeErr(w, 400, fmt.Errorf("id required"))
		return
	}

	if body.Namespace == "" {
		writeErr(w, 400, fmt.Errorf("namespace required"))
		return
	}

	if body.Name == "" {
		writeErr(w, 400, fmt.Errorf("name required"))
		return
	}

	if err := h.dag.CreateDAG(r.Context(), body.ID, body.Namespace, body.Name); err != nil {
		writeErr(w, 500, fmt.Errorf("failed to create DAG: %w", err))
		return
	}

	writeJSON(w, 201, map[string]any{
		"id":      body.ID,
		"status":  "created",
		"message": "DAG created successfully",
	})
}


// DELETE /dags/{dagId}
func (h *Handlers) deleteDAG(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
	if dagID == "" {
		writeErr(w, 400, fmt.Errorf("dagId required"))
		return
	}
	if err := h.dag.DeleteDAG(r.Context(), dagID); err != nil {
		writeErr(w, 500, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /definitions
func (h *Handlers) createDefinition(w http.ResponseWriter, r *http.Request) {
	type req struct {
		Namespace       string          `json:"namespace"`
		Name            string          `json:"name"`
		Version         int             `json:"version"`
		Kind            string          `json:"kind"`
		PayloadTemplate json.RawMessage `json:"payloadTemplate"`
	}

	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, fmt.Errorf("invalid request: %w", err))
		return
	}

	defID, err := h.job.AddDefinition(
		r.Context(),
		body.Namespace,
		body.Name,
		body.Version,
		body.Kind,
		string(body.PayloadTemplate),
	)

	// Case 1: Database or internal error
	if err != nil {
		writeErr(w, 500, fmt.Errorf("database error: %w", err))
		return
	}

	// Case 2: No ID returned — indicates duplicate or logic error
	if defID == "" {
		writeJSON(w, 409, map[string]string{
			"error": "duplicate definition (namespace, name, version already exists)",
		})
		return
	}

	// Case 3: Success
	writeJSON(w, 201, map[string]string{
		"status": "created",
		"def_id": defID,
	})
}

// POST /dags/{dagId}/jobs
func (h *Handlers) addJob(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
	type req struct {
		DefID    string          `json:"defId"`
		Priority int             `json:"priority"`
		DueAt    *time.Time      `json:"dueAt"`
		Payload  json.RawMessage `json:"payload"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	id, err := h.job.AddJob(r.Context(), dagID, body.DefID, body.Priority, body.DueAt, string(body.Payload))
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 201, map[string]any{"id": id})
}

// POST /dags/{dagId}/dependencies
func (h *Handlers) addDependency(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
	type req struct {
		ParentJobID int64 `json:"parentJobId"`
		ChildJobID  int64 `json:"childJobId"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	if err := h.job.AddDependency(r.Context(), dagID, body.ParentJobID, body.ChildJobID); err != nil {
		writeErr(w, 409, err)
		return
	}
	writeJSON(w, 201, map[string]string{"status": "ok"})
}

// POST /jobs/{jobId}/complete
func (h *Handlers) complete(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobId")
	var id int64
	if _, err := fmt.Sscan(jobID, &id); err != nil {
		writeErr(w, 400, fmt.Errorf("invalid job id"))
		return
	}
	if err := h.job.MarkComplete(r.Context(), id); err != nil {
		writeErr(w, 500, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /jobs/{jobId}/fail
func (h *Handlers) fail(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobId")
	var id int64
	if _, err := fmt.Sscan(jobID, &id); err != nil {
		writeErr(w, 400, fmt.Errorf("invalid job id"))
		return
	}
	type req struct{ Error string }
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	if err := h.job.MarkFail(r.Context(), id, body.Error); err != nil {
		writeErr(w, 500, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) getJob(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "jobId")
	jobID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeErr(w, 400, fmt.Errorf("invalid job ID"))
		return
	}

	job, err := h.job.Load(r.Context(), jobID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, 404, fmt.Errorf("job not found"))
			return
		}
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, map[string]any{
		"id":      job.ID,
		"kind":    job.Kind,
		"payload": json.RawMessage(job.PayloadJSON),
	})
}

