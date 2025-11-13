package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/edkuperman/chronosched/internal/dag"
	"github.com/edkuperman/chronosched/internal/db"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handlers wires up all API endpoints.
type Handlers struct {
	db  *pgxpool.Pool
	dag *db.DAGRepo
	job *db.JobRepo
	sch *db.SchedulerRepo
	ns  *db.NamespaceRepo
}

func NewHandlers(pool *pgxpool.Pool) *Handlers {
	return &Handlers{
		db:  pool,
		dag: db.NewDAGRepo(pool),
		job: db.NewJobRepo(pool),
		sch: db.NewSchedulerRepo(pool),
		ns:  db.NewNamespaceRepo(pool),
	}
}

// Health check (wired inline in router)
func (h *Handlers) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// ===== Namespace Handlers =====

func (h *Handlers) listNamespaces(w http.ResponseWriter, r *http.Request) {
	nsRepo := h.ns
	if nsRepo == nil {
		nsRepo = db.NewNamespaceRepo(h.db)
	}
	ns, err := nsRepo.List(r.Context())
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, ns)
}

func (h *Handlers) createNamespace(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	if body.Name == "" {
		writeErr(w, 400, fmt.Errorf("name required"))
		return
	}

	nsRepo := h.ns
	if nsRepo == nil {
		nsRepo = db.NewNamespaceRepo(h.db)
	}
	id, err := nsRepo.Create(r.Context(), body.Name)
	if err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 201, map[string]any{
		"namespace_id": id,
		"name":         body.Name,
	})
}

func (h *Handlers) getNamespace(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	nsRepo := h.ns
	if nsRepo == nil {
		nsRepo = db.NewNamespaceRepo(h.db)
	}
	exists, err := nsRepo.Exists(r.Context(), name)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	if !exists {
		writeErr(w, 404, fmt.Errorf("namespace not found"))
		return
	}
	writeJSON(w, 200, map[string]any{"id": name, "name": name})
}

func (h *Handlers) renameNamespace(w http.ResponseWriter, r *http.Request) {
	oldName := chi.URLParam(r, "name")
	type req struct {
		NewName string `json:"new_name"`
	}
	var body req
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	if body.NewName == "" {
		writeErr(w, 400, fmt.Errorf("new_name required"))
		return
	}
	nsRepo := h.ns
	if nsRepo == nil {
		nsRepo = db.NewNamespaceRepo(h.db)
	}
	if err := nsRepo.Rename(r.Context(), oldName, body.NewName); err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, map[string]any{"old": oldName, "new": body.NewName})
}

func (h *Handlers) deleteNamespace(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	nsRepo := h.ns
	if nsRepo == nil {
		nsRepo = db.NewNamespaceRepo(h.db)
	}
	if err := nsRepo.Delete(r.Context(), name); err != nil {
		writeErr(w, 500, err)
		return
	}
	w.WriteHeader(204)
}

// ===== DAGs =====

func (h *Handlers) listDAGs(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	rows, err := h.db.Query(r.Context(), "SELECT id, name FROM dags WHERE namespace=$1 ORDER BY name;", ns)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()
	type item struct{ ID, Name string }
	var out []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.ID, &it.Name); err != nil {
			writeErr(w, 500, err)
			return
		}
		out = append(out, it)
	}
	writeJSON(w, 200, out)
}

func (h *Handlers) bulkUpsertDAGs(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	type in struct{ ID, Name string }
	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	for _, d := range list {
		if d.ID == "" {
			writeErr(w, 400, fmt.Errorf("id required for upsert"))
			return
		}
		if err := h.dag.CreateDAG(r.Context(), d.ID, ns, d.Name); err != nil {
			writeErr(w, 500, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"count": len(list)})
}

func (h *Handlers) createDAGs(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	type in struct{ ID, Name string }
	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	for _, d := range list {
		if d.ID == "" {
			writeErr(w, 400, fmt.Errorf("id required"))
			return
		}
		if err := h.dag.CreateDAG(r.Context(), d.ID, ns, d.Name); err != nil {
			writeErr(w, 409, fmt.Errorf("dag exists: %s", d.ID))
			return
		}
	}
	writeJSON(w, 201, map[string]any{"count": len(list)})
}

func (h *Handlers) getDAG(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	row := h.db.QueryRow(r.Context(), "SELECT id, namespace, name, created_at FROM dags WHERE id=$1;", id)
	var resp struct {
		ID, Namespace, Name string
		CreatedAt           time.Time
	}
	if err := row.Scan(&resp.ID, &resp.Namespace, &resp.Name, &resp.CreatedAt); err != nil {
		writeErr(w, 404, fmt.Errorf("not found"))
		return
	}
	writeJSON(w, 200, resp)
}

func (h *Handlers) updateDAG(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	type in struct {
		Name string `json:"name"`
	}
	var body in
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	if body.Name == "" {
		writeErr(w, 400, fmt.Errorf("name required"))
		return
	}
	if _, err := h.db.Exec(r.Context(), "UPDATE dags SET name=$1 WHERE id=$2;", body.Name, id); err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "name": body.Name})
}

func (h *Handlers) deleteDAGByID(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := h.dag.DeleteDAG(r.Context(), id); err != nil {
		writeErr(w, 500, err)
		return
	}
	w.WriteHeader(204)
}

// ===== Definitions =====

func (h *Handlers) listDefinitions(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	rows, err := h.db.Query(r.Context(), "SELECT def_id, name, version, kind FROM job_definitions WHERE namespace=$1 ORDER BY name, version;", ns)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()
	type def struct {
		ID, Name, Kind string
		Version        int
	}
	var out []def
	for rows.Next() {
		var d def
		if err := rows.Scan(&d.ID, &d.Name, &d.Version, &d.Kind); err != nil {
			writeErr(w, 500, err)
			return
		}
		out = append(out, d)
	}
	writeJSON(w, 200, out)
}

func (h *Handlers) bulkUpsertDefinitions(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	type in struct {
		Name            string          `json:"name"`
		Version         int             `json:"version"`
		Kind            string          `json:"kind"`
		PayloadTemplate json.RawMessage `json:"payload_template"`
	}
	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	for _, it := range list {
		if _, err := h.job.AddDefinition(r.Context(), ns, it.Name, it.Version, it.Kind, string(it.PayloadTemplate)); err != nil {
			writeErr(w, 500, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"count": len(list)})
}

func (h *Handlers) createDefinitions(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	type in struct {
		Name            string          `json:"name"`
		Version         int             `json:"version"`
		Kind            string          `json:"kind"`
		PayloadTemplate json.RawMessage `json:"payload_template"`
	}
	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	for _, it := range list {
		if _, err := h.job.AddDefinition(r.Context(), ns, it.Name, it.Version, it.Kind, string(it.PayloadTemplate)); err != nil {
			writeErr(w, 409, fmt.Errorf("definition exists: %s v%d", it.Name, it.Version))
			return
		}
	}
	writeJSON(w, 201, map[string]any{"count": len(list)})
}

func (h *Handlers) getDefinition(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	row := h.db.QueryRow(r.Context(), "SELECT def_id, namespace, name, version, kind, payload_template, created_at FROM job_definitions WHERE def_id=$1;", id)
	var resp struct {
		ID, Namespace, Name, Kind string
		Version                   int
		Payload                   json.RawMessage
		CreatedAt                 time.Time
	}
	if err := row.Scan(&resp.ID, &resp.Namespace, &resp.Name, &resp.Version, &resp.Kind, &resp.Payload, &resp.CreatedAt); err != nil {
		writeErr(w, 404, fmt.Errorf("not found"))
		return
	}
	writeJSON(w, 200, resp)
}

func (h *Handlers) updateDefinition(w http.ResponseWriter, r *http.Request) {
	writeErr(w, 400, fmt.Errorf("definitions are immutable; create a new version via POST"))
}

func (h *Handlers) newDefinitionVersion(w http.ResponseWriter, r *http.Request) {
	writeErr(w, 501, fmt.Errorf("not implemented"))
}

func (h *Handlers) listJobs(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
	rows, err := h.db.Query(r.Context(), "SELECT id, def_id, status FROM jobs WHERE dag_id=$1 ORDER BY id;", dagID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()
	type item struct {
		ID     int64
		DefID  string
		Status string
	}
	var out []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.ID, &it.DefID, &it.Status); err != nil {
			writeErr(w, 500, err)
			return
		}
		out = append(out, it)
	}
	writeJSON(w, 200, out)
}

func (h *Handlers) bulkUpsertJobs(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	_ = ns
	dagID := chi.URLParam(r, "dagId")

	type in struct {
		DefID    string     `json:"def_id"`
		Payload  string     `json:"payload,omitempty"`
		Priority int        `json:"priority,omitempty"`
		DueAt    *time.Time `json:"due_at,omitempty"`
	}
	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}

	for _, it := range list {
		// Supply defaults for optional fields
		priority := it.Priority
		if priority == 0 {
			priority = 0
		}
		dueAt := it.DueAt
		payload := it.Payload
		if payload == "" {
			payload = "{}"
		}

		if _, err := h.job.AddJob(
			r.Context(),
			dagID,
			it.DefID,
			priority,
			dueAt,
			payload,
		); err != nil {
			writeErr(w, 500, err)
			return
		}
	}

	writeJSON(w, 200, map[string]any{"count": len(list)})
}

func (h *Handlers) createJobs(w http.ResponseWriter, r *http.Request) {
	h.bulkUpsertJobs(w, r)
}

func (h *Handlers) getJobByPath(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	jobID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeErr(w, 400, fmt.Errorf("invalid job id"))
		return
	}
	job, err := h.job.Load(r.Context(), jobID)
	if err != nil {
		writeErr(w, 404, fmt.Errorf("not found"))
		return
	}
	writeJSON(w, 200, map[string]any{"id": job.ID, "kind": job.Kind, "payload": json.RawMessage(job.PayloadJSON)})
}

func (h *Handlers) updateJob(w http.ResponseWriter, r *http.Request) {
	writeErr(w, 501, fmt.Errorf("not implemented"))
}
func (h *Handlers) deleteJob(w http.ResponseWriter, r *http.Request) {
	writeErr(w, 501, fmt.Errorf("not implemented"))
}

// ===== Dependencies =====

func (h *Handlers) listDependencies(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
	rows, err := h.db.Query(r.Context(), "SELECT parent_job_id, child_job_id FROM job_dependencies WHERE dag_id=$1;", dagID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()
	type dep struct{ ParentID, ChildID int64 }
	var out []dep
	for rows.Next() {
		var d dep
		if err := rows.Scan(&d.ParentID, &d.ChildID); err != nil {
			writeErr(w, 500, err)
			return
		}
		out = append(out, d)
	}
	writeJSON(w, 200, out)
}

func (h *Handlers) bulkUpsertDependencies(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
	type in struct{ ParentID, ChildID int64 }
	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	for _, d := range list {
		if err := h.job.AddDependency(r.Context(), dagID, d.ParentID, d.ChildID); err != nil {
			writeErr(w, 400, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"count": len(list)})
}

func (h *Handlers) createDependencies(w http.ResponseWriter, r *http.Request) {
	h.bulkUpsertDependencies(w, r)
}

// Scan all DAGs and perform DFS-based cycle detection for each.
func (h *Handlers) checkGlobalCycles(w http.ResponseWriter, r *http.Request) {
	rows, err := h.db.Query(r.Context(), `SELECT id FROM dags;`)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()

	type dagResult struct {
		DagID  string     `json:"dag_id"`
		Cycles [][]string `json:"cycles,omitempty"`
	}

	src := &dag.DBEdges{DB: h.db}
	dfs := dag.DFSDetector{}

	var out []dagResult

	for rows.Next() {
		var dagID string
		if err := rows.Scan(&dagID); err != nil {
			writeErr(w, 500, err)
			return
		}

		cycles, err := dfs.DetectCycles(r.Context(), src, dagID)
		if err != nil {
			writeErr(w, 500, fmt.Errorf("failed cycle check for DAG %s: %w", dagID, err))
			return
		}

		if len(cycles) > 0 {
			out = append(out, dagResult{
				DagID:  dagID,
				Cycles: cycles,
			})
		}
	}

	writeJSON(w, 200, map[string]any{
		"count":   len(out),
		"results": out,
	})
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

// Utility functions
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

