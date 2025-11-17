package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/edkuperman/chronosched/internal/dag"
	"github.com/edkuperman/chronosched/internal/db"
	"github.com/edkuperman/chronosched/internal/scheduler"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handlers wires up all API endpoints.
type Handlers struct {
	db  *pgxpool.Pool
	dag *db.DAGRepo
	job *db.JobRepo
	dep *db.DependencyRepo
	sch *db.SchedulerRepo
	ns  *db.NamespaceRepo
	Scheduler *scheduler.Scheduler
}

func NewHandlers(pool *pgxpool.Pool) *Handlers {
	return &Handlers{
		db:  pool,
		dag: db.NewDAGRepo(pool),
		job: db.NewJobRepo(pool),
		dep: db.NewDependencyRepo(pool),
		sch: db.NewSchedulerRepo(pool),
		ns:  db.NewNamespaceRepo(pool),
	}
}

func (h *Handlers) SetScheduler(s *scheduler.Scheduler) {
	h.Scheduler = s
}

// Health check (wired inline in router)
func (h *Handlers) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
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
	ns, err := nsRepo.GetByName(r.Context(), name)
	if errors.Is(err, pgx.ErrNoRows) {
		writeErr(w, 404, fmt.Errorf("namespace not found"))
		return
	}
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, ns)
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
	w.WriteHeader(http.StatusNoContent)
}

// ===== DAGs =====

func (h *Handlers) getDAG(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	namespace_id := chi.URLParam(r, "namespace_id")

	row := h.db.QueryRow(r.Context(), `
		SELECT id, namespace, name, created_at, deleted
		FROM (
			SELECT id, namespace, name, version, created_at, deleted
			  FROM dags
			 WHERE id = $1
			UNION ALL
			SELECT id, namespace, name, created_at, deleted
			  FROM dags_history
			 WHERE id = $1 AND namespace = $2
		) s
		ORDER BY created_at DESC
		LIMIT 1;
	`, id, namespace_id)

	var resp struct {
		ID        string    `json:"id"`
		Namespace string    `json:"namespace"`
		Name      string    `json:"name"`
		Version   int       `json:"version"`
		CreatedAt time.Time `json:"created_at"`
		Deleted   bool      `json:"deleted"`
	}
	if err := row.Scan(&resp.ID, &resp.Namespace, &resp.Name, &resp.CreatedAt, &resp.Deleted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, 404, fmt.Errorf("not found"))
			return
		}
		writeErr(w, 500, err)
		return
	}
	writeJSON(w, 200, resp)
}

func (h *Handlers) listDAGs(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")

	rows, err := h.db.Query(r.Context(), `
		SELECT d.id, d.name, d.version
		FROM dags d
		WHERE d.namespace = $1
		  AND d.deleted = FALSE
		  AND (d.namespace, d.name, d.version) IN (
		    SELECT namespace, name, MAX(version)
		    FROM dags
		    WHERE namespace = $1 AND deleted = FALSE
		    GROUP BY namespace, name
		  )
		ORDER BY d.name;
	`, ns)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()

	type item struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Version int    `json:"version"`
	}

	var out []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.ID, &it.Name, &it.Version); err != nil {
			writeErr(w, 500, err)
			return
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, out)
}

func (h *Handlers) bulkUpsertDAGs(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")

	type in struct {
		Name string `json:"name"`
	}

	type result struct {
		Name    string  `json:"name"`
		Version int     `json:"version"`
		ID      *string `json:"id,omitempty"`
		Error   string  `json:"error,omitempty"`
	}

	var inputs []in
	if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
		writeErr(w, 400, err)
		return
	}
	if len(inputs) == 0 {
		writeErr(w, 400, fmt.Errorf("at least one DAG required"))
		return
	}

	results := make([]result, 0, len(inputs))

	for _, body := range inputs {
		name := strings.TrimSpace(body.Name)
		res := result{Name: name}

		if name == "" {
			res.Error = "name required"
			results = append(results, res)
			continue
		}

		// Versioned upsert, analogous to bulkUpsertDefinitions.
		id, version, err := h.dagInsertVersioned(r.Context(), ns, name)
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}

		res.ID = &id
		res.Version = version
		results = append(results, res)
	}

	writeJSON(w, 207, map[string]any{
		"results": results,
	})
}

// helper: we don't change DAGRepo's public surface, but expose a small wrapper
// from handlers to keep DAGRepo minimal. This is just an adapter that calls
// the versioned insert logic we added in DAGRepo.
func (h *Handlers) dagInsertVersioned(ctx context.Context, ns, name string) (string, int, error) {
	// We don't want to duplicate SQL here, so use the existing repo's logic.
	// CreateDAG gives us an id; we re-query version in a cheap, single-row SELECT.
	id, err := h.dag.CreateDAG(ctx, ns, name)
	if err != nil {
		return "", 0, err
	}

	var version int
	if err := h.db.QueryRow(ctx, `
		SELECT version
		FROM dags
		WHERE id = $1;
	`, id).Scan(&version); err != nil {
		return "", 0, err
	}

	return id, version, nil
}

func (h *Handlers) createDAGs(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")

	type in struct {
		Name string `json:"name"`
	}
	type out struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Version int    `json:"version"`
	}

	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	if len(list) == 0 {
		writeErr(w, 400, fmt.Errorf("at least one DAG required"))
		return
	}

	results := make([]out, 0, len(list))

	for _, d := range list {
		name := strings.TrimSpace(d.Name)
		if name == "" {
			writeErr(w, 400, fmt.Errorf("name is required"))
			return
		}

		id, version, err := h.dagInsertVersioned(r.Context(), ns, name)
		if err != nil {
			writeErr(w, 409, err)
			return
		}

		results = append(results, out{
			ID:      id,
			Name:    name,
			Version: version,
		})
	}

	writeJSON(w, 201, results)
}

func (h *Handlers) updateDAG(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	oldID := chi.URLParam(r, "id")

	var body struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	newName := strings.TrimSpace(body.Name)
	if newName == "" {
		writeErr(w, 400, fmt.Errorf("name required"))
		return
	}

	newID, newVersion, err := h.dag.UpdateDAG(r.Context(), oldID, ns, newName)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeErr(w, 409, err)
			return
		}
		if strings.Contains(err.Error(), "not found") {
			writeErr(w, 404, err)
			return
		}
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, map[string]any{
		"old_id":    oldID,
		"new_id":    newID,
		"name":      newName,
		"version":   newVersion,
		"namespace": ns,
	})
}


func (h *Handlers) deleteDAG(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	id := chi.URLParam(r, "id")

	// Optional: validate that the DAG belongs to the namespace
	var exists bool
	if err := h.db.QueryRow(
		r.Context(),
		`SELECT EXISTS (
             SELECT 1 FROM dags
              WHERE id = $1
                AND namespace = $2
                AND deleted = FALSE
         );`,
		id, ns,
	).Scan(&exists); err != nil {
		writeErr(w, 500, err)
		return
	}

	if !exists {
		writeErr(w, 404, fmt.Errorf("dag not found"))
		return
	}

	if err := h.dag.DeleteDAG(r.Context(), id); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 204, map[string]any{
		"id":        id,
		"namespace": ns,
		"deleted":   true,
	})
}

// ===== Definitions =====

func (h *Handlers) listDefinitions(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")

	rows, err := h.db.Query(r.Context(), `
		SELECT d.def_id, d.name, d.version, d.kind
		FROM job_definitions d
		WHERE d.namespace = $1
		  AND d.deleted = FALSE
		  AND (d.namespace, d.name, d.version) IN (
		    SELECT namespace, name, MAX(version)
		    FROM job_definitions
		    WHERE namespace = $1 AND deleted = FALSE
		    GROUP BY namespace, name
		  )
		ORDER BY d.name;
	`, ns)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()

	type def struct {
		ID      string `json:"def_id"`
		Name    string `json:"name"`
		Version int    `json:"version"`
		Kind    string `json:"kind"`
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
	if err := rows.Err(); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, out)
}

func (h *Handlers) bulkUpsertDefinitions(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")

	type in struct {
		Name            string          `json:"name"`
		Kind            string          `json:"kind"`
		PayloadTemplate json.RawMessage `json:"payload_template"`
		CronSpec        *string         `json:"cron_spec,omitempty"`
		DelayInterval   *string         `json:"delay_interval,omitempty"`
	}

	type result struct {
		Name    string  `json:"name"`
		Version int     `json:"version"`
		DefID   *string `json:"def_id,omitempty"`
		Error   string  `json:"error,omitempty"`
	}

	var inputs []in
	if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
		writeErr(w, 400, err)
		return
	}

	if len(inputs) == 0 {
		writeErr(w, 400, fmt.Errorf("at least one definition required"))
		return
	}

	results := make([]result, 0, len(inputs))

	for _, body := range inputs {
		res := result{Name: strings.TrimSpace(body.Name)}
		name := res.Name
		kind := strings.TrimSpace(body.Kind)

		if name == "" {
			res.Error = "name required"
			results = append(results, res)
			continue
		}
		if kind == "" {
			res.Error = "kind required"
			results = append(results, res)
			continue
		}
		if len(body.PayloadTemplate) == 0 {
			res.Error = "payload_template required"
			results = append(results, res)
			continue
		}

		defID, version, err := h.job.AddDefinition(
			r.Context(),
			ns,
			name,
			kind,
			string(body.PayloadTemplate),
			body.CronSpec,
			body.DelayInterval,
		)
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}

		res.DefID = &defID
		res.Version = version
		results = append(results, res)
	}

	// 207 Multi-Status: per-item success/error, similar to createDefinitions
	writeJSON(w, 207, map[string]any{
		"results": results,
	})
}

func (h *Handlers) createDefinitions(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")

	type in struct {
		Name            string          `json:"name"`
		Kind            string          `json:"kind"`
		PayloadTemplate json.RawMessage `json:"payload_template"`
		CronSpec        *string         `json:"cron_spec,omitempty"`
		DelayInterval   *string         `json:"delay_interval,omitempty"`
	}
	type result struct {
		Name    string  `json:"name"`
		Version int     `json:"version"`
		DefID   *string `json:"def_id,omitempty"`
		Error   string  `json:"error,omitempty"`
	}

	var inputs []in
	if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
		writeErr(w, 400, err)
		return
	}

	results := make([]result, 0, len(inputs))

	for _, body := range inputs {
		res := result{Name: strings.TrimSpace(body.Name)}
		name := res.Name
		kind := strings.TrimSpace(body.Kind)

		if name == "" {
			res.Error = "name required"
			results = append(results, res)
			continue
		}
		if kind == "" {
			res.Error = "kind required"
			results = append(results, res)
			continue
		}
		if len(body.PayloadTemplate) == 0 {
			res.Error = "payload_template required"
			results = append(results, res)
			continue
		}

		defID, version, err := h.job.AddDefinition(
			r.Context(),
			ns,
			name,
			kind,
			string(body.PayloadTemplate),
			body.CronSpec,
			body.DelayInterval,
		)
		if err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}

		res.DefID = &defID
		res.Version = version
		results = append(results, res)
	}

	if err := h.Scheduler.LoadAndRegister(r.Context()); err != nil {
		log.Printf("scheduler reload failed: %v", err)
	}

	writeJSON(w, 201, map[string]any{
		"results": results,
	})
}

func (h *Handlers) loadDefinitionByID(ctx context.Context, defID string) (*struct {
	DefID         string
	Namespace     string
	Name          string
	Version       int
	Kind          string
	PayloadJSON   json.RawMessage
	CronSpec      *string
	DelayInterval *string
	Deleted       bool
}, error) {
	row := h.db.QueryRow(ctx, `
		SELECT def_id, namespace, name, version, kind, payload_template,
		       cron_spec, delay_interval::text, deleted
		  FROM job_definitions
		 WHERE def_id = $1;
	`, defID)

	var cronSpec, delayInterval *string
	var payload json.RawMessage
	var deleted bool
	var ns, name, kind, id string
	var version int

	if err := row.Scan(&id, &ns, &name, &version, &kind, &payload, &cronSpec, &delayInterval, &deleted); err != nil {
		return nil, err
	}

	return &struct {
		DefID         string
		Namespace     string
		Name          string
		Version       int
		Kind          string
		PayloadJSON   json.RawMessage
		CronSpec      *string
		DelayInterval *string
		Deleted       bool
	}{
		DefID:         id,
		Namespace:     ns,
		Name:          name,
		Version:       version,
		Kind:          kind,
		PayloadJSON:   payload,
		CronSpec:      cronSpec,
		DelayInterval: delayInterval,
		Deleted:       deleted,
	}, nil
}

func (h *Handlers) getDefinition(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	defID := chi.URLParam(r, "id")

	row := h.db.QueryRow(r.Context(), `
		SELECT def_id, namespace, name, version, kind, payload_template,
		       cron_spec, delay_interval::text, created_at, deleted
		FROM (
			SELECT def_id, namespace, name, version, kind, payload_template,
			       cron_spec, delay_interval::text, created_at, deleted
			  FROM job_definitions
			 WHERE namespace = $1 AND def_id = $2
			UNION ALL
			SELECT def_id, namespace, name, version, kind, payload_template,
			       cron_spec, delay_interval::text, created_at, deleted
			  FROM job_definitions_history
			 WHERE namespace = $1 AND def_id = $2
		) s
		ORDER BY created_at DESC
		LIMIT 1;
	`, ns, defID)

	var out struct {
		DefID         string          `json:"def_id"`
		Namespace     string          `json:"namespace"`
		Name          string          `json:"name"`
		Version       int             `json:"version"`
		Kind          string          `json:"kind"`
		Payload       json.RawMessage `json:"payload_template"`
		CronSpec      *string         `json:"cron_spec,omitempty"`
		DelayInterval *string         `json:"delay_interval,omitempty"`
		CreatedAt     time.Time       `json:"created_at"`
		Deleted       bool            `json:"deleted"`
	}

	if err := row.Scan(
		&out.DefID,
		&out.Namespace,
		&out.Name,
		&out.Version,
		&out.Kind,
		&out.Payload,
		&out.CronSpec,
		&out.DelayInterval,
		&out.CreatedAt,
		&out.Deleted,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, 404, fmt.Errorf("definition not found"))
			return
		}
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, out)
}

// updateDefinition creates a new version of an existing definition in-place of updates.
// PUT /api/v1/definitions/{namespace_id}/{id}
func (h *Handlers) updateDefinition(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	defID := chi.URLParam(r, "id")

	existing, err := h.loadDefinitionByID(r.Context(), defID)
	if err != nil || existing.Namespace != ns {
		writeErr(w, 404, fmt.Errorf("definition not found"))
		return
	}

	var body struct {
		Name          string          `json:"name"`
		Kind          string          `json:"kind"`
		Payload       json.RawMessage `json:"payload_template"`
		CronSpec      *string         `json:"cron_spec,omitempty"`
		DelayInterval *string         `json:"delay_interval,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}

	name := body.Name
	if name == "" {
		name = existing.Name
	}

	// If the name is changed, enforce active-name uniqueness within the namespace.
	if name != existing.Name {
		var exists bool
		if err := h.db.QueryRow(
			r.Context(),
			`SELECT EXISTS(SELECT 1 FROM job_definitions WHERE namespace = $1 AND name = $2 AND deleted = FALSE AND def_id <> $3);`,
			ns, name, defID,
		).Scan(&exists); err != nil {
			writeErr(w, 500, err)
			return
		}
		if exists {
			writeErr(w, 409, fmt.Errorf("definition name already exists in namespace"))
			return
		}
	}

	kind := body.Kind
	if kind == "" {
		kind = existing.Kind
	}

	payload := body.Payload
	if len(payload) == 0 {
		payload = existing.PayloadJSON
	}

	cronSpec := body.CronSpec
	if cronSpec == nil {
		cronSpec = existing.CronSpec
	}

	delayInterval := body.DelayInterval
	if delayInterval == nil {
		delayInterval = existing.DelayInterval
	}

	// Use the repo to create the next immutable version (version computed server-side).
	defRepo := h.job
	if defRepo == nil {
		defRepo = db.NewJobRepo(h.db)
	}

	_, newVersion, err := defRepo.AddDefinition(
		r.Context(),
		ns,
		name,
		kind,
		string(payload),
		cronSpec,
		delayInterval,
	)
	if err != nil {
		writeErr(w, 500, err)
		return
	}

	// Soft-delete the old active version; prune logic will archive it.
	if _, err := h.db.Exec(r.Context(),
		`UPDATE job_definitions SET deleted = TRUE WHERE def_id = $1;`, defID); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, map[string]any{
		"def_id":    defID,
		"namespace": ns,
		"name":      name,
		"version":   newVersion,
	})
}

func (h *Handlers) deleteDefinition(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	defID := chi.URLParam(r, "id")

	existing, err := h.loadDefinitionByID(r.Context(), defID)
	if err != nil || existing.Namespace != ns {
		writeErr(w, 404, fmt.Errorf("definition not found"))
		return
	}

	if _, err := h.db.Exec(r.Context(),
		`UPDATE job_definitions SET deleted = TRUE WHERE def_id = $1;`, defID); err != nil {
		writeErr(w, 500, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ===== Jobs =====

func (h *Handlers) JobRepo() *db.JobRepo {
	return h.job
}

func (h *Handlers) listJobs(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dag_id")
	dagUUID, err := uuid.Parse(dagID)
	if err != nil {
		writeErr(w, 400, fmt.Errorf("invalid dag id"))
		return
	}
	rows, err := h.db.Query(r.Context(), "SELECT id, def_id, status FROM jobs WHERE dag_id=$1 ORDER BY id;", dagUUID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()

	type item struct {
		ID     int64  `json:"id"`
		DefID  string `json:"def_id"`
		Status string `json:"status"`
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
	dagID := chi.URLParam(r, "dag_id")

	type in struct {
		DefID    string     `json:"def_id"`
		Payload  string     `json:"payload,omitempty"`
		Priority int        `json:"priority,omitempty"`
		DueAt    *time.Time `json:"due_at,omitempty"`
	}

	type result struct {
		DefID string  `json:"def_id"`
		ID    *int64  `json:"id,omitempty"`
		Error string  `json:"error,omitempty"`
	}

	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	if len(list) == 0 {
		writeErr(w, 400, fmt.Errorf("at least one job required"))
		return
	}

	results := make([]result, 0, len(list))

	for _, it := range list {
		res := result{DefID: strings.TrimSpace(it.DefID)}

		if res.DefID == "" {
			res.Error = "def_id required"
			results = append(results, res)
			continue
		}

		payload := it.Payload
		if strings.TrimSpace(payload) == "" {
			payload = "{}"
		}

		id, err := h.job.AddJob(
			r.Context(),
			dagID,
			res.DefID,
			it.Priority,
			it.DueAt,
			payload,
		)
		if err != nil {
			res.Error = err.Error()
		} else {
			res.ID = &id
		}

		results = append(results, res)
	}

	writeJSON(w, 207, map[string]any{
		"results": results,
	})
}

func (h *Handlers) createJobs(w http.ResponseWriter, r *http.Request) {
	h.bulkUpsertJobs(w, r)
}

// getJob returns a single job by ID.
// GET /api/v1/dags/{dag_id}/jobs/{id}
func (h *Handlers) getJob(w http.ResponseWriter, r *http.Request) {
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

	writeJSON(w, 200, map[string]any{
		"id":      job.ID,
		"def_id":  job.DefID,
		"kind":    job.Kind,
		"payload": json.RawMessage(job.PayloadJSON),
		"due_at":  job.DueAt,
		"status":  nil, // optional — Load doesn't return status yet
	})
}

// updateJob updates mutable fields of a queued job.
// PUT /api/v1/dags/{dag_id}/jobs/{id}
func (h *Handlers) updateJob(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dag_id")
	jobIDStr := chi.URLParam(r, "id")
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		writeErr(w, 400, fmt.Errorf("invalid job id"))
		return
	}

	var body struct {
		Priority *int             `json:"priority,omitempty"`
		DueAt    *time.Time       `json:"due_at,omitempty"`
		Payload  *json.RawMessage `json:"payload_json,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}

	var status string
	if err := h.db.QueryRow(r.Context(),
		`SELECT status FROM jobs WHERE id = $1 AND dag_id = $2;`,
		jobID, dagID,
	).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, 404, fmt.Errorf("job not found"))
			return
		}
		writeErr(w, 500, err)
		return
	}
	if status != "queued" {
		writeErr(w, 400, fmt.Errorf("only queued jobs can be updated"))
		return
	}

	sets := []string{}
	args := []any{}
	idx := 1

	if body.Priority != nil {
		sets = append(sets, fmt.Sprintf("priority = $%d", idx))
		args = append(args, *body.Priority)
		idx++
	}
	if body.DueAt != nil {
		sets = append(sets, fmt.Sprintf("due_at = $%d", idx))
		args = append(args, *body.DueAt)
		idx++
	}
	if body.Payload != nil {
		sets = append(sets, fmt.Sprintf("payload_json = $%d", idx))
		args = append(args, *body.Payload)
		idx++
	}

	if len(sets) == 0 {
		writeJSON(w, 200, map[string]string{"status": "no changes"})
		return
	}

	query := "UPDATE jobs SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE id = $%d AND dag_id = $%d", idx, idx+1)
	args = append(args, jobID, dagID)

	if _, err := h.db.Exec(r.Context(), query, args...); err != nil {
		writeErr(w, 500, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// deleteJob cancels a job by marking it deleted.
// DELETE /api/v1/dags/{dag_id}/jobs/{id}
func (h *Handlers) deleteJob(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dag_id")
	jobIDStr := chi.URLParam(r, "id")
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		writeErr(w, 400, fmt.Errorf("invalid job id"))
		return
	}

	if _, err := h.db.Exec(r.Context(),
		`UPDATE jobs SET deleted = TRUE WHERE id = $1 AND dag_id = $2;`,
		jobID, dagID,
	); err != nil {
		writeErr(w, 500, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ===== Dependencies =====

func (h *Handlers) listDependencies(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dag_id")

	// If parent_id and child_id are provided, treat this as a single dependency lookup.
	q := r.URL.Query()
	parentStr := q.Get("parent_id")
	childStr := q.Get("child_id")

	type dep struct {
		ParentID int64  `json:"parent_job_id"`
		ChildID  int64  `json:"child_job_id"`
		Type     string `json:"dependency_type"`
	}

	// Single dependency (live + history)
	if parentStr != "" && childStr != "" {
		var parentID, childID int64
		if _, err := fmt.Sscan(parentStr, &parentID); err != nil {
			writeErr(w, 400, fmt.Errorf("invalid parent_id"))
			return
		}
		if _, err := fmt.Sscan(childStr, &childID); err != nil {
			writeErr(w, 400, fmt.Errorf("invalid child_id"))
			return
		}

		row := h.db.QueryRow(r.Context(), `
			SELECT dag_id, parent_job_id, child_job_id, dependency_type
			FROM (
				SELECT dag_id, parent_job_id, child_job_id, dependency_type, NULL::timestamptz AS archived_at
				  FROM job_dependencies
				 WHERE dag_id = $1 AND parent_job_id = $2 AND child_job_id = $3
				UNION ALL
				SELECT dag_id, parent_job_id, child_job_id, dependency_type, archived_at
				  FROM job_dependencies_history
				 WHERE dag_id = $1 AND parent_job_id = $2 AND child_job_id = $3
			) s
			ORDER BY archived_at DESC NULLS FIRST
			LIMIT 1;
		`, dagID, parentID, childID)

		var d dep
		var dummyDag string
		if err := row.Scan(&dummyDag, &d.ParentID, &d.ChildID, &d.Type); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				writeErr(w, 404, fmt.Errorf("dependency not found"))
				return
			}
			writeErr(w, 500, err)
			return
		}

		// Reuse the bulk response shape but with a single result.
		writeJSON(w, 200, map[string]any{
			"count":   1,
			"results": []dep{d},
		})
		return
	}

	// Bulk list: only active dependencies for this DAG.
	rows, err := h.db.Query(r.Context(), `
		SELECT parent_job_id, child_job_id, dependency_type
		FROM job_dependencies
		WHERE dag_id = $1;
	`, dagID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()

	var out []dep
	for rows.Next() {
		var d dep
		if err := rows.Scan(&d.ParentID, &d.ChildID, &d.Type); err != nil {
			writeErr(w, 500, err)
			return
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, map[string]any{
		"count":   len(out),
		"results": out,
	})
}

func (h *Handlers) bulkUpsertDependencies(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dag_id")

	type in struct {
		ParentID       string `json:"parent_id"`
		ChildID        string `json:"child_id"`
		DependencyType string `json:"dependency_type"` // "data" | "order-only"
	}

	type result struct {
		ParentID string  `json:"parent_id"`
		ChildID  string  `json:"child_id"`
		ID       *int64  `json:"id,omitempty"`
		Error    string  `json:"error,omitempty"`
	}

	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	if len(list) == 0 {
		writeErr(w, 400, fmt.Errorf("at least one dependency is required"))
		return
	}

	results := make([]result, 0, len(list))

	for _, it := range list {
		res := result{
			ParentID: strings.TrimSpace(it.ParentID),
			ChildID:  strings.TrimSpace(it.ChildID),
		}

		if res.ParentID == "" || res.ChildID == "" {
			res.Error = "parent_id and child_id are required"
			results = append(results, res)
			continue
		}

		if it.DependencyType != "data" && it.DependencyType != "order-only" {
			res.Error = "dependency_type must be 'data' or 'order-only'"
			results = append(results, res)
			continue
		}

		id, err := h.dep.UpsertDependency(
			r.Context(),
			dagID,
			res.ParentID,
			res.ChildID,
			it.DependencyType,
		)

		if err != nil {
			res.Error = err.Error()
		} else {
			res.ID = &id
		}

		results = append(results, res)
	}

	writeJSON(w, 207, map[string]any{
		"results": results,
	})
}

func (h *Handlers) createDependencies(w http.ResponseWriter, r *http.Request) {
	h.bulkUpsertDependencies(w, r)
}

// updateDependency updates dependency_type for a given (parent_id, child_id) in a DAG.
// PATCH /api/v1/dags/{dag_id}/dependencies
func (h *Handlers) updateDependency(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dag_id")

	type in struct {
		ParentID       int64  `json:"parent_id"`
		ChildID        int64  `json:"child_id"`
		DependencyType string `json:"dependency_type"`
	}

	var body in
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}

	if body.ParentID == 0 || body.ChildID == 0 {
		writeErr(w, 400, fmt.Errorf("parent_id and child_id are required"))
		return
	}

	if body.DependencyType != "data" && body.DependencyType != "order-only" {
		writeErr(w, 400, fmt.Errorf("dependency_type must be 'data' or 'order-only'"))
		return
	}

	cmd, err := h.db.Exec(r.Context(), `
		UPDATE job_dependencies
		   SET dependency_type = $4
		 WHERE dag_id = $1
		   AND parent_job_id = $2
		   AND child_job_id = $3;
	`, dagID, body.ParentID, body.ChildID, body.DependencyType)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	if cmd.RowsAffected() == 0 {
		writeErr(w, 404, fmt.Errorf("dependency not found"))
		return
	}

	writeJSON(w, 200, map[string]any{
		"parent_job_id":   body.ParentID,
		"child_job_id":    body.ChildID,
		"dependency_type": body.DependencyType,
	})
}

// deleteDependency deletes an individual dependency and archives it to history.
// DELETE /api/v1/dags/{dag_id}/dependencies?parent_id=...&child_id=...
func (h *Handlers) deleteDependency(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dag_id")
	q := r.URL.Query()
	parentStr := q.Get("parent_id")
	childStr := q.Get("child_id")

	if parentStr == "" || childStr == "" {
		writeErr(w, 400, fmt.Errorf("parent_id and child_id are required"))
		return
	}

	var parentID, childID int64
	if _, err := fmt.Sscan(parentStr, &parentID); err != nil {
		writeErr(w, 400, fmt.Errorf("invalid parent_id"))
		return
	}
	if _, err := fmt.Sscan(childStr, &childID); err != nil {
		writeErr(w, 400, fmt.Errorf("invalid child_id"))
		return
	}

	// Archive to history first.
	cmd, err := h.db.Exec(r.Context(), `
		INSERT INTO job_dependencies_history(dag_id, parent_job_id, child_job_id, dependency_type, archived_at)
		SELECT dag_id, parent_job_id, child_job_id, dependency_type, now()
		  FROM job_dependencies
		 WHERE dag_id = $1
		   AND parent_job_id = $2
		   AND child_job_id = $3;
	`, dagID, parentID, childID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	if cmd.RowsAffected() == 0 {
		writeErr(w, 404, fmt.Errorf("dependency not found"))
		return
	}

	// Delete from active table.
	if _, err := h.db.Exec(r.Context(), `
		DELETE FROM job_dependencies
		 WHERE dag_id = $1
		   AND parent_job_id = $2
		   AND child_job_id = $3;
	`, dagID, parentID, childID); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, map[string]any{
		"dag_id":        dagID,
		"parent_job_id": parentID,
		"child_job_id":  childID,
		"archived":      true,
	})
}

// ======== Prune & Diagnostics ========

// prune archives soft-deleted or completed entities into their respective
// history tables and removes them from the active working set.
// It is safe and idempotent.
func (h *Handlers) prune(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tx, err := h.db.Begin(ctx)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer tx.Rollback(ctx)

	result := struct {
		JobDefinitionsArchived int `json:"job_definitions_archived"`
		DAGsArchived           int `json:"dags_archived"`
		JobsArchived           int `json:"jobs_archived"`
		DependenciesArchived   int `json:"dependencies_archived"`
	}{}

	//---------------------------------------
	// 1. PRUNE JOB-LEVEL DEPENDENCIES
	//    (rows explicitly marked deleted = TRUE)
	//---------------------------------------
	cmd, err := tx.Exec(ctx, `
		INSERT INTO job_dependencies_history (
			dag_id,
			parent_job_id,
			child_job_id,
			dependency_type,
			created_at,
			deleted
		)
		SELECT
			dag_id,
			parent_job_id,
			child_job_id,
			dependency_type,
			created_at,
			deleted
		FROM job_dependencies
		WHERE deleted = TRUE
		ON CONFLICT DO NOTHING;
	`)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	result.DependenciesArchived = int(cmd.RowsAffected())

	_, err = tx.Exec(ctx, `
		DELETE FROM job_dependencies
		WHERE deleted = TRUE;
	`)
	if err != nil {
		writeErr(w, 500, err)
		return
	}

	//---------------------------------------
	// 2. PRUNE JOBS
	//    A job is ready to archive if:
	//    - deleted = TRUE OR
	//    - status IN ('succeeded','failed','cancelled')
	//---------------------------------------
	cmd, err = tx.Exec(ctx, `
		INSERT INTO jobs_history (
			id,
			dag_id,
			def_id,
			version,
			deleted,
			status,
			priority,
			due_at,
			payload_json,
			binary_data,
			lease_owner,
			lease_until,
			enqueued_at,
			started_at,
			finished_at,
			last_error,
			last_scheduled_at
		)
		SELECT
			id,
			dag_id,
			def_id,
			version,
			deleted,
			status,
			priority,
			due_at,
			payload_json,
			binary_data,
			lease_owner,
			lease_until,
			enqueued_at,
			started_at,
			finished_at,
			last_error,
			last_scheduled_at
		FROM jobs
		WHERE deleted = TRUE
		   OR status IN ('succeeded','failed','cancelled')
		ON CONFLICT DO NOTHING;
	`)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	result.JobsArchived = int(cmd.RowsAffected())

	_, err = tx.Exec(ctx, `
		DELETE FROM jobs
		WHERE deleted = TRUE
		   OR status IN ('succeeded','failed','cancelled');
	`)
	if err != nil {
		writeErr(w, 500, err)
		return
	}

	//---------------------------------------
	// 3. PRUNE DAGs
	//    (rows marked deleted = TRUE)
	//    NOTE: jobs were already archived/deleted above,
	//    so ON DELETE CASCADE from jobs(dag_id) won't
	//    silently drop un-archived jobs.
	//---------------------------------------
	cmd, err = tx.Exec(ctx, `
		INSERT INTO dags_history (
			id,
			namespace,
			name,
			version,
			created_at,
			deleted
		)
		SELECT
			id,
			namespace,
			name,
			version,
			created_at,
			deleted
		FROM dags
		WHERE deleted = TRUE
		ON CONFLICT DO NOTHING;
	`)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	result.DAGsArchived = int(cmd.RowsAffected())

	_, err = tx.Exec(ctx, `
		DELETE FROM dags
		WHERE deleted = TRUE;
	`)
	if err != nil {
		writeErr(w, 500, err)
		return
	}

	//---------------------------------------
	// 4. PRUNE JOB DEFINITIONS
	//    (rows marked deleted = TRUE)
	//---------------------------------------
	cmd, err = tx.Exec(ctx, `
		INSERT INTO job_definitions_history (
			def_id,
			namespace,
			name,
			version,
			kind,
			payload_template,
			cron_spec,
			delay_interval,
			created_at,
			deleted
		)
		SELECT
			def_id,
			namespace,
			name,
			version,
			kind,
			payload_template,
			cron_spec,
			delay_interval,
			created_at,
			deleted
		FROM job_definitions
		WHERE deleted = TRUE
		ON CONFLICT DO NOTHING;
	`)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	result.JobDefinitionsArchived = int(cmd.RowsAffected())

	_, err = tx.Exec(ctx, `
		DELETE FROM job_definitions
		WHERE deleted = TRUE;
	`)
	if err != nil {
		writeErr(w, 500, err)
		return
	}

	//---------------------------------------
	// COMMIT
	//---------------------------------------
	if err := tx.Commit(ctx); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, result)
}


// POST /api/v1/dags/{dag_id}/jobs/{jobId}/complete
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

// POST /api/v1/dags/{dag_id}/jobs/{jobId}/fail
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

func (h *Handlers) listJobsInNamespace(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	nsID := chi.URLParam(r, "namespace_id")

	nsRepo := h.ns
	if nsRepo == nil {
		nsRepo = db.NewNamespaceRepo(h.db)
	}

	exists, err := nsRepo.ExistsByID(ctx, nsID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	if !exists {
		writeErr(w, 404, fmt.Errorf("namespace not found"))
		return
	}

	jobs, err := h.job.ListByNamespace(ctx, nsID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, jobs)
}

// listCronSchedules returns all cron-enabled job_definitions and,
// if present, the cron DAG that the scheduler will use for them.
//
// This matches the scheduler.ensureCronDAG logic:
//   - cron DAG name = "__cron__" + definition name
func (h *Handlers) listCronSchedules(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	const q = `
SELECT
  jd.def_id::text,
  jd.namespace::text,
  jd.name,
  jd.cron_spec,
  d.id::text    AS dag_id,
  d.name        AS dag_name
FROM job_definitions jd
LEFT JOIN dags d
  ON d.namespace = jd.namespace
 AND d.name = ('__cron__' || jd.name)
 AND d.deleted = FALSE
WHERE jd.deleted = FALSE
  AND jd.cron_spec IS NOT NULL
  AND trim(jd.cron_spec) <> ''
ORDER BY jd.namespace, jd.name;
`

	rows, err := h.db.Query(ctx, q)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()

	type cronSchedule struct {
		DefID       string  `json:"def_id"`
		NamespaceID string  `json:"namespace_id"`
		Name        string  `json:"name"`
		CronSpec    string  `json:"cron_spec"`
		DAGID       *string `json:"dag_id,omitempty"`
		DAGName     *string `json:"dag_name,omitempty"`
	}

	var out []cronSchedule

	for rows.Next() {
		var s cronSchedule
		var dagID *string
		var dagName *string

		if err := rows.Scan(
			&s.DefID,
			&s.NamespaceID,
			&s.Name,
			&s.CronSpec,
			&dagID,
			&dagName,
		); err != nil {
			writeErr(w, 500, err)
			return
		}

		s.DAGID = dagID
		s.DAGName = dagName

		out = append(out, s)
	}

	if err := rows.Err(); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, out)
}

// listRunsInDAG returns all job rows for a given DAG.
// Transitional model: this is a direct view over the jobs table.
func (h *Handlers) listRunsInDAG(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dagID := chi.URLParam(r, "dag_id")

	const q = `
		SELECT
			id,
			dag_id::text,
			def_id::text,
			status::text,
			priority,
			due_at,
			payload_json::text,
			enqueued_at,
			started_at,
			finished_at,
			last_error
		FROM jobs
		WHERE dag_id = $1
		  AND deleted = FALSE
		ORDER BY id DESC;
	`

	rows, err := h.db.Query(ctx, q, dagID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()

	type jobRun struct {
		ID         int64      `json:"id"`
		DAGID      string     `json:"dag_id"`
		DefID      string     `json:"def_id"`
		Status     string     `json:"status"`
		Priority   int        `json:"priority"`
		DueAt      *time.Time `json:"due_at,omitempty"`
		Payload    string     `json:"payload_json"`
		EnqueuedAt time.Time  `json:"enqueued_at"`
		StartedAt  *time.Time `json:"started_at,omitempty"`
		FinishedAt *time.Time `json:"finished_at,omitempty"`
		LastError  *string    `json:"last_error,omitempty"`
	}

	var out []jobRun

	for rows.Next() {
		var jr jobRun
		if err := rows.Scan(
			&jr.ID,
			&jr.DAGID,
			&jr.DefID,
			&jr.Status,
			&jr.Priority,
			&jr.DueAt,
			&jr.Payload,
			&jr.EnqueuedAt,
			&jr.StartedAt,
			&jr.FinishedAt,
			&jr.LastError,
		); err != nil {
			writeErr(w, 500, err)
			return
		}
		out = append(out, jr)
	}

	if err := rows.Err(); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, out)
}

// listRunsForJob returns all runs for the same (dag_id, def_id) as the given job_id.
func (h *Handlers) listRunsForJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dagID := chi.URLParam(r, "dag_id")
	jobIDStr := chi.URLParam(r, "job_id")

	jobID, err := strconv.Atoi(jobIDStr)
	if err != nil {
		writeErr(w, 400, fmt.Errorf("invalid job_id"))
		return
	}

	// First, figure out which def_id this job uses.
	const qDef = `
		SELECT def_id::text
		FROM jobs
		WHERE id = $1
		  AND dag_id = $2
		  AND deleted = FALSE;
	`

	var defID string
	if err := h.db.QueryRow(ctx, qDef, jobID, dagID).Scan(&defID); err != nil {
		writeErr(w, 404, fmt.Errorf("job not found"))
		return
	}

	// Now list all jobs for this (dag_id, def_id).
	const q = `
		SELECT
			id,
			dag_id::text,
			def_id::text,
			status::text,
			priority,
			due_at,
			payload_json::text,
			enqueued_at,
			started_at,
			finished_at,
			last_error
		FROM jobs
		WHERE dag_id = $1
		  AND def_id = $2::uuid
		  AND deleted = FALSE
		ORDER BY id DESC;
	`

	rows, err := h.db.Query(ctx, q, dagID, defID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer rows.Close()

	type jobRun struct {
		ID         int64      `json:"id"`
		DAGID      string     `json:"dag_id"`
		DefID      string     `json:"def_id"`
		Status     string     `json:"status"`
		Priority   int        `json:"priority"`
		DueAt      *time.Time `json:"due_at,omitempty"`
		Payload    string     `json:"payload_json"`
		EnqueuedAt time.Time  `json:"enqueued_at"`
		StartedAt  *time.Time `json:"started_at,omitempty"`
		FinishedAt *time.Time `json:"finished_at,omitempty"`
		LastError  *string    `json:"last_error,omitempty"`
	}

	var out []jobRun

	for rows.Next() {
		var jr jobRun
		if err := rows.Scan(
			&jr.ID,
			&jr.DAGID,
			&jr.DefID,
			&jr.Status,
			&jr.Priority,
			&jr.DueAt,
			&jr.Payload,
			&jr.EnqueuedAt,
			&jr.StartedAt,
			&jr.FinishedAt,
			&jr.LastError,
		); err != nil {
			writeErr(w, 500, err)
			return
		}
		out = append(out, jr)
	}

	if err := rows.Err(); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, out)
}

// retryJob creates a new queued job for the same dag_id/def_id as the given job_id.
// Transitional model: this directly inserts into jobs.
func (h *Handlers) retryJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dagID := chi.URLParam(r, "dag_id")
	jobIDStr := chi.URLParam(r, "job_id")

	jobID, err := strconv.Atoi(jobIDStr)
	if err != nil {
		writeErr(w, 400, fmt.Errorf("invalid job_id"))
		return
	}

	const selectJob = `
		SELECT def_id::text, priority, payload_json::text
		FROM jobs
		WHERE id = $1
		  AND dag_id = $2
		  AND deleted = FALSE;
	`

	var defID string
	var priority int
	var payload string

	if err := h.db.QueryRow(ctx, selectJob, jobID, dagID).Scan(&defID, &priority, &payload); err != nil {
		writeErr(w, 404, fmt.Errorf("job not found"))
		return
	}

	if payload == "" || payload == "null" {
		payload = "{}"
	}

	const insertJob = `
		INSERT INTO jobs (
			dag_id,
			def_id,
			status,
			priority,
			due_at,
			payload_json
		)
		VALUES (
			$1,
			$2::uuid,
			'queued',
			$3,
			now(),
			$4::jsonb
		)
		RETURNING id;
	`

	var newID int64
	if err := h.db.QueryRow(ctx, insertJob, dagID, defID, priority, payload).Scan(&newID); err != nil {
		writeErr(w, 500, err)
		return
	}

	w.WriteHeader(201)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
