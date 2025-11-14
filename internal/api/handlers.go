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
	"github.com/google/uuid"
	"context"
	"errors"
	"strings"
	"github.com/jackc/pgx/v5"
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
            // CREATE CASE
            id, err := h.dag.CreateDAG(r.Context(), ns, d.Name)
            if err != nil {
                writeErr(w, 500, err)
                return
            }
            // return the id to the client
            d.ID = id
            continue
        }

        // UPDATE CASE
        if err := h.dag.UpdateDAG(r.Context(), d.ID, ns, d.Name); err != nil {
            writeErr(w, 500, err)
            return
        }
    }

    writeJSON(w, 200, map[string]any{
        "count": len(list),
        "items": list, 
    })
}

func (h *Handlers) createDAGs(w http.ResponseWriter, r *http.Request) {
    ns := chi.URLParam(r, "namespace_id")

    type in struct {
        Name string `json:"name"`
    }

    type out struct {
        ID string `json:"id"`
    }

    var list []in
    if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
        writeErr(w, 400, err)
        return
    }

    results := make([]out, 0, len(list))

    for _, d := range list {
        if d.Name == "" {
            writeErr(w, 400, fmt.Errorf("name is required"))
            return
        }

        id, err := h.dag.CreateDAG(r.Context(), ns, d.Name)
        if err != nil {
            writeErr(w, 409, err)
            return
        }

        results = append(results, out{ID: id})
    }

    writeJSON(w, 201, results)
}

func (h *Handlers) getDAG(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	row := h.db.QueryRow(r.Context(), `
		SELECT id, namespace, name, created_at, deleted
		FROM (
			SELECT id, namespace, name, created_at, deleted
			  FROM dags
			 WHERE id = $1
			UNION ALL
			SELECT id, namespace, name, created_at, deleted
			  FROM dags_history
			 WHERE id = $1
		) s
		ORDER BY created_at DESC
		LIMIT 1;
	`, id)

	var resp struct {
		ID        string    `json:"id"`
		Namespace string    `json:"namespace"`
		Name      string    `json:"name"`
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


func (h *Handlers) updateDAG(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
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

	if err := h.dag.UpdateDAG(r.Context(), id, ns, body.Name); err != nil {
		writeErr(w, 500, err)
		return
	}
}

// deleteDAGByID soft-deletes a DAG by marking it deleted = TRUE.
// This does NOT remove associated definitions; prune logic handles old versions.
func (h *Handlers) deleteDAGByID(w http.ResponseWriter, r *http.Request) {
    ns := chi.URLParam(r, "namespace_id")
    id := chi.URLParam(r, "id")

    res, err := h.db.Exec(
        r.Context(),
        `UPDATE dags SET deleted = TRUE WHERE id = $1 AND namespace = $2;`,
        id, ns,
    )
    if err != nil {
        writeErr(w, 500, err)
        return
    }

    if res.RowsAffected() == 0 {
        writeErr(w, 404, fmt.Errorf("dag not found"))
        return
    }

    w.WriteHeader(http.StatusNoContent)
}

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
		CronSpec       *string         `json:"cron_spec,omitempty"`
		DelayInterval  *string         `json:"delay_interval,omitempty"`
	}

	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	if len(list) == 0 {
		writeErr(w, 400, fmt.Errorf("at least one definition required"))
		return
	}

	count := 0
	for _, it := range list {
		name := strings.TrimSpace(it.Name)
		if name == "" {
			writeErr(w, 400, fmt.Errorf("name required for all definitions"))
			return
		}
		kind := strings.TrimSpace(it.Kind)
		if kind == "" {
			writeErr(w, 400, fmt.Errorf("kind required for definition %q", name))
			return
		}
		if len(it.PayloadTemplate) == 0 {
			writeErr(w, 400, fmt.Errorf("payload_template required for definition %q", name))
			return
		}

		if _, err := h.job.AddDefinition(
			r.Context(),
			ns,
			name,
			0, // auto-assign next version in repo
			kind,
			string(it.PayloadTemplate),
			it.CronSpec,
			it.DelayInterval,
		); err != nil {
			writeErr(w, 500, err)
			return
		}
		count++
	}

	writeJSON(w, 200, map[string]any{"count": count})
}


func (h *Handlers) createDefinitions(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")

	type in struct {
		Name            string          `json:"name"`
		Kind            string          `json:"kind"`
		PayloadTemplate json.RawMessage `json:"payload_template"`
		CronSpec       *string         `json:"cron_spec,omitempty"`
		DelayInterval  *string         `json:"delay_interval,omitempty"`
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

		defID, err := h.job.AddDefinition(
			r.Context(),
			ns,
			name,
			0, // auto-assign next version in repo
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

		var version int
		if err := h.db.QueryRow(
			r.Context(),
			`SELECT version FROM job_definitions WHERE def_id = $1;`,
			defID,
		).Scan(&version); err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}

		res.DefID = &defID
		res.Version = version
		results = append(results, res)
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

	defIDNew, err := defRepo.AddDefinition(
		r.Context(),
		ns,
		name,
		0, // auto-assign next version in repo (based on live + history)
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

	// Load the assigned version of the new definition.
	var newVersion int
	if err := h.db.QueryRow(
		r.Context(),
		`SELECT version FROM job_definitions WHERE def_id = $1;`,
		defIDNew,
	).Scan(&newVersion); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, map[string]any{
		"old_def_id":  defID,
		"new_def_id":  defIDNew,
		"namespace":   ns,
		"name":        name,
		"new_version": newVersion,
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

func (h *Handlers) JobRepo() *db.JobRepo {
    return h.job
}

func (h *Handlers) listJobs(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
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
    dagID := chi.URLParam(r, "dagId")

    type in struct {
        DefID    string     `json:"def_id"`
        Payload  string     `json:"payload,omitempty"`
        Priority int        `json:"priority,omitempty"`
        DueAt    *time.Time `json:"due_at,omitempty"`
    }

    type out struct {
        ID    int64  `json:"id"`
        DefID string `json:"def_id"`
    }

    var list []in
    if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
        writeErr(w, 400, err)
        return
    }

    results := make([]out, 0, len(list))

    for _, it := range list {
        payload := it.Payload
        if payload == "" {
            payload = "{}"
        }

        id, err := h.job.AddJob(
            r.Context(),
            dagID,
            it.DefID,
            it.Priority,
            it.DueAt,
            payload,
        )
        if err != nil {
            writeErr(w, 500, err)
            return
        }

        results = append(results, out{
            ID:    id,
            DefID: it.DefID,
        })
    }

    writeJSON(w, 200, results)
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

    writeJSON(w, 200, map[string]any{
        "id":        job.ID,
        "def_id":    job.DefID,
        "kind":      job.Kind,
        "payload":   json.RawMessage(job.PayloadJSON),
        "due_at":    job.DueAt,
        "status":    nil, // optional — Load doesn't return status yet
    })
}

// updateJob updates mutable fields of a queued job.
// PUT /api/v1/dags/{namespace_id}/{dagId}/jobs/{id}
func (h *Handlers) updateJob(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
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

// deleteJob cancels a job by marking its status as 'cancelled'.
// DELETE /api/v1/dags/{namespace_id}/{dagId}/jobs/{id}
func (h *Handlers) deleteJob(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
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

func (h *Handlers) deleteJobGlobal(w http.ResponseWriter, r *http.Request) {
	ns := chi.URLParam(r, "namespace_id")
	jobIDStr := chi.URLParam(r, "jobId")
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		writeErr(w, 400, fmt.Errorf("invalid job id"))
		return
	}

	// Soft delete job by id within the given namespace via its DAG.
	cmd, err := h.db.Exec(r.Context(), `
		UPDATE jobs j
		   SET deleted = TRUE
		  FROM dags d
		 WHERE j.dag_id = d.id
		   AND j.id = $1
		   AND d.namespace = $2;
	`, jobID, ns)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	if cmd.RowsAffected() == 0 {
		writeErr(w, 404, fmt.Errorf("job not found"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ===== Dependencies =====

func (h *Handlers) listDependencies(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")

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
	dagID := chi.URLParam(r, "dagId")
	type in struct {
		ParentID int64  `json:"parent_job_id"`
		ChildID  int64  `json:"child_job_id"`
		Type     string `json:"dependency_type,omitempty"`
	}
	var list []in
	if err := json.NewDecoder(r.Body).Decode(&list); err != nil {
		writeErr(w, 400, err)
		return
	}
	for _, d := range list {
		if err := h.job.AddDependency(r.Context(), dagID, d.ParentID, d.ChildID, d.Type); err != nil {
			writeErr(w, 400, err)
			return
		}
	}
	writeJSON(w, 200, map[string]any{"count": len(list)})
}

func (h *Handlers) createDependencies(w http.ResponseWriter, r *http.Request) {
	h.bulkUpsertDependencies(w, r)
}

func (h *Handlers) updateDependency(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")

	var body struct {
		ParentID int64  `json:"parent_job_id"`
		ChildID  int64  `json:"child_job_id"`
		Type     string `json:"dependency_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err)
		return
	}
	if body.ParentID == 0 || body.ChildID == 0 {
		writeErr(w, 400, fmt.Errorf("parent_job_id and child_job_id are required"))
		return
	}
	if body.Type == "" {
		writeErr(w, 400, fmt.Errorf("dependency_type is required"))
		return
	}

	cmd, err := h.db.Exec(r.Context(), `
		UPDATE job_dependencies
		SET dependency_type = $4
		WHERE dag_id = $1 AND parent_job_id = $2 AND child_job_id = $3;
	`, dagID, body.ParentID, body.ChildID, body.Type)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	if cmd.RowsAffected() == 0 {
		writeErr(w, 404, fmt.Errorf("dependency not found"))
		return
	}

	writeJSON(w, 200, map[string]any{
		"dag_id":          dagID,
		"parent_job_id":   body.ParentID,
		"child_job_id":    body.ChildID,
		"dependency_type": body.Type,
	})
}

func (h *Handlers) deleteDependency(w http.ResponseWriter, r *http.Request) {
	dagID := chi.URLParam(r, "dagId")
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

	// Archive the dependency if it exists, then delete it from the live table.
	tx, err := h.db.Begin(r.Context())
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer tx.Rollback(r.Context())

	cmdIns, err := tx.Exec(r.Context(), `
		INSERT INTO job_dependencies_history(dag_id, parent_job_id, child_job_id, dependency_type, archived_at)
		SELECT dag_id, parent_job_id, child_job_id, dependency_type, now()
		FROM job_dependencies
		WHERE dag_id = $1 AND parent_job_id = $2 AND child_job_id = $3;
	`, dagID, parentID, childID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}

	cmdDel, err := tx.Exec(r.Context(), `
		DELETE FROM job_dependencies
		WHERE dag_id = $1 AND parent_job_id = $2 AND child_job_id = $3;
	`, dagID, parentID, childID)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	if cmdDel.RowsAffected() == 0 {
		writeErr(w, 404, fmt.Errorf("dependency not found"))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, map[string]any{
		"dag_id":        dagID,
		"parent_job_id": parentID,
		"child_job_id":  childID,
		"archived":      cmdIns.RowsAffected() > 0,
	})
}


func (h *Handlers) prune(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tx, err := h.db.Begin(ctx)
	if err != nil {
		writeErr(w, 500, err)
		return
	}
	defer tx.Rollback(ctx)

	var defsArchived, dagsArchived, jobsArchived, depsArchived int64

	// Prune job definitions: deleted and no active jobs reference them.
	if cmd, err := tx.Exec(ctx, `
		INSERT INTO job_definitions_history(
			def_id, namespace, name, version, kind,
			payload_template, cron_spec, delay_interval,
			deleted, created_at, archived_at
		)
		SELECT def_id, namespace, name, version, kind,
		       payload_template, cron_spec, delay_interval,
		       deleted, created_at, now()
		  FROM job_definitions d
		 WHERE deleted = TRUE
		   AND NOT EXISTS (
		         SELECT 1 FROM jobs j WHERE j.def_id = d.def_id
		       );
	`); err != nil {
		writeErr(w, 500, err)
		return
	} else {
		defsArchived += cmd.RowsAffected()
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM job_definitions d
		 WHERE deleted = TRUE
		   AND NOT EXISTS (
		         SELECT 1 FROM jobs j WHERE j.def_id = d.def_id
		       );
	`); err != nil {
		writeErr(w, 500, err)
		return
	}

	// Prune dags: deleted and no jobs remain in that DAG.
	if cmd, err := tx.Exec(ctx, `
		INSERT INTO dags_history(
			id, namespace, name, version, deleted, created_at, archived_at
		)
		SELECT id, namespace, name, version, deleted, created_at, now()
		  FROM dags d
		 WHERE deleted = TRUE
		   AND NOT EXISTS (
		         SELECT 1 FROM jobs j WHERE j.dag_id = d.id
		       );
	`); err != nil {
		writeErr(w, 500, err)
		return
	} else {
		dagsArchived += cmd.RowsAffected()
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM dags d
		 WHERE deleted = TRUE
		   AND NOT EXISTS (
		         SELECT 1 FROM jobs j WHERE j.dag_id = d.id
		       );
	`); err != nil {
		writeErr(w, 500, err)
		return
	}

	// Jobs are prunable when finished and either in a terminal state or explicitly deleted.
	// (status IN ('succeeded','failed','cancelled') OR deleted = TRUE) AND finished_at IS NOT NULL
	const jobPredicate = "(status IN ('succeeded','failed','cancelled') OR deleted = TRUE) AND finished_at IS NOT NULL"

	// Archive dependencies touching pruned jobs.
	depSQL := "INSERT INTO job_dependencies_history(" +
		"dag_id, parent_job_id, child_job_id, dependency_type, archived_at) " +
		"SELECT jd.dag_id, jd.parent_job_id, jd.child_job_id, jd.dependency_type, now() " +
		"FROM job_dependencies jd " +
		"WHERE jd.parent_job_id IN (SELECT id FROM jobs WHERE " + jobPredicate + ") " +
		"   OR jd.child_job_id  IN (SELECT id FROM jobs WHERE " + jobPredicate + ");"

	if cmd, err := tx.Exec(ctx, depSQL); err != nil {
		writeErr(w, 500, err)
		return
	} else {
		depsArchived += cmd.RowsAffected()
	}

	// Archive jobs themselves.
	jobInsertSQL := "INSERT INTO jobs_history(" +
		"id, dag_id, def_id, status, priority, due_at, " +
		"payload_json, binary_data, lease_owner, lease_until, " +
		"enqueued_at, started_at, finished_at, last_error, " +
		"version, deleted, archived_at) " +
		"SELECT id, dag_id, def_id, status, priority, due_at, " +
		"payload_json, binary_data, lease_owner, lease_until, " +
		"enqueued_at, started_at, finished_at, last_error, " +
		"version, deleted, now() " +
		"FROM jobs WHERE " + jobPredicate + ";"

	if cmd, err := tx.Exec(ctx, jobInsertSQL); err != nil {
		writeErr(w, 500, err)
		return
	} else {
		jobsArchived += cmd.RowsAffected()
	}

	jobDeleteSQL := "DELETE FROM jobs WHERE " + jobPredicate + ";"
	if _, err := tx.Exec(ctx, jobDeleteSQL); err != nil {
		writeErr(w, 500, err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		writeErr(w, 500, err)
		return
	}

	writeJSON(w, 200, map[string]any{
		"job_definitions_archived": defsArchived,
		"dags_archived":            dagsArchived,
		"jobs_archived":            jobsArchived,
		"dependencies_archived":    depsArchived,
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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}



