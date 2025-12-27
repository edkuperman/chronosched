package api

import (
    "encoding/json"
    "net/http"
    "strconv"

    "github.com/edkuperman/chronosched/internal/repository"
    "github.com/go-chi/chi/v5"
)

// Helpers

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    _ = json.NewEncoder(w).Encode(v)
}

// Health
func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ===== Namespaces =====

type namespaceCreateRequest struct {
    Name string `json:"name"`
}

type namespaceRenameRequest struct {
    NewName string `json:"new_name"`
}

func (h *Handler) listNamespaces(w http.ResponseWriter, r *http.Request) {
    namespaces, err := h.Repos.Namespaces.List(r.Context())
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusOK, namespaces)
}

func (h *Handler) createNamespace(w http.ResponseWriter, r *http.Request) {
    var req namespaceCreateRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }
    if req.Name == "" {
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
    name := chi.URLParam(r, "name")
    ns, err := h.Repos.Namespaces.GetByName(r.Context(), name)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }
    writeJSON(w, http.StatusOK, ns)
}

func (h *Handler) renameNamespace(w http.ResponseWriter, r *http.Request) {
    name := chi.URLParam(r, "name")
    var req namespaceRenameRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }
    if req.NewName == "" {
        http.Error(w, "new_name is required", http.StatusBadRequest)
        return
    }

    _, err := h.Repos.Namespaces.Rename(r.Context(), name, req.NewName)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    writeJSON(w, http.StatusOK, map[string]string{
        "old": name,
        "new": req.NewName,
    })
}

func (h *Handler) deleteNamespace(w http.ResponseWriter, r *http.Request) {
    name := chi.URLParam(r, "name")
    if err := h.Repos.Namespaces.Delete(r.Context(), name); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

// ===== DAGs =====

type dagCreateRequest struct {
    Name    string `json:"name"`
    Version int    `json:"version,omitempty"`
}

type dagUpsertResult struct {
    ID      string `json:"id,omitempty"`
    Name    string `json:"name"`
    Version int    `json:"version"`
    Error   string `json:"error,omitempty"`
}

type dagUpsertResponse struct {
    Results []dagUpsertResult `json:"results"`
}

func (h *Handler) listDAGs(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    dags, err := h.Repos.DAGs.ListByNamespace(r.Context(), nsID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusOK, dags)
}

func (h *Handler) createDAG(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    var reqs []dagCreateRequest
    if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }
    var created []repository.DAG
    for _, req := range reqs {
        if req.Name == "" {
            continue
        }
        version := req.Version
        if version == 0 {
            version = 1
        }
        dag, err := h.Repos.DAGs.Create(r.Context(), nsID, req.Name, version)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        created = append(created, *dag)
    }
    writeJSON(w, http.StatusCreated, created)
}

func (h *Handler) upsertDAG(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    var reqs []dagCreateRequest
    if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }
    resp := dagUpsertResponse{}
    for _, req := range reqs {
        if req.Name == "" {
            resp.Results = append(resp.Results, dagUpsertResult{
                Name:  "",
                Error: "name is required",
            })
            continue
        }
        version := req.Version
        if version == 0 {
            version = 1
        }
        dag, err := h.Repos.DAGs.Upsert(r.Context(), repository.DAG{
            Namespace: nsID,
            Name:      req.Name,
            Version:   version,
        })
        if err != nil {
            resp.Results = append(resp.Results, dagUpsertResult{
                Name:    req.Name,
                Version: version,
                Error:   err.Error(),
            })
        } else {
            resp.Results = append(resp.Results, dagUpsertResult{
                ID:      dag.ID,
                Name:    dag.Name,
                Version: dag.Version,
            })
        }
    }
    // OpenAPI uses 207 for multi-status
    writeJSON(w, http.StatusMultiStatus, resp)
}

func (h *Handler) getDAG(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    id := chi.URLParam(r, "id")
    dag, err := h.Repos.DAGs.Get(r.Context(), nsID, id)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }
    writeJSON(w, http.StatusOK, dag)
}

type dagUpdateRequest struct {
    Name string `json:"name"`
}

func (h *Handler) updateDAG(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    id := chi.URLParam(r, "id")
    var req dagUpdateRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }
    dag, err := h.Repos.DAGs.Get(r.Context(), nsID, id)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }
    dag.Name = req.Name
    updated, err := h.Repos.DAGs.Update(r.Context(), *dag)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusOK, updated)
}

func (h *Handler) deleteDAG(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    id := chi.URLParam(r, "id")
    if err := h.Repos.DAGs.Delete(r.Context(), nsID, id); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

// ===== Definitions =====

type jobDefinitionCreateRequest struct {
    Name            string                 `json:"name"`
    Kind            string                 `json:"kind"`
    PayloadTemplate map[string]interface{} `json:"payload_template"`
    CronSpec        *string                `json:"cron_spec,omitempty"`
    DelayInterval   *string                `json:"delay_interval,omitempty"`
}

type jobDefinitionResult struct {
    DefID   string `json:"def_id,omitempty"`
    Name    string `json:"name"`
    Version int    `json:"version"`
    Error   string `json:"error,omitempty"`
}

type bulkDefinitionResponse struct {
    Results []jobDefinitionResult `json:"results"`
}

func (h *Handler) listDefinitions(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    defs, err := h.Repos.Definitions.ListByNamespace(r.Context(), nsID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusOK, defs)
}

func (h *Handler) createDefinition(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    var reqs []jobDefinitionCreateRequest
    if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }

    resp := bulkDefinitionResponse{}
    for _, req := range reqs {
        if req.Name == "" || req.Kind == "" {
            resp.Results = append(resp.Results, jobDefinitionResult{
                Name:  req.Name,
                Error: "name and kind are required",
            })
            continue
        }
        payloadBytes, err := json.Marshal(req.PayloadTemplate)
        if err != nil {
            resp.Results = append(resp.Results, jobDefinitionResult{
                Name:  req.Name,
                Error: "invalid payload_template",
            })
            continue
        }

        def := repository.JobDefinition{
            Namespace:       nsID,
            Name:            req.Name,
            Version:         1,
            Kind:            req.Kind,
            PayloadTemplate: payloadBytes,
            CronSpec:        req.CronSpec,
            DelayInterval:   req.DelayInterval,
        }
        created, err := h.Repos.Definitions.Create(r.Context(), def)
        if err != nil {
            resp.Results = append(resp.Results, jobDefinitionResult{
                Name:  req.Name,
                Error: err.Error(),
            })
        } else {
            resp.Results = append(resp.Results, jobDefinitionResult{
                DefID:   created.DefID,
                Name:    created.Name,
                Version: created.Version,
            })
        }
    }

    writeJSON(w, http.StatusCreated, resp)
}

func (h *Handler) bulkUpsertDefinitions(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    var reqs []jobDefinitionCreateRequest
    if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }

    // Convert to repository models
    var defs []repository.JobDefinition
    for _, req := range reqs {
        if req.Name == "" || req.Kind == "" {
            continue
        }
        payloadBytes, err := json.Marshal(req.PayloadTemplate)
        if err != nil {
            continue
        }
        defs = append(defs, repository.JobDefinition{
            Namespace:       nsID,
            Name:            req.Name,
            Version:         1,
            Kind:            req.Kind,
            PayloadTemplate: payloadBytes,
            CronSpec:        req.CronSpec,
            DelayInterval:   req.DelayInterval,
        })
    }

    err := h.Repos.Definitions.BulkUpsert(r.Context(), nsID, defs)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // Build a coarse multi-status-style response
    resp := bulkDefinitionResponse{}
    for _, d := range defs {
        resp.Results = append(resp.Results, jobDefinitionResult{
            Name:    d.Name,
            Version: d.Version,
        })
    }
    writeJSON(w, http.StatusMultiStatus, resp)
}

func (h *Handler) getDefinition(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    defID := chi.URLParam(r, "id")
    def, err := h.Repos.Definitions.Get(r.Context(), nsID, defID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }
    writeJSON(w, http.StatusOK, def)
}

func (h *Handler) updateDefinition(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    defID := chi.URLParam(r, "id")
    var req jobDefinitionCreateRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }

    def, err := h.Repos.Definitions.Get(r.Context(), nsID, defID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }

    if req.Name != "" {
        def.Name = req.Name
    }
    if req.Kind != "" {
        def.Kind = req.Kind
    }
    if req.PayloadTemplate != nil {
        payloadBytes, err := json.Marshal(req.PayloadTemplate)
        if err != nil {
            http.Error(w, "invalid payload_template", http.StatusBadRequest)
            return
        }
        def.PayloadTemplate = payloadBytes
    }
    def.CronSpec = req.CronSpec
    def.DelayInterval = req.DelayInterval

    updated, err := h.Repos.Definitions.Update(r.Context(), *def)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusOK, updated)
}

func (h *Handler) deleteDefinition(w http.ResponseWriter, r *http.Request) {
    nsID := chi.URLParam(r, "namespace_id")
    defID := chi.URLParam(r, "id")
    if err := h.Repos.Definitions.Delete(r.Context(), nsID, defID); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

// ===== Jobs under DAG =====

type jobCreateRequest struct {
    DefID    string                 `json:"def_id"`
    Payload  map[string]interface{} `json:"payload_json"`
    Priority int                    `json:"priority"`
}

func (h *Handler) listJobs(w http.ResponseWriter, r *http.Request) {
    dagID := chi.URLParam(r, "dag_id")
    jobs, err := h.Repos.Jobs.ListByDAG(r.Context(), dagID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusOK, jobs)
}

func (h *Handler) createDagJob(w http.ResponseWriter, r *http.Request) {
    dagID := chi.URLParam(r, "dag_id")
    var req jobCreateRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }
    if req.DefID == "" {
        http.Error(w, "def_id is required", http.StatusBadRequest)
        return
    }
    payloadBytes, err := json.Marshal(req.Payload)
    if err != nil {
        http.Error(w, "invalid payload_json", http.StatusBadRequest)
        return
    }
    job, err := h.Repos.Jobs.Create(r.Context(), dagID, req.DefID, payloadBytes, req.Priority)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusCreated, job)
}

// For now, bulkUpsertJobs is not used; implement as 501 to avoid silent behavior.
func (h *Handler) bulkUpsertJobs(w http.ResponseWriter, r *http.Request) {
    http.Error(w, "bulk job upsert not implemented", http.StatusNotImplemented)
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
    idStr := chi.URLParam(r, "id")
    id, err := strconv.ParseInt(idStr, 10, 64)
    if err != nil {
        http.Error(w, "invalid job id", http.StatusBadRequest)
        return
    }
    job, err := h.Repos.Jobs.Get(r.Context(), id)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }
    writeJSON(w, http.StatusOK, job)
}

func (h *Handler) updateJob(w http.ResponseWriter, r *http.Request) {
    // For simplicity, support status updates via complete/fail endpoints only.
    http.Error(w, "job update not implemented; use /complete or /fail", http.StatusNotImplemented)
}

func (h *Handler) deleteJob(w http.ResponseWriter, r *http.Request) {
    idStr := chi.URLParam(r, "id")
    id, err := strconv.ParseInt(idStr, 10, 64)
    if err != nil {
        http.Error(w, "invalid job id", http.StatusBadRequest)
        return
    }
    if err := h.Repos.Jobs.Delete(r.Context(), id); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

type jobStateChangeRequest struct {
    Error string `json:"error,omitempty"`
}

func (h *Handler) completeJob(w http.ResponseWriter, r *http.Request) {
    jobIDStr := chi.URLParam(r, "jobId")
    id, err := strconv.ParseInt(jobIDStr, 10, 64)
    if err != nil {
        http.Error(w, "invalid job id", http.StatusBadRequest)
        return
    }
    if err := h.Repos.Jobs.MarkSucceeded(r.Context(), id); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) failJob(w http.ResponseWriter, r *http.Request) {
    jobIDStr := chi.URLParam(r, "jobId")
    id, err := strconv.ParseInt(jobIDStr, 10, 64)
    if err != nil {
        http.Error(w, "invalid job id", http.StatusBadRequest)
        return
    }
    var req jobStateChangeRequest
    _ = json.NewDecoder(r.Body).Decode(&req)
    if err := h.Repos.Jobs.MarkFailed(r.Context(), id, req.Error); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

// ===== Dependencies =====

type dependencyRequest struct {
    ParentJobID    int64  `json:"parent_job_id"`
    ChildJobID     int64  `json:"child_job_id"`
    DependencyType string `json:"dependency_type"`
}

func (h *Handler) listDependencies(w http.ResponseWriter, r *http.Request) {
    dagID := chi.URLParam(r, "dag_id")
    deps, err := h.Repos.Deps.ListByDAG(r.Context(), dagID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusOK, deps)
}

func (h *Handler) createDependency(w http.ResponseWriter, r *http.Request) {
    dagID := chi.URLParam(r, "dag_id")
    var req dependencyRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }
    dep := repository.Dependency{
        ParentJobID:    req.ParentJobID,
        ChildJobID:     req.ChildJobID,
        DependencyType: req.DependencyType,
    }
    if err := h.Repos.Deps.Create(r.Context(), dagID, dep); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusCreated, dep)
}

func (h *Handler) bulkUpsertDependencies(w http.ResponseWriter, r *http.Request) {
    dagID := chi.URLParam(r, "dag_id")
    var reqs []dependencyRequest
    if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }
    var deps []repository.Dependency
    for _, req := range reqs {
        deps = append(deps, repository.Dependency{
            ParentJobID:    req.ParentJobID,
            ChildJobID:     req.ChildJobID,
            DependencyType: req.DependencyType,
        })
    }
    if err := h.Repos.Deps.BulkUpsert(r.Context(), dagID, deps); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusMultiStatus)
}

func (h *Handler) patchDependencies(w http.ResponseWriter, r *http.Request) {
    dagID := chi.URLParam(r, "dag_id")
    var reqs []dependencyRequest
    if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
        http.Error(w, "invalid JSON body", http.StatusBadRequest)
        return
    }
    var deps []repository.Dependency
    for _, req := range reqs {
        deps = append(deps, repository.Dependency{
            ParentJobID:    req.ParentJobID,
            ChildJobID:     req.ChildJobID,
            DependencyType: req.DependencyType,
        })
    }
    if err := h.Repos.Deps.Patch(r.Context(), dagID, deps); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusMultiStatus)
}

func (h *Handler) deleteDependencies(w http.ResponseWriter, r *http.Request) {
    dagID := chi.URLParam(r, "dag_id")
    if err := h.Repos.Deps.DeleteAll(r.Context(), dagID); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

// ===== Admin =====

func (h *Handler) checkGlobalCycles(w http.ResponseWriter, r *http.Request) {
    resp, err := h.Repos.Admin.CheckGlobalCycles(r.Context())
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) prune(w http.ResponseWriter, r *http.Request) {
    summary, err := h.Repos.Admin.Prune(r.Context())
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    writeJSON(w, http.StatusOK, summary)
}
