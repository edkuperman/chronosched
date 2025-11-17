package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// NewRouter wires all HTTP routes to the Handlers.
//
// Semantics (transitional, but user-facing correct):
//   - DAGs are workflows
//   - /dags/{dag_id}/jobs = DAG-scoped jobs (DAG "nodes")
//   - /dags/{dag_id}/dependencies = edges between those jobs
//   - /jobs/{namespace_id} = namespace-wide job diagnostics
func NewRouter(h *Handlers) http.Handler {
	r := chi.NewRouter()

	// Basic middleware
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// --------------------------
	// Health
	// --------------------------
	r.Get("/healthz", h.healthz)

	// --------------------------
	// Namespaces
	// --------------------------
	r.Get("/api/v1/namespaces", h.listNamespaces)
	r.Post("/api/v1/namespaces", h.createNamespace)

	r.Get("/api/v1/namespace/{name}", h.getNamespace)
	r.Put("/api/v1/namespace/{name}", h.renameNamespace)
	r.Delete("/api/v1/namespace/{name}", h.deleteNamespace)

	// --------------------------
	// DAGs (workflows)
	// --------------------------
	// Collection in a namespace
	r.Get("/api/v1/dags/{namespace_id}", h.listDAGs)
	r.Post("/api/v1/dags/{namespace_id}", h.createDAGs)
	r.Put("/api/v1/dags/{namespace_id}", h.bulkUpsertDAGs)

	// Single DAG by id within a namespace
	r.Get("/api/v1/dags/{namespace_id}/{id}", h.getDAG)
	r.Put("/api/v1/dags/{namespace_id}/{id}", h.updateDAG)
	r.Delete("/api/v1/dags/{namespace_id}/{id}", h.deleteDAG)

	// --------------------------
	// Job Definitions (templates)
	// --------------------------
	r.Get("/api/v1/definitions/{namespace_id}", h.listDefinitions)
	r.Post("/api/v1/definitions/{namespace_id}", h.createDefinitions)
	r.Put("/api/v1/definitions/{namespace_id}", h.bulkUpsertDefinitions)

	r.Get("/api/v1/definitions/{namespace_id}/{id}", h.getDefinition)
	r.Put("/api/v1/definitions/{namespace_id}/{id}", h.updateDefinition)
	r.Delete("/api/v1/definitions/{namespace_id}/{id}", h.deleteDefinition)

	// --------------------------
	// Jobs (DAG-scoped "nodes")
	// --------------------------
	// These represent the jobs that belong to a DAG (the workflow graph).
	r.Get("/api/v1/dags/{dag_id}/jobs", h.listJobs)
	r.Post("/api/v1/dags/{dag_id}/jobs", h.createJobs)
	r.Put("/api/v1/dags/{dag_id}/jobs", h.bulkUpsertJobs)

	// Single job within a DAG
	r.Get("/api/v1/dags/{dag_id}/jobs/{id}", h.getJob)
	r.Put("/api/v1/dags/{dag_id}/jobs/{id}", h.updateJob)
	r.Delete("/api/v1/dags/{dag_id}/jobs/{id}", h.deleteJob)

	// --------------------------
	// Job lifecycle (DAG-scoped)
	// --------------------------
	r.Post("/api/v1/dags/{dag_id}/jobs/{jobId}/complete", h.complete)
	r.Post("/api/v1/dags/{dag_id}/jobs/{jobId}/fail", h.fail)

	// --------------------------
	// Job Runs (transitional model)
	// --------------------------
	// Views over the existing jobs table.
	r.Get("/api/v1/dags/{dag_id}/runs", h.listRunsInDAG)
	r.Get("/api/v1/dags/{dag_id}/jobs/{job_id}/runs", h.listRunsForJob)
	r.Post("/api/v1/dags/{dag_id}/jobs/{job_id}/retry", h.retryJob)

	// --------------------------
	// Dependencies (DAG-scoped edges)
	// --------------------------
	r.Get("/api/v1/dags/{dag_id}/dependencies", h.listDependencies)
	r.Post("/api/v1/dags/{dag_id}/dependencies", h.createDependencies)
	r.Put("/api/v1/dags/{dag_id}/dependencies", h.bulkUpsertDependencies)
	r.Patch("/api/v1/dags/{dag_id}/dependencies", h.updateDependency)
	r.Delete("/api/v1/dags/{dag_id}/dependencies", h.deleteDependency)

	// --------------------------
	// Namespace-level job diagnostics
	// --------------------------
	// Used by the Python demo to verify scheduler behavior.
	r.Get("/api/v1/jobs/{namespace_id}", h.listJobsInNamespace)

	// --------------------------
	// Admin / Diagnostics
	// --------------------------
	r.Get("/api/v1/admin/check/global-cycles", h.checkGlobalCycles)
	r.Post("/api/v1/admin/prune", h.prune)

	// Cron schedules → DAG mapping introspection
	r.Get("/api/v1/admin/schedules", h.listCronSchedules)

	return r
}
