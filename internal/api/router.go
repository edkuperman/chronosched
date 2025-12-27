package api

import (
    "net/http"

    "github.com/go-chi/chi/v5"
)

// NewHTTPHandler constructs the top-level HTTP router and mounts
// all public and internal routes in a structured way.
func NewHTTPHandler(h *Handler) http.Handler {
    r := chi.NewRouter()

    // Health
    r.Get("/healthz", h.healthz)

    // Public API routes
    r.Route("/api/v1", func(r chi.Router) {
        // Namespaces
        r.Get("/namespaces", h.listNamespaces)
        r.Post("/namespaces", h.createNamespace)
        r.Route("/namespace/{name}", func(r chi.Router) {
            r.Get("/", h.getNamespace)
            r.Put("/", h.renameNamespace)
            r.Delete("/", h.deleteNamespace)
        })

        // DAGs by namespace
        r.Route("/dags/{namespace_id}", func(r chi.Router) {
            r.Get("/", h.listDAGs)
            r.Post("/", h.createDAG)
            r.Put("/", h.upsertDAG)
        })
        r.Route("/dags/{namespace_id}/{id}", func(r chi.Router) {
            r.Get("/", h.getDAG)
            r.Put("/", h.updateDAG)
            r.Delete("/", h.deleteDAG)
        })

        // Definitions by namespace
        r.Route("/definitions/{namespace_id}", func(r chi.Router) {
            r.Get("/", h.listDefinitions)
            r.Post("/", h.createDefinition)
            r.Put("/", h.bulkUpsertDefinitions)
        })
        r.Route("/definitions/{namespace_id}/{id}", func(r chi.Router) {
            r.Get("/", h.getDefinition)
            r.Put("/", h.updateDefinition)
            r.Delete("/", h.deleteDefinition)
        })

        // Jobs under a DAG
        r.Route("/dags/{dag_id}/jobs", func(r chi.Router) {
            r.Get("/", h.listJobs)
            r.Post("/", h.createDagJob)
            r.Put("/", h.bulkUpsertJobs)
        })
        r.Route("/dags/{dag_id}/jobs/{id}", func(r chi.Router) {
            r.Get("/", h.getJob)
            r.Put("/", h.updateJob)
            r.Delete("/", h.deleteJob)
        })
        r.Post("/dags/{dag_id}/jobs/{jobId}/complete", h.completeJob)
        r.Post("/dags/{dag_id}/jobs/{jobId}/fail", h.failJob)

        // Dependencies under a DAG
        r.Route("/dags/{dag_id}/dependencies", func(r chi.Router) {
            r.Get("/", h.listDependencies)
            r.Post("/", h.createDependency)
            r.Put("/", h.bulkUpsertDependencies)
            r.Patch("/", h.patchDependencies)
            r.Delete("/", h.deleteDependencies)
        })

        // Admin
        r.Get("/admin/check/global-cycles", h.checkGlobalCycles)
        r.Post("/admin/prune", h.prune)

        // Existing flat job creation route (legacy)
        r.Post("/jobs", h.createJob)
    })

    // Internal worker gateway
    r.Route("/internal/workers", func(r chi.Router) {
        r.Post("/lease", h.leaseJobs)
        r.Post("/result", h.reportResult)
    })

    return r
}
