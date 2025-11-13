package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func NewRouter(h *Handlers) http.Handler {
    r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)    

    // Unversioned health endpoint
    r.Get("/healthz", h.healthz)

    // Versioned API routes
    r.Route("/api/v1", func(r chi.Router) {
        // Namespaces
        r.Route("/namespaces", func(r chi.Router) {
            r.Get("/", h.listNamespaces)
            r.Post("/", h.createNamespace)
        })

        r.Route("/namespace/{name}", func(r chi.Router) {
            r.Get("/", h.getNamespace)
            r.Put("/", h.renameNamespace)
            r.Delete("/", h.deleteNamespace)
        })

        // DAGs
        r.Route("/dags/{namespace_id}", func(r chi.Router) {
            r.Get("/", h.listDAGs)
            r.Post("/", h.createDAGs)
            r.Put("/", h.bulkUpsertDAGs)
        })

        r.Route("/dags/{namespace_id}/{id}", func(r chi.Router) {
            r.Get("/", h.getDAG)
            r.Put("/", h.updateDAG)
            r.Delete("/", h.deleteDAGByID)
        })

        // Job definitions
        r.Route("/definitions/{namespace_id}", func(r chi.Router) {
            r.Get("/", h.listDefinitions)
            r.Post("/", h.createDefinitions)
            r.Put("/", h.bulkUpsertDefinitions)
        })

        r.Route("/definitions/{namespace_id}/{id}", func(r chi.Router) {
            r.Get("/", h.getDefinition)
        })

        // Jobs
        r.Route("/dags/{namespace_id}/{dagId}/jobs", func(r chi.Router) {
            r.Get("/", h.listJobs)
            r.Post("/", h.createJobs)
            r.Put("/", h.bulkUpsertJobs)
        })

        r.Route("/dags/{namespace_id}/{dagId}/jobs/{id}", func(r chi.Router) {
            r.Get("/", h.getJobByPath)
        })

        // Dependencies
        r.Route("/dags/{namespace_id}/{dagId}/dependencies", func(r chi.Router) {
            r.Get("/", h.listDependencies)
            r.Post("/", h.createDependencies)
            r.Put("/", h.bulkUpsertDependencies)
        })

        // Job completion / failure
        r.Route("/jobs/{namespace_id}/{jobId}", func(r chi.Router) {
            r.Post("/complete", h.complete)
            r.Post("/fail", h.fail)
        })

        // Admin
        r.Get("/admin/check/global-cycles", h.checkGlobalCycles)
    })

    return r
}
