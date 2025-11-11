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

	// Health
	r.Get("/healthz", h.healthz)

	// ===== Namespaces =====
	r.Route("/namespaces", func(r chi.Router) {
		r.Get("/", h.listNamespaces)
		r.Post("/", h.createNamespace)
	})
	r.Route("/namespace/{name}", func(r chi.Router) {
		r.Get("/", h.getNamespace)
		r.Put("/", h.renameNamespace)
		r.Delete("/", h.deleteNamespace)
	})

	// ===== DAGs =====
	r.Route("/dags/{namespace_id}", func(r chi.Router) {
		r.Get("/", h.listDAGs)
		r.Put("/", h.bulkUpsertDAGs)
		r.Post("/", h.createDAGs)
	})
	r.Route("/dags/{namespace_id}/{id}", func(r chi.Router) {
		r.Get("/", h.getDAG)
		r.Put("/", h.updateDAG)
		r.Delete("/", h.deleteDAGByID)
	})

	// ===== Definitions =====
	r.Route("/definitions/{namespace_id}", func(r chi.Router) {
		r.Get("/", h.listDefinitions)
		r.Put("/", h.bulkUpsertDefinitions)
		r.Post("/", h.createDefinitions)
	})
	r.Route("/definitions/{namespace_id}/{id}", func(r chi.Router) {
		r.Get("/", h.getDefinition)
		r.Put("/", h.updateDefinition)
		r.Post("/", h.newDefinitionVersion)
	})

	// ===== Jobs =====
	r.Route("/dags/{namespace_id}/{dagId}/jobs", func(r chi.Router) {
		r.Get("/", h.listJobs)
		r.Put("/", h.bulkUpsertJobs)
		r.Post("/", h.createJobs)
	})
	r.Route("/dags/{namespace_id}/{dagId}/jobs/{id}", func(r chi.Router) {
		r.Get("/", h.getJobByPath)
		r.Put("/", h.updateJob)
		r.Delete("/", h.deleteJob)
	})

	// ===== Dependencies =====
	r.Route("/dags/{namespace_id}/{dagId}/dependencies", func(r chi.Router) {
		r.Get("/", h.listDependencies)
		r.Put("/", h.bulkUpsertDependencies)
		r.Post("/", h.createDependencies)
	})

	// ===== Job Completion =====
	r.Post("/jobs/{namespace_id}/{jobId}/complete", h.complete)
	r.Post("/jobs/{namespace_id}/{jobId}/fail", h.fail)

	// ===== Admin =====
	r.Get("/admin/check/global-cycles", h.checkGlobalCycles)

	return r
}
