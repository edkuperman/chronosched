package api

import (
	"github.com/go-chi/chi/v5"
	httpSwagger "github.com/swaggo/http-swagger"
)

func NewRouter(h *Handlers) *chi.Mux {
	r := chi.NewRouter()

	// Swagger UI at root
	r.Get("/*", httpSwagger.WrapHandler)

	r.Get("/healthz", h.healthz)
	r.Post("/dags", h.createDAG)
	r.Delete("/dags/{dagId}", h.deleteDAG)
	r.Post("/definitions", h.createDefinition)
	r.Post("/dags/{dagId}/jobs", h.addJob)
	r.Post("/dags/{dagId}/dependencies", h.addDependency)
	r.Post("/jobs/{jobId}/complete", h.complete)
	r.Post("/jobs/{jobId}/fail", h.fail)
	r.Get("/jobs/{jobId}", h.getJob)

	return r
}
