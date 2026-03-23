package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func NewHTTPHandler(h *Handler) http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", h.healthz)

	r.Get("/", swaggerUIHandler("/openapi/chronosched.yaml"))
	r.Handle("/openapi/*", http.StripPrefix("/openapi/", http.FileServer(http.Dir("openapi"))))
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/namespaces", h.listNamespaces)
		r.Post("/namespaces", h.createNamespace)
		r.Get("/namespaces/{name}", h.getNamespace)

		r.Get("/namespaces/{namespace_id}/job-definitions", h.listDefinitions)
		r.Post("/job-definitions", h.createDefinition)
		r.Get("/job-definitions/{definition_id}", h.getDefinition)
		r.Delete("/job-definitions/{definition_id}", h.deleteDefinition)
		r.Put("/job-definitions/{definition_id}", h.updateDefinition)
		r.Post("/job-definitions/{definition_id}/enable", h.enableDefinition)
		r.Post("/job-definitions/{definition_id}/disable", h.disableDefinition)
		r.Post("/job-definitions/{definition_id}/pause", h.pauseDefinition)
		r.Post("/job-definitions/{definition_id}/resume", h.resumeDefinition)
		r.Get("/job-definitions/{definition_id}/usages", h.definitionUsages)

		r.Get("/namespaces/{namespace_id}/dags", h.listDAGs)
		r.Post("/namespaces/{namespace_id}/dags", h.createDAG)
		r.Get("/dags/{dag_id}", h.getDAG)
		r.Delete("/dags/{dag_id}", h.deleteDAG)
		r.Get("/dags/{dag_id}/versions", h.listDAGVersions)
		r.Post("/dags/{dag_id}/versions", h.createDAGVersion)
		r.Post("/dags/{dag_id}/runs", h.createRun)
		r.Get("/dags/{dag_id}/runs", h.listRuns)

		r.Get("/dag-versions/{dag_version_id}", h.getDAGVersion)
		r.Get("/dag-versions/{dag_version_id}/graph", h.getDAGGraph)
		r.Post("/dag-versions/{dag_version_id}/activate", h.activateDAGVersion)
		r.Post("/dag-versions/{dag_version_id}/revert", h.revertDAGVersion)

		r.Get("/runs/{run_id}", h.getRun)
		r.Get("/runs/{run_id}/jobs", h.listRunJobs)
		r.Get("/runs/{run_id}/graph", h.getRunGraph)

		r.Get("/namespaces/{namespace_id}/jobs/problems", h.listNamespaceProblemJobs)
		r.Post("/namespaces/{namespace_id}/jobs/{job_id}/restart", h.restartNamespaceJob)

		r.Get("/jobs/{job_id}/readiness", h.getJobReadiness)
		r.Post("/jobs/{job_id}/events", h.postJobEvent)
		r.Get("/admin/check/global-cycles", h.checkGlobalCycles)
	})

	r.Route("/internal/workers", func(r chi.Router) {
		r.Post("/lease", h.leaseJobs)
		r.Post("/dispatch-result", h.reportDispatchResult)
	})
	return r
}
