import os
from typing import Any

import requests


class ChronoschedClient:
    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or os.getenv("CHRONOSCHED_BASE_URL") or "http://localhost:8080").rstrip("/")
        self.session = requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _request(self, method: str, path: str, **kwargs):
        resp = self.session.request(method, self._url(path), timeout=15, **kwargs)
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {path} failed: {resp.status_code} {resp.text}")
        if resp.status_code == 204 or not resp.text:
            return None
        return resp.json()

    def healthz(self):
        return self._request("GET", "/healthz")

    def post_job_event(self, job_id: int, status: str, **payload: Any):
        body = {"status": status}
        body.update(payload)
        return self._request("POST", f"/api/v1/jobs/{job_id}/events", json=body)

    def create_namespace(self, name: str):
        return self._request("POST", "/api/v1/namespaces", json={"name": name})

    def list_namespaces(self):
        return self._request("GET", "/api/v1/namespaces")

    def get_namespace(self, name: str):
        return self._request("GET", f"/api/v1/namespaces/{name}")

    def create_job_definition(self, *, namespace_id: str, name: str, description: str = "", kind: str = "cmd",
                              payload_template=None, schedule=None, is_enabled: bool = True):
        return self._request("POST", "/api/v1/job-definitions", json={
            "namespace_id": namespace_id,
            "name": name,
            "description": description,
            "kind": kind,
            "payload_template": payload_template or {},
            "schedule": schedule,
            "is_enabled": is_enabled,
        })

    def list_job_definitions(self, namespace_id: str):
        return self._request("GET", f"/api/v1/namespaces/{namespace_id}/job-definitions")

    def get_job_definition(self, definition_id: str):
        return self._request("GET", f"/api/v1/job-definitions/{definition_id}")

    def update_job_definition(self, definition_id: str, **payload):
        return self._request("PUT", f"/api/v1/job-definitions/{definition_id}", json=payload)

    def delete_job_definition(self, definition_id: str):
        return self._request("DELETE", f"/api/v1/job-definitions/{definition_id}")

    def get_job_definition_usages(self, definition_id: str):
        return self._request("GET", f"/api/v1/job-definitions/{definition_id}/usages")

    def enable_job_definition(self, definition_id: str):
        return self._request("POST", f"/api/v1/job-definitions/{definition_id}/enable")

    def disable_job_definition(self, definition_id: str):
        return self._request("POST", f"/api/v1/job-definitions/{definition_id}/disable")

    def pause_job_definition(self, definition_id: str):
        return self._request("POST", f"/api/v1/job-definitions/{definition_id}/pause")

    def resume_job_definition(self, definition_id: str):
        return self._request("POST", f"/api/v1/job-definitions/{definition_id}/resume")

    def list_dags(self, namespace_id: str):
        return self._request("GET", f"/api/v1/namespaces/{namespace_id}/dags")

    def create_dag(self, namespace_id: str, *, name: str, description: str = ""):
        return self._request("POST", f"/api/v1/namespaces/{namespace_id}/dags", json={"name": name, "description": description})

    def get_dag(self, dag_id: str):
        return self._request("GET", f"/api/v1/dags/{dag_id}")

    def delete_dag(self, dag_id: str):
        return self._request("DELETE", f"/api/v1/dags/{dag_id}")

    def list_dag_versions(self, dag_id: str):
        return self._request("GET", f"/api/v1/dags/{dag_id}/versions")

    def publish_dag_version(self, dag_id: str, *, version_note: str = "", based_on_version_id=None, nodes=None, edges=None):
        return self._request("POST", f"/api/v1/dags/{dag_id}/versions", json={
            "version_note": version_note,
            "based_on_version_id": based_on_version_id,
            "nodes": nodes or [],
            "edges": edges or [],
        })

    def get_dag_version(self, dag_version_id: str):
        return self._request("GET", f"/api/v1/dag-versions/{dag_version_id}")

    def activate_dag_version(self, dag_version_id: str):
        return self._request("POST", f"/api/v1/dag-versions/{dag_version_id}/activate")

    def revert_dag_version(self, dag_version_id: str, activate: bool = False, note: str = "revert copy"):
        return self._request("POST", f"/api/v1/dag-versions/{dag_version_id}/revert", json={"activate": activate, "note": note})

    def get_dag_graph(self, dag_version_id: str):
        return self._request("GET", f"/api/v1/dag-versions/{dag_version_id}/graph")

    def trigger_run(self, dag_id: str, dag_version_id: str | None = None):
        payload = {}
        if dag_version_id:
            payload["dag_version_id"] = dag_version_id
        return self._request("POST", f"/api/v1/dags/{dag_id}/runs", json=payload)

    def list_runs(self, dag_id: str):
        return self._request("GET", f"/api/v1/dags/{dag_id}/runs")

    def get_run(self, run_id: int):
        return self._request("GET", f"/api/v1/runs/{run_id}")

    def list_run_jobs(self, run_id: int):
        return self._request("GET", f"/api/v1/runs/{run_id}/jobs")

    def get_run_graph(self, run_id: int):
        return self._request("GET", f"/api/v1/runs/{run_id}/graph")

    def get_job_readiness(self, job_id: int):
        return self._request("GET", f"/api/v1/jobs/{job_id}/readiness")
