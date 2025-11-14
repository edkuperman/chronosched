import uuid
import requests
import json
import os
import time


BASE = os.getenv("CHRONOSCHED_BASE", "http://localhost:8080")
API_PREFIX = "/api/v1"


def request(method: str, path: str, **kwargs):
    url = BASE + API_PREFIX + path
    print(f"{method} {path}")
    r = requests.request(method, url, **kwargs)
    print(f"-> {r.status_code}")
    if r.text:
        try:
            print(json.dumps(r.json(), indent=2))
        except Exception:
            print(r.text)
    print("-" * 60)
    r.raise_for_status()
    if "application/json" in r.headers.get("content-type", ""):
        return r.json()
    return None


def create_namespace(name: str) -> str:
    """
    POST /api/v1/namespaces
    Body: NamespaceCreateRequest { name }
    Returns Namespace { namespace_id, name }
    """
    body = {"name": name}
    obj = request("POST", "/namespaces", json=body)
    ns_id = obj["namespace_id"]
    print(f"Namespace '{name}' created: {ns_id}")
    return ns_id


def create_dag(namespace_id: str, name: str) -> str:
    """
    POST /api/v1/dags/{namespace_id}
    Body: array of DAGCreateRequest [{ name }]
    Returns an array of DAG objects; extract DAG id from response.
    """
    body = [{ "name": name }]
    result = request("POST", f"/dags/{namespace_id}", json=body)

    # result must be a list with 1 element
    if not isinstance(result, list) or len(result) == 0:
        raise RuntimeError("DAG creation returned unexpected response: " + str(result))

    dag_id = result[0]["id"]
    print(f"DAG '{name}' created with id = {dag_id}")
    return dag_id


def create_definitions(namespace_id: str):
    """
    POST /api/v1/definitions/{namespace_id}
    Body: array of JobDefinitionCreateRequest
      { name, kind, payload_template, cron_spec?, delay_interval? }

    We then GET /definitions/{namespace_id} and map by name -> def_id.
    """
    defs_body = [
        {
            "name": "Root",
            "kind": "cmd",
            "payload_template": {"cmd": "echo ROOT && sleep 1 && echo DONE ROOT"},
        },
        {
            "name": "Child5s",
            "kind": "cmd",
            "payload_template": {"cmd": "echo CHILD && sleep 2 && echo DONE CHILD"},
        },
        {
            "name": "Leaf",
            "kind": "cmd",
            "payload_template": {"cmd": "echo LEAF && sleep 1 && echo DONE LEAF"},
        },
    ]

    request("POST", f"/definitions/{namespace_id}", json=defs_body)

    # Now list definitions and map by name -> def_id
    all_defs = request("GET", f"/definitions/{namespace_id}") or []
    by_name = {d["name"]: d["def_id"] for d in all_defs}
    print("Definition IDs:", by_name)
    return by_name


def create_jobs(namespace_id: str, dag_id: str, def_ids: dict) -> dict:
    """
    POST /api/v1/dags/{namespace_id}/{dagId}/jobs
    Body: array of JobCreate [{ def_id, payload, priority?, due_at? }]

    We then GET /dags/{namespace_id}/{dagId}/jobs and map by def_id -> job.id.
    """
    # Create one job per definition
    for name in ["Root", "Child5s", "Leaf"]:
        body = [
            {
                "def_id": def_ids[name],
                # According to YAML, payload is a string; server code treats it as JSON-ish.
                "payload": json.dumps({"cmd": f"echo running {name}"}),
                "priority": 0,
                # due_at omitted (optional)
            }
        ]
        request("POST", f"/dags/{namespace_id}/{dag_id}/jobs", json=body)

    # Now list jobs in this DAG and map them back to names by def_id
    jobs = request("GET", f"/dags/{namespace_id}/{dag_id}/jobs") or []
    def_id_to_name = {v: k for k, v in def_ids.items()}
    job_ids_by_name = {}
    for j in jobs:
        name = def_id_to_name.get(j["def_id"])
        if name and name not in job_ids_by_name:
            job_ids_by_name[name] = j["id"]

    print("Job IDs by name:", job_ids_by_name)
    return job_ids_by_name


def complete_job(namespace_id: str, job_id: int):
    """
    POST /api/v1/jobs/{namespace_id}/{jobId}/complete
    No request body, 204 response.
    """
    request("POST", f"/jobs/{namespace_id}/{job_id}/complete")


def fail_job(namespace_id: str, job_id: int):
    """
    POST /api/v1/jobs/{namespace_id}/{jobId}/fail
    No request body, 204 response.
    """
    request("POST", f"/jobs/{namespace_id}/{job_id}/fail")


def prune():
    """
    POST /api/v1/admin/prune
    """
    request("POST", "/admin/prune")

def test_scheduler(namespace_id: str):
    """
    Create a definition with a cron_spec that fires every 5 seconds.
    Wait for 2–3 jobs to be created by scheduler and verify they appear.
    """

    print("----- Testing Scheduler -----")

    # 1. Create cron-enabled job definition
    body = [
        {
            "name": "ScheduledTest",
            "kind": "cmd",
            "payload_template": {"cmd": "echo CRON_TEST"},
            "cron_spec": "*/5 * * * * *"  # every 5 seconds (note: 6-field cron)
        }
    ]

    print("Creating scheduled job definition...")
    defs = request("POST", f"/definitions/{namespace_id}", json=body)

    # 2. Fetch definition ID
    all_defs = request("GET", f"/definitions/{namespace_id}") or []
    def_map = {d["name"]: d["def_id"] for d in all_defs}
    def_id = def_map["ScheduledTest"]

    print(f"Scheduled definition created with def_id = {def_id}")

    print("Waiting 12 seconds for scheduler to fire twice...")

    # 3. Sleep long enough for 2 scheduler ticks
    time.sleep(12)

    # 4. Fetch all jobs globally (jobs without DAG)
    jobs = request("GET", f"/jobs/{namespace_id}") or []

    # Filter scheduler-generated jobs (dag_id is null)
    sched_jobs = [
        j for j in jobs
        if j.get("def_id") == def_id and j.get("dag_id") is None
    ]

    print("Scheduler jobs found:")
    for j in sched_jobs:
        print(json.dumps(j, indent=2))

    # 5. Verify we got at least 2 jobs
    if len(sched_jobs) < 2:
        raise RuntimeError(
            f"Scheduler did not produce enough jobs; expected >=2, got {len(sched_jobs)}"
        )

    print("Scheduler test PASSED.")
    print("-----------------------------")



def main():
    # 1. Create namespace (NAME -> namespace_id)
    ns_name = "demo"
    namespace_id = create_namespace(ns_name)

    # 2. Create DAG in that namespace
    dag_id = create_dag(namespace_id, "quick_run")

    # 3. Create job definitions and discover their def_ids
    def_ids = create_definitions(namespace_id)

    # 4. Test cron scheduler
    test_scheduler(namespace_id)

    # 5. Create jobs tied to those definitions and DAG
    job_ids = create_jobs(namespace_id, dag_id, def_ids)

    # 6. Simulate running the DAG
    print("Simulating DAG execution...")

    root_id = job_ids["Root"]
    child_id = job_ids["Child5s"]
    leaf_id = job_ids["Leaf"]

    print("Completing Root")
    complete_job(namespace_id, root_id)
    time.sleep(1)

    print("Completing Child5s")
    complete_job(namespace_id, child_id)
    time.sleep(1)

    print("Completing Leaf")
    complete_job(namespace_id, leaf_id)

    # 6. Prune
    print("Pruning archived entities...")
    prune()
    print("Done.")


if __name__ == "__main__":
    main()
