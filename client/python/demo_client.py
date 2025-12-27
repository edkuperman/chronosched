#!/usr/bin/env python3
import requests
import json
import time

BASE = "http://server:8080"
API_PREFIX = "/api/v1"


# ------------------------------------------------------------
# Request helpers
# ------------------------------------------------------------
def request_root(method: str, url: str, **kwargs):
    r = requests.request(method, url, **kwargs)
    print(f"{method.upper()} {url}")
    print(f"-> {r.status_code}")

    # Print JSON or raw text
    if r.status_code >= 400:
        try:
            print(json.dumps(r.json(), indent=2))
        except Exception:
            print(r.text)

    r.raise_for_status()

    try:
        return r.json()
    except Exception:
        return r.text


def request(method: str, path: str, **kwargs):
    # /healthz is unversioned
    if path == "/healthz":
        url = BASE + path
    else:
        url = BASE + API_PREFIX + path
    return request_root(method, url, **kwargs)


# ------------------------------------------------------------
# Simple wrappers around API endpoints
# ------------------------------------------------------------
def create_namespace(name: str) -> str:
    body = {"name": name}
    resp = request("POST", "/namespaces", json=body)
    return resp["namespace_id"]


def create_dag(namespace_id: str, name: str) -> str:
    body = [{"name": name}]
    resp = request("POST", f"/dags/{namespace_id}", json=body)
    dag_id = resp[0]["id"]
    print(f"Created DAG {name} = {dag_id}")
    return dag_id


def create_definitions(namespace_id: str, defs):
    """defs is a list of dicts with name/kind/payload_template/etc."""
    resp = request("POST", f"/definitions/{namespace_id}", json=defs)
    return resp


def create_jobs(dag_id: str, jobs):
    """jobs is list of {def_id, depends_on}."""
    resp = request("POST", f"/dags/{dag_id}/jobs", json=jobs)
    return resp


# ------------------------------------------------------------
# Non-cron DAG demo
# ------------------------------------------------------------
def create_and_run_non_cron_demo(namespace_id: str):
    print("------------------------------------------------------------")
    print("Running non-cron DAG demo...")

    # 1. Create DAG
    dag_id = create_dag(namespace_id, "quick_run")

    # 2. Prepare definitions (payload_template must be JSON string)
    defs = [
        {
            "name": "Root",
            "kind": "cmd",
            "payload_template": {"cmd": "echo ROOT"},
        },
        {
            "name": "Child5s",
            "kind": "cmd",
            "payload_template": {"cmd": "echo CHILD && sleep 5"},
        },
        {
            "name": "Leaf",
            "kind": "cmd",
            "payload_template": {"cmd": "echo LEAF"},
        },
    ]

    # 3. Create definitions using API
    defs_resp = create_definitions(namespace_id, defs)

    # ---- FIX PART: Extract list correctly ----
    defs_list = defs_resp["results"]

    # Create name → def_id mapping
    defs_by_name = {d["name"]: d["def_id"] for d in defs_list}

    root_def_id = defs_by_name["Root"]
    child_def_id = defs_by_name["Child5s"]
    leaf_def_id = defs_by_name["Leaf"]

    # 4. Create jobs with dependencies
    jobs = [
        {"def_id": root_def_id,  "depends_on": []},
        {"def_id": child_def_id, "depends_on": [root_def_id]},
        {"def_id": leaf_def_id,  "depends_on": [child_def_id]},
    ]

    job_resp = create_jobs(dag_id, jobs)

    print("Created jobs:")
    for j in job_resp:
        print(j)

    print("Demo DAG created; workers should process jobs in order")
    print("------------------------------------------------------------")


# ------------------------------------------------------------
# Cron scheduler demo
# ------------------------------------------------------------
def test_scheduler(namespace_id: str):
    print("------------------------------------------------------------")
    print("Testing scheduler...")

    # 1. Create a DAG for scheduled definitions
    cron_dag_id = create_dag(namespace_id, "ScheduledTest")

    # 2. Create cron-enabled job definition
    body = [
        {
            "name": "ScheduledTest",
            "kind": "cmd",
            "payload_template": {"cmd": "echo CRON_TEST"},
            "cron_spec": "*/5 * * * * *",  # every 5 seconds
        }
    ]
    defs = create_definitions(namespace_id, body)
    def_id = defs["results"][0]["def_id"]
    print(f"Created cron-enabled job definition = {def_id}")

    # 3. wait 12 seconds for scheduler to fire at least twice
    print("Waiting ~12 seconds for scheduler to enqueue jobs...")
    time.sleep(12)

    # 4. Fetch jobs for namespace
    jobs = request("GET", f"/jobs/{namespace_id}")

    # Identify scheduler-produced jobs
    sched_jobs = [
        j
        for j in jobs
        if j.get("def_id") == def_id and j.get("dag_id") == cron_dag_id
    ]

    print(f"Found {len(sched_jobs)} scheduler jobs")

    if len(sched_jobs) < 2:
        raise RuntimeError(
            f"Scheduler did not produce enough jobs; expected >=2 got {len(sched_jobs)}"
        )

    print("Scheduler jobs:")
    for j in sched_jobs:
        print(j)
    print("------------------------------------------------------------")


# ------------------------------------------------------------
# Main program
# ------------------------------------------------------------
def main():
    print("Starting Chronosched demo...")

    # Health check
    try:
        request("GET", "/healthz")
    except Exception:
        print("Server not ready; waiting 2 seconds...")
        time.sleep(2)
        request("GET", "/healthz")

    # Create namespace
    ns = "demo"
    print(f"Ensuring namespace '{ns}' exists...")
    namespace_id = create_namespace(ns)
    print(f"Namespace created: {namespace_id}")
    print("------------------------------------------------------------")

    # 1. non-cron demo
    create_and_run_non_cron_demo(namespace_id)

    print("Waiting 10 seconds for worker to execute non-cron jobs...")
    time.sleep(10)

    # 2. cron scheduler demo
    test_scheduler(namespace_id)

    print("All demo tests completed successfully.")


if __name__ == "__main__":
    main()
