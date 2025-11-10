import uuid
import requests
import json
import time
import os

BASE = os.getenv("CHRONOSCHED_BASE", "http://localhost:8080")

def post(path, body):
    r = requests.post(BASE + path, json=body)
    print(f"POST {path} -> {r.status_code}")
    if r.text:
        try:
            print(json.dumps(r.json(), indent=2))
        except Exception:
            print(r.text)
    print("-" * 60)
    r.raise_for_status()
    return r.json() if r.headers.get("content-type", "").startswith("application/json") else None


def get(path):
    r = requests.get(BASE + path)
    print(f"GET {path} -> {r.status_code}")
    if r.text:
        try:
            print(json.dumps(r.json(), indent=2))
        except Exception:
            print(r.text)
    print("-" * 60)
    r.raise_for_status()
    return r.json() if r.headers.get("content-type", "").startswith("application/json") else None


# --------------------------------------------------
# 1. Create a DAG
# --------------------------------------------------
dag_id = str(uuid.uuid4())
print(f"Creating DAG {dag_id}")
post("/dags", {"id": dag_id, "namespace": "finance", "name": "daily_etl"})

# --------------------------------------------------
# 2. Create job definitions
# --------------------------------------------------
defs = {}
jobs = [("Wait", "echo *** 1 Start Wait && sleep 0 && echo Done Wait ***"), 
        ("Wait5s", "echo *** 2 Start Wait5s && sleep 5 && echo Done Wait5s ***"), 
        ("Wait10s", "echo *** 3 Start Wait10s && sleep 10 && echo Done Wait10s ***")]

for name, cmd in jobs:
    res = post(
        "/definitions",
        {
            "namespace": "core",
            "name": name,
            "version": 1,
            "kind": "cmd",
            "payloadTemplate": {"cmd": cmd},
        },
    )
    defs[name] = res

# --------------------------------------------------
# 3. Add jobs to DAG
# --------------------------------------------------
job_ids = {}
for name in ["Wait", "Wait5s", "Wait10s"]:
    def_id = defs.get(name, {}).get("def_id")   # use the real definition ID if the API returns one
    body = {
        "defId": def_id or str(uuid.uuid4()),
        "priority": 1,
        "dueAt": None,
        "payload": {"cmd": f"echo running {name}"},
    }
    res = post(f"/dags/{dag_id}/jobs", body)
    job_ids[name] = res.get("id")

# --------------------------------------------------
# 4. Add dependencies (Wait -> Wait5s -> Wait10s)
# --------------------------------------------------
if job_ids["Wait"] and job_ids["Wait5s"]:
    post(
        f"/dags/{dag_id}/dependencies",
        {"parentJobId": job_ids["Wait"], "childJobId": job_ids["Wait5s"]},
    )

if job_ids["Wait5s"] and job_ids["Wait10s"]:
    post(
        f"/dags/{dag_id}/dependencies",
        {"parentJobId": job_ids["Wait5s"], "childJobId": job_ids["Wait10s"]},
    )

# --------------------------------------------------
# 5. Simulate job completion flow
# --------------------------------------------------
time.sleep(2)
print("Marking first job complete ...")
post(f"/jobs/{job_ids['Wait']}/complete", {})

time.sleep(2)
print("Marking second job complete ...")
post(f"/jobs/{job_ids['Wait5s']}/complete", {})

# --------------------------------------------------
# 6. Query back final job states
# --------------------------------------------------
for name, jid in job_ids.items():
    print(f"Querying job {name} (id={jid})")
    try:
        get(f"/jobs/{jid}")
    except requests.exceptions.RequestException:
        print(f"Job {jid} endpoint not implemented yet.")
    time.sleep(0.5)

print("Demo completed")
