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
post("/dags", {"id": dag_id, "namespace": "demo", "name": "quick_run"})

# --------------------------------------------------
# 2. Create job definitions
# --------------------------------------------------
# Mix: dependency jobs + cron jobs for the demo
defs = {}
definitions = [
    # simple DAG jobs
    {"name": "Root", "kind": "cmd", "payload": {"cmd": "echo ROOT && sleep 1 && echo DONE ROOT"}},
    {"name": "Child5s", "kind": "cmd", "payload": {"cmd": "echo CHILD 5s && sleep 2 && echo DONE CHILD"}},
    {"name": "Leaf", "kind": "cmd", "payload": {"cmd": "echo LEAF && sleep 1 && echo DONE LEAF"}},
    # cron-based quick jobs
    {"name": "Every5s", "kind": "cmd", "payload": {"cmd": "echo CRON_5S && date"}, "cron_spec": "@every 5s"},
    {"name": "Every10s", "kind": "cmd", "payload": {"cmd": "echo CRON_10S && date"}, "cron_spec": "@every 10s"},
]

for d in definitions:
    body = {
        "namespace": "demo",
        "name": d["name"],
        "version": 1,
        "kind": d["kind"],
        "payloadTemplate": d["payload"],
    }
    # include cron_spec if defined
    if "cron_spec" in d:
        body["cronSpec"] = d["cron_spec"]
    res = post("/definitions", body)
    defs[d["name"]] = res

# --------------------------------------------------
# 3. Add jobs to DAG (for non-cron ones)
# --------------------------------------------------
job_ids = {}
for name in ["Root", "Child5s", "Leaf"]:
    def_id = defs.get(name, {}).get("def_id")
    body = {
        "defId": def_id,
        "priority": 1,
        "payload": {"cmd": f"echo running {name}"},
    }
    res = post(f"/dags/{dag_id}/jobs", body)
    job_ids[name] = res.get("id")

# --------------------------------------------------
# 4. Add dependencies (Root -> Child5s -> Leaf)
# --------------------------------------------------
post(f"/dags/{dag_id}/dependencies", {"parentJobId": job_ids["Root"], "childJobId": job_ids["Child5s"]})
post(f"/dags/{dag_id}/dependencies", {"parentJobId": job_ids["Child5s"], "childJobId": job_ids["Leaf"]})

# --------------------------------------------------
# 5. Simulate job completions to trigger children
# --------------------------------------------------
print("Simulating DAG execution chain...")
time.sleep(1)
print("Marking Root complete")
post(f"/jobs/{job_ids['Root']}/complete", {})

time.sleep(2)
print("Marking Child5s complete")
post(f"/jobs/{job_ids['Child5s']}/complete", {})

# --------------------------------------------------
# 6. Show job status
# --------------------------------------------------
for name, jid in job_ids.items():
    print(f"Querying {name} (job_id={jid})")
    try:
        get(f"/jobs/{jid}")
    except requests.exceptions.RequestException:
        print(f"Job {jid} endpoint not implemented yet.")
    time.sleep(0.5)

# --------------------------------------------------
# 7. Observe cron jobs firing
# --------------------------------------------------
print("\nWatching cron jobs fire for ~15 seconds...")
for i in range(3):
    time.sleep(5)
    print(f"\n--- Tick {i+1} ---")
    for cname in ["Every5s", "Every10s"]:
        try:
            get(f"/definitions/{defs[cname]['def_id']}")
        except Exception:
            pass

print("Demo complete.")
