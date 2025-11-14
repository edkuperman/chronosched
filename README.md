# Chronosched

Chronosched is a lightweight **distributed job scheduler** with **DAG-aware execution** and **PostgreSQL persistence**.  
It supports both **dependency-driven workflows** and **time-based (cron) schedules**.

---

## Overview

Chronosched models all work as **jobs** and **job definitions**:

| Concept | Description |
|----------|--------------|
| **Job Definition** | Immutable template describing *what* to run — including kind (`cmd`, `http`, `binary`), payload, and scheduling metadata (`cron_spec`, `delay_interval`). |
| **Job** | An executable instance of a definition — typically inserted into a DAG (directed acyclic graph). |
| **DAG** | A workflow graph connecting jobs by dependency (parent → child). DAG edges enforce that a child runs only after all its parents succeed. |
| **Frontier** | Tracks readiness of jobs in each DAG based on their in-degree (unmet dependencies). |
| **Scheduler** | Periodically registers all definitions with `cron_spec` and enqueues new job runs automatically. |
| **Worker** | Dequeues ready jobs, executes them, renews leases, and marks them `succeeded` or `failed`. |


---

## Entity Versioning & Pruning

All core entities in Chronosched are versioned and logically deleted rather than hard-deleted:

- **Job Definitions**
  - Immutable by design. Any "update" creates a new `(namespace, name, version)` row.
  - The old version is marked `deleted = TRUE` but may still be referenced by existing jobs/DAGs.
- **DAGs**
  - Each DAG row has a `version` and `deleted` flag.
  - You can safely introduce a new DAG version while old jobs still reference the previous version.
- **Jobs**
  - Jobs carry a `version` and a `deleted` flag.
  - Deleting a job via the API marks it `status = 'cancelled'` and `deleted = TRUE` while preserving history until prune time.
- **Dependencies**
  - Edges between jobs live in `job_dependencies` and include a `dependency_type` field:
    - `order-only` — the child only depends on the **ordering** of the parent (must run after).
    - `data` (default) — the child both depends on the ordering **and** is assumed to consume the parent’s result.

A dedicated admin endpoint moves no-longer-referenced rows into history tables:

- `POST /api/v1/admin/prune`
  - Moves prunable rows from:
    - `job_definitions` → `job_definitions_history`
    - `dags` → `dags_history`
    - `jobs` → `jobs_history`
    - `job_dependencies` → `job_dependencies_history`
  - Prunable jobs are those that:
    - are in a terminal status (`succeeded`, `failed`, `cancelled`) **or** explicitly marked `deleted = TRUE`, **and**
    - have a non-NULL `finished_at`.

This keeps the active working set small while preserving a full audit trail in the history tables.
---

## Scheduling Options

Chronosched supports three scheduling modes:

| Mode | Field(s) Used | Trigger | Example |
|------|----------------|----------|----------|
| **Time-driven** | `cron_spec` | Fires periodically (like a CRON daemon). | `'@every 5s'`, `'0 * * * *'` |
| **Dependency-driven** | DAG edges (`parent → child`) | Runs after all parent jobs succeed. | `JobB` runs after `JobA` completes. |
| **Delayed dependency** | `delay_interval` | Runs after parents succeed, offset by a delay. | Run 5 min after parent success. |

If a definition has:
- a `cron_spec` → it’s **registered** by the scheduler loop and runs periodically.
- no `cron_spec` → it’s **triggered** only when its parents succeed.
- a `delay_interval` → its execution is deferred by that interval once ready.

---

## Directory Layout

```
chronosched/
├── cmd/
│   ├── server/     # API service
│   ├── worker/     # Worker process
│   └── demo/       # Example / quick-run entrypoints
├── internal/
│   ├── api/        # HTTP handlers (chi router)
│   ├── db/         # JobRepo, SchedulerRepo
│   ├── scheduler/  # CRON scheduler registration
│   └── worker/     # Runner, executors
├── migrate/initdb/
│   ├── 000_create_db_if_not_exists.sql
│   ├── 001_switch_to_chronosched_db.sql
│   ├── 002_schema.sql
│   ├── 003_indexes.sql
│   ├── 004_tuning.sql # tuning notes
│   └── 005_seed_demo.sql # demo seed
├── demo_client.py   # interactive demo script
└── README.md
```

---

## Running the Demo

### Start services

```bash
docker compose up --build
```

This launches:
- PostgreSQL 18 with schema initialization.
- The Chronosched API server at `http://localhost:8080`.
- (Optionally) a worker service polling for jobs.

---

### Run the demo client

```bash
python demo_client.py
```

The script will:
1. Create a new DAG.
2. Define several jobs — both dependency-based and cron-based.
3. Add dependencies (`Root → Child5s → Leaf`).
4. Simulate parent completions to trigger children.
5. Observe cron jobs firing every few seconds.

The whole demo finishes in ~25 seconds.

---

## Example Output (trimmed)

```
Creating DAG 9a7f...  -> 201
POST /definitions (Root, Child5s, Leaf, Every5s, Every10s)
...
Marking Root complete
Marking Child5s complete
Querying job Leaf -> ready
Watching cron jobs fire for ~15 seconds...
CRON_5S at 10:00:05
CRON_10S at 10:00:10
```

---

## Key Components

| Component | Description |
|------------|--------------|
| **JobRepo.DequeueReady()** | Atomic job leasing (`FOR UPDATE SKIP LOCKED`) for concurrency-safe workers. |
| **Trigger `on_job_succeeded_make_children_ready()`** | Automatically sets `due_at = now() + delay_interval` for children. |
| **Runner** | Configurable worker (poll rate, concurrency, lease renewal). |
| **Scheduler** | Registers CRON jobs (`cron_spec`) and enqueues runs. |
| **Autovacuum tuning** | Keeps the `jobs` table healthy under high update volume. |

---

## API Endpoints

| Path                                          | Description                                                              
| --------------------------------------------- | ------------------------------------------------------------------------ | 
| `POST /api/v1/dags`                           | Create a new DAG                                                         |
| `GET /api/v1/dags`                            | List all DAGs                                                            |
| `GET /api/v1/dags/{dagId}`                    | Retrieve DAG details                                                     |
| `DELETE /api/v1/dags/{dagId}`                 | Delete a DAG (preserving shared jobs)                                    |
| `POST /api/v1/definitions/bulk`               | Bulk create or upsert job definitions                                    |
| `POST /api/v1/definitions`                    | Create a single job definition (supports `cronSpec` and `delayInterval`) |
| `GET /api/v1/definitions/{defId}`             | Fetch definition info                                                    |
| `GET /api/v1/definitions`                     | List all job definitions (optionally by namespace)                       |
| `POST /api/v1/dags/{dagId}/jobs`              | Add a job instance to a DAG                                              |
| `GET /api/v1/dags/{dagId}/jobs`               | List all jobs in a DAG                                                   |
| `GET /api/v1/jobs/{jobId}`                    | Retrieve job details                                                     |
| `POST /api/v1/dags/{dagId}/dependencies/bulk` | Bulk insert dependencies into a DAG                                      |
| `POST /api/v1/dags/{dagId}/dependencies`      | Add a single parent→child dependency                                     |
| `GET /api/v1/dags/{dagId}/dependencies`       | List dependencies for a DAG                                              |
| `POST /api/v1/jobs/{jobId}/complete`          | Mark job as **succeeded**                                                |
| `POST /api/v1/jobs/{jobId}/fail`              | Mark job as **failed**                                                   |
| `POST /api/v1/jobs/{jobId}/retry`             | Reset a failed job for re-execution (if implemented)                     |
| `GET /api/v1/scheduler/reload`                | Force scheduler to reload all cron definitions                           |
| `GET /api/v1/healthz`                         | Health check endpoint                                                    |

---

## Example: Delayed Dependency Job

Chronosched also supports *delayed dependencies* — jobs that run after their parent(s) succeed, but only after a specified delay interval.

Here’s how you’d define and connect one:

```bash
# 1️ Create a new namespace (if not already present)
curl -X POST http://localhost:8080/api/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{
    "name": "demo",
    "description": "Demo namespace"
  }'

# Expected response:
# {
#   "id": "NAMESPACE_ID_HERE",
#   "name": "demo"
# }

# 2️ Retrieve the namespace ID
# (If you didn’t store it from creation, you can look it up:)
curl -X GET http://localhost:8080/api/v1/namespaces | jq .

# Save the "id" field from the "demo" entry as NAMESPACE_ID.

# 3️ Create a parent job definition
curl -X POST http://localhost:8080/api/v1/namespaces/NAMESPACE_ID/definitions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ParentJob",
    "version": 1,
    "kind": "cmd",
    "payloadTemplate": {"cmd": "echo Parent && sleep 1 && echo Done Parent"}
  }'

# 4️ Create a child job definition with delay_interval
curl -X POST http://localhost:8080/api/v1/namespaces/NAMESPACE_ID/definitions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "DelayedChild",
    "version": 1,
    "kind": "cmd",
    "payloadTemplate": {"cmd": "echo Delayed Child && date"},
    "delayInterval": "5 minutes"
  }'

# 5️ Create a DAG under the same namespace
curl -X POST http://localhost:8080/api/v1/namespaces/NAMESPACE_ID/dags \
  -H "Content-Type: application/json" \
  -d '{
    "name": "delayed_run"
  }'

# Expected response: { "id": "DAG_ID_HERE", "namespaceId": "NAMESPACE_ID_HERE" }

# 6️ Create job instances inside the DAG
curl -X POST http://localhost:8080/api/v1/namespaces/NAMESPACE_ID/dags/DAG_ID_HERE/jobs \
  -H "Content-Type: application/json" \
  -d '{"defId": "DEF_ID_PARENT"}'

curl -X POST http://localhost:8080/api/v1/namespaces/NAMESPACE_ID/dags/DAG_ID_HERE/jobs \
  -H "Content-Type: application/json" \
  -d '{"defId": "DEF_ID_CHILD"}'

# 7️ Link the dependency (Parent → DelayedChild)
curl -X POST http://localhost:8080/api/v1/namespaces/NAMESPACE_ID/dags/DAG_ID_HERE/dependencies \
  -H "Content-Type: application/json" \
  -d '{
    "parentJobId": JOB_ID_PARENT,
    "childJobId": JOB_ID_CHILD
  }'

# 8️ Complete the parent job
curl -X POST http://localhost:8080/api/v1/namespaces/NAMESPACE_ID/jobs/JOB_ID_PARENT/complete

# Result:
# The child job will transition to 'queued' with due_at = now() + interval '5 minutes'
# and will be picked up by workers automatically after the delay expires.

```

The trigger automatically applies:
```sql
due_at = now() + delay_interval
```
once the parent succeeds — no extra logic required.

---

## License

MIT © 2025 — Edward Kuperman  
Open-source distributed scheduler with DAG orchestration.
