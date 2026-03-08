# Chronosched v2

Chronosched is an experimental Postgres-backed scheduler built around four core ideas:

- **work definitions are reusable**
- **scheduling belongs to the work definition**
- **dependencies belong to a versioned DAG**
- **runs materialize runtime jobs from an enabled DAG version**

## Model

### Job definitions
A job definition describes reusable work.

Examples:
- `etl-load-sales`
- `sales-stats`
- `email-stats`

A definition may include an optional schedule.

### Cron schedule

```json
{
  "schedule": {
    "type": "cron",
    "cron": "0 23 * * 0",
    "timezone": "America/New_York",
    "on_failure": "continue"
  }
}
```

### Interval schedule

```json
{
  "schedule": {
    "type": "interval",
    "interval_seconds": 10,
    "start_at": "2026-03-06T12:04:57Z",
    "on_failure": "continue"
  }
}
```

A schedule that includes `cron` but omits `type` is treated as a cron schedule.

### DAGs and DAG versions
A DAG is the stable workflow identity.
A DAG version is an immutable snapshot of:

- nodes
- edges
- definition bindings

Only **one version is enabled** for a DAG at a time.

### Runs
A run is a concrete execution created from a DAG version.

- **manual runs** materialize the whole DAG version
- **scheduled runs** materialize the scheduled node on its own due time
- downstream jobs remain in `waiting` until DAG dependencies are satisfied

That lets a scheduled definition such as `sales-stats` trigger different dependency chains in different DAGs while preserving each node's own cron or interval schedule.

## Why the split matters

The same definition can be reused in multiple DAGs.

Example:

- `sales-stats` has a weekly cron schedule
- in one DAG: `etl -> sales-stats`
- in another DAG: `sales-stats`
- in another DAG: `load -> validate -> sales-stats -> email`

The schedule belongs to the **definition**.
The orchestration belongs to the **DAG version**.

## Versioning and revert

Chronosched versions the DAG structure.

You can:

- create new versions
- list versions
- inspect the latest version number
- activate one version at a time
- revert by creating a **new copy** from a prior version

Revert is copy-based rather than reference-based so that each version remains a stable snapshot.

## API surface

All public endpoints are under `/api/v2`.

### Namespaces
- `GET /api/v2/namespaces`
- `POST /api/v2/namespaces`
- `GET /api/v2/namespaces/{name}`

### Job definitions
- `GET /api/v2/namespaces/{namespace_id}/job-definitions`
- `POST /api/v2/job-definitions`
- `GET /api/v2/job-definitions/{definition_id}`
- `PUT /api/v2/job-definitions/{definition_id}`
- `POST /api/v2/job-definitions/{definition_id}/enable`
- `POST /api/v2/job-definitions/{definition_id}/disable`
- `POST /api/v2/job-definitions/{definition_id}/pause`
- `POST /api/v2/job-definitions/{definition_id}/resume`
- `GET /api/v2/job-definitions/{definition_id}/usages`

### DAGs and versions
- `GET /api/v2/namespaces/{namespace_id}/dags`
- `POST /api/v2/namespaces/{namespace_id}/dags`
- `GET /api/v2/dags/{dag_id}`
- `GET /api/v2/dags/{dag_id}/versions`
- `POST /api/v2/dags/{dag_id}/versions`
- `GET /api/v2/dag-versions/{dag_version_id}`
- `GET /api/v2/dag-versions/{dag_version_id}/graph`
- `POST /api/v2/dag-versions/{dag_version_id}/activate`
- `POST /api/v2/dag-versions/{dag_version_id}/revert`

### Runs and runtime graph
- `POST /api/v2/dags/{dag_id}/runs`
- `GET /api/v2/dags/{dag_id}/runs`
- `GET /api/v2/runs/{run_id}`
- `GET /api/v2/runs/{run_id}/jobs`
- `GET /api/v2/runs/{run_id}/graph`
- `GET /api/v2/jobs/{job_id}/readiness`

### Internal worker gateway
- `POST /internal/workers/lease`
- `POST /internal/workers/result`

## Graph support

The API has first-class graph read models for a future UI.

### Authoring graph
`GET /api/v2/dag-versions/{dag_version_id}/graph`

Returns:
- nodes
- edges
- definition mapping
- schedule metadata on nodes

### Runtime graph
`GET /api/v2/runs/{run_id}/graph`

Returns:
- runtime jobs
- statuses
- runtime edges
- readiness info

This is intended to support:
- visual DAG inspection
- workflow editors
- runtime monitoring views

## Python client

The Python client mirrors the model:

- `create_job_definition(...)`
- `create_dag(...)`
- `publish_dag_version(...)`
- `activate_dag_version(...)`
- `revert_dag_version(...)`
- `get_dag_graph(...)`
- `trigger_run(...)`
- `get_run_graph(...)`
- `get_job_readiness(...)`

Files:

- `client/python/chronosched_client.py`
- `client/python/demo_client.py`

## Running

### Production

Linux/macOS/WSL:

```bash
./internal/scripts/run-prod.sh
```

Windows PowerShell:

```powershell
./internal/scripts/run-prod.ps1
```

Equivalent:

```bash
docker compose up --build
```

### Debug

Linux/macOS/WSL:

```bash
./internal/scripts/run-debug.sh
```

Windows PowerShell:

```powershell
./internal/scripts/run-debug.ps1
```

Equivalent:

```bash
docker compose -f docker-compose.yml -f docker-compose.debug.yml up --build
```

## Scheduling semantics

Chronosched now supports two scheduling modes on a job definition:

- **cron**: wall-clock aligned scheduling such as `*/10 * * * * *`
- **interval**: relative scheduling starting at a specific timestamp and repeating every `interval_seconds`

Examples:

- `{"type":"cron","cron":"*/10 * * * * *"}` runs on `:00, :10, :20, :30, :40, :50`
- `{"type":"interval","interval_seconds":10,"start_at":"2026-03-06T12:04:57Z"}` runs at `12:04:57`, `12:05:07`, `12:05:17`, and so on

For DAG edges between scheduled nodes, Chronosched:

- always materializes each scheduled node on **its own** cron or interval schedule
- keeps jobs in `waiting` until required scheduled parents have a matching successful run at or before the child run's `scheduled_at`

## Current limitations

This is still an experimental project.

Notable limitations:

- the worker currently supports REST callback execution and a small callback result contract; richer executors are still future work
- job definitions themselves are not versioned yet
- scheduled execution uses per-node scheduler state and currently focuses on forward scheduling rather than catch-up replay
- runtime retry policy and cancellation semantics are intentionally minimal

## License

MIT. See `LICENSE`.


## REST callback demo service

The Python service under `client/python` is now a FastAPI-based callback service.

It demonstrates two scheduled REST jobs:

- `hello_5s` runs every 5 seconds
- `hello_10s` runs every 10 seconds

The jobs are modeled as reusable work definitions with `kind = "rest"`.
Chronosched invokes the callback URL stored in the definition payload and expects a JSON response shaped like:

```json
{
  "success": true,
  "message": "optional detail"
}
```

or

```json
{
  "success": false,
  "error": "reason"
}
```

For this demo the 10-second job is independently scheduled, but it remains **waiting** until the required 5-second scheduled run for the same effective boundary succeeds in the same enabled DAG version.
If that upstream scheduled run fails, is cancelled, or is missed, the 10-second run remains blocked and its callback is not executed.

The included Python demo still uses cron schedules only. Interval scheduling is available through the API and data model even though the sample callback demo does not create an interval definition by default.

After two completed 10-second runs, the Python service disables the definitions, deletes the DAG, deletes the definitions, and shuts itself down.


## Runtime topology

Chronosched now runs as separate stateless services:

- `server`: REST API only
- `scheduler`: cron materialization / enqueue loop
- `worker`: stateless executor that leases work from any API server
- `db`: shared Postgres source of truth

The active DAG version is enforced through `dags.active_version_id`. Scheduled occurrences are materialized idempotently in the database using a unique occurrence key on `(trigger_type, trigger_node_id, scheduled_at)` for both cron and interval runs.

## Running the demo

```bash
docker compose up --build -d
```

Useful logs:

```bash
docker compose logs -f server
docker compose logs -f scheduler
docker compose logs -f worker
docker compose logs -f python-service
```
## Expected Python service output

The `python-service` logs should look roughly like:

```
python-service-1  | INFO:     Started server process [1]
python-service-1  | INFO:     Waiting for application startup.
python-service-1  | INFO:     Application startup complete.
python-service-1  | INFO:     Uvicorn running on http://0.0.0.0:8090 (Press CTRL+C to quit)
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-5s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-10s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-5s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-5s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-10s HTTP/1.1" 200 OK
python-service-1  | INFO:     Shutting down
python-service-1  | INFO:     Waiting for application shutdown.
python-service-1  | INFO:     Application shutdown complete.
```

The exact IP and port values depend on the container network.