# Chronosched

Chronosched is a PostgreSQL-backed DAG scheduler written in Go.

Its core idea is to treat **job definitions**, **schedules**, and **DAG orchestration** as separate first-class concepts.

Chronosched separates three concerns into independent concepts:

- **job definitions** describe reusable work
- **schedules** belong to job definitions
- **DAG versions** define orchestration and dependencies

At runtime, the scheduler creates **runs**, and each run materializes one or more **jobs** that the worker executes.

## Quick start (local demo)

Run the full stack locally:

```bash
docker compose up --build -d
```

Check the API server:

```bash
curl http://localhost:8080/healthz
```

Expected:

```json
{"status":"ok"}
```

Open the API UI:

http://localhost:8080/

## Design goals

Chronosched focuses on a few core design principles:

- **Separation of concerns** between job definitions, schedules, and orchestration
- **Reusable job definitions** across multiple workflows
- **Immutable DAG versions** so workflows can evolve safely
- **Deterministic execution semantics** for reliable orchestration
- **Durable state management** backed by PostgreSQL
- **API-first integration** with external workers and services
- **Explicit orchestration model** separating task definitions from workflow structure
- **AI-assisted failure analysis** to explain workflow failures and recovery steps

## Why this design

A single job definition can be reused across multiple workflows.

For example, `sales-stats` might:

- run by itself on a weekly cadence
- appear after `etl-load-sales` in one DAG
- appear after `load -> validate` in another DAG

The schedule stays attached to the definition, while dependency ordering stays attached to the DAG version.

That split makes reuse easier and allows workflows to evolve through new DAG versions without changing the underlying task definitions.

## When to use Chronosched

Chronosched is designed for systems that require deterministic
workflow orchestration with reusable job definitions.

Typical scenarios include:

- **data pipelines** where the same tasks appear in multiple workflows
- **batch processing systems** whose orchestration evolves over time
- **internal platform services** that expose scheduling via an API
- **automation systems** that need durable execution state
- **multi-tenant environments** where workflows must be isolated by namespace

Because schedules, job definitions, and orchestration graphs are
independent concepts, Chronosched works well in environments where
workflows may change while the underlying tasks remain reusable.

## System architecture

```mermaid
flowchart LR
    Client[Client / CLI / UI] -->|HTTP /api/v1| Server[API Server
cmd/server]
    Scheduler[Scheduler
cmd/scheduler] -->|reads schedules
creates runs| DB[(PostgreSQL)]
    Server -->|CRUD + run APIs| DB
    Worker[Worker
cmd/worker] -->|lease jobs| Server
    Worker -->|dispatch accepted / failed| Server
    Worker -->|executes REST callbacks| Callback[External service / demo callback service]
    Callback -->|POST job events| Server
```

### Components

- **API server** exposes the REST API, worker gateway, and Swagger/OpenAPI UI.
- **Scheduler** scans for due schedules and creates runs.
- **Worker** leases queued jobs, dispatches them, and tracks dispatch outcomes.
- **PostgreSQL** stores namespaces, definitions, DAGs, DAG versions, runs, jobs, and queue state.

## Core model

### Namespaces

Namespaces are the top-level isolation boundary.

Typical examples:

- `finance`
- `analytics`
- `billing`

### Job definitions

A job definition describes reusable work.

Examples:

- `etl-load-sales`
- `sales-stats`
- `email-report`
- `cleanup-temp-files`

A definition includes:

- logical identity and metadata
- execution kind
- payload template
- optional schedule
- enabled / paused state

### Schedules

Schedules live on **job definitions**, not on DAGs.

#### Interval example

```json
{
  "schedule": {
    "type": "interval",
    "interval_seconds": 10,
    "start_at": "2026-03-06T12:04:57Z"
  }
}
```

#### Cron example

```json
{
  "schedule": {
    "type": "cron",
    "cron": "0 23 * * 0",
    "timezone": "America/New_York"
  }
}
```

### DAGs and DAG versions

A **DAG** is the stable workflow identity.

A **DAG version** is an immutable snapshot of:

- nodes
- edges
- bindings from nodes to job definitions

Only one version is active for a DAG at a time.

### Runs and jobs

A **run** is a concrete execution of a DAG version.

- **manual runs** materialize the whole DAG
- **scheduled runs** start from the scheduled node and allow downstream work to proceed when dependencies are satisfied

A run contains runtime **jobs**.

## Definitions vs DAG versions vs runs

```mermaid
flowchart TD
    subgraph Definitions[Reusable job definitions]
        D1[etl-load-sales]
        D2[sales-stats
cron / interval optional]
        D3[email-report]
    end

    subgraph DAG[DAG: weekly-sales]
        V1[DAG version 1]
        V2[DAG version 2]
    end

    D1 --> V1
    D2 --> V1
    D3 --> V1
    D1 --> V2
    D2 --> V2
    D3 --> V2

    subgraph Version1Graph[Version 1 graph]
        V1A[etl]
        V1B[stats]
        V1C[email]
        V1A --> V1B --> V1C
    end

    subgraph Version2Graph[Version 2 graph]
        V2A[etl]
        V2B[validate]
        V2C[stats]
        V2D[email]
        V2A --> V2B --> V2C --> V2D
    end

    V1 --> Run1[Run 101]
    V1 --> Run2[Run 102]
    V2 --> Run3[Run 201]
```

The same definitions can be reused while the orchestration evolves through new DAG versions.

## Design tradeoffs

Chronosched intentionally makes several design tradeoffs:

- PostgreSQL is used as the single source of truth, prioritizing consistency and simplicity over horizontal scalability
- The execution model is callback-based (REST), favoring integration simplicity over tightly coupled workers
- DAG versions are immutable, prioritizing safety and reproducibility over in-place mutation
- Scheduling and orchestration are separated, improving reuse at the cost of additional conceptual complexity

## Job execution lifecycle

```mermaid
flowchart LR
    Due[Definition schedule becomes due] --> Scheduler[Scheduler creates run]
    Scheduler --> Waiting[Jobs inserted as waiting or queued]
    Waiting --> Ready[Dependencies satisfied]
    Ready --> Lease[Worker leases queued job]
    Lease --> Dispatch[Worker dispatches execution]
    Dispatch --> Started[Callback posts started]
    Started --> Heartbeat[Optional heartbeat]
    Heartbeat --> Done[Callback posts succeeded or failed]
    Done --> Refresh[Run status refreshed]
```

Common job states include:

- `waiting`
- `queued`
- `dispatching`
- `dispatched`
- `running`
- `succeeded`
- `failed`
- `lost`
- `missed`
- `blocked`
- `cancelled`
- `skipped`

## Project layout

```text
chronosched/
├── cmd/
│   ├── scheduler/
│   ├── server/
│   └── worker/
├── client/
│   └── python/
├── internal/
│   ├── api/
│   ├── dag/
│   ├── dal/
│   │   └── sql/
│   ├── logger/
│   ├── repository/
│   ├── scheduler/
│   ├── scripts/
│   └── worker/
├── migrate/
│   └── initdb/
├── openapi/
│   └── chronosched.yaml
├── docker-compose.yml
└── docker-compose.debug.yml
```

## API overview

All public endpoints are rooted under **`/api/v1`**.

### Namespaces

- `GET /api/v1/namespaces`
- `POST /api/v1/namespaces`
- `GET /api/v1/namespaces/{name}`

### Job definitions

- `GET /api/v1/namespaces/{namespace_id}/job-definitions`
- `POST /api/v1/job-definitions`
- `GET /api/v1/job-definitions/{definition_id}`
- `PUT /api/v1/job-definitions/{definition_id}`
- `DELETE /api/v1/job-definitions/{definition_id}`
- `POST /api/v1/job-definitions/{definition_id}/enable`
- `POST /api/v1/job-definitions/{definition_id}/disable`
- `POST /api/v1/job-definitions/{definition_id}/pause`
- `POST /api/v1/job-definitions/{definition_id}/resume`
- `GET /api/v1/job-definitions/{definition_id}/usages`

### DAGs and versions

- `GET /api/v1/namespaces/{namespace_id}/dags`
- `POST /api/v1/namespaces/{namespace_id}/dags`
- `GET /api/v1/dags/{dag_id}`
- `DELETE /api/v1/dags/{dag_id}`
- `GET /api/v1/dags/{dag_id}/versions`
- `POST /api/v1/dags/{dag_id}/versions`
- `GET /api/v1/dag-versions/{dag_version_id}`
- `GET /api/v1/dag-versions/{dag_version_id}/graph`
- `POST /api/v1/dag-versions/{dag_version_id}/activate`
- `POST /api/v1/dag-versions/{dag_version_id}/revert`

### Runs and jobs

- `POST /api/v1/dags/{dag_id}/runs`
- `GET /api/v1/dags/{dag_id}/runs`
- `GET /api/v1/runs/{run_id}`
- `GET /api/v1/runs/{run_id}/jobs`
- `GET /api/v1/runs/{run_id}/graph`
- `GET /api/v1/runs/{run_id}/summary`
- `GET /api/v1/jobs/{job_id}/readiness`
- `POST /api/v1/jobs/{job_id}/events`
- `GET /api/v1/namespaces/{namespace_id}/jobs/problems`

### Internal worker gateway

- `POST /internal/workers/lease`
- `POST /internal/workers/dispatch-result`

## Running the project

### Start everything

```bash
docker compose up --build -d
```

This starts:

- PostgreSQL
- API server on `http://localhost:8080`
- scheduler
- worker
- Python callback demo service on `http://localhost:8090`

### Check health

```bash
curl http://localhost:8080/healthz
```

Expected response:

```json
{"status":"ok"}
```

### Open the API UI

Open:

```text
http://localhost:8080/
```

The OpenAPI document is served from:

```text
http://localhost:8080/openapi/chronosched.yaml
```

## Callback demo service

The Python demo service under `client/python` exposes callback endpoints used by the worker.

The demo defines three jobs:

- **hello-5s** — a REST callback job scheduled every 5 seconds
- **hello-10s** — a REST callback job scheduled every 10 seconds
- **reportJob** — a REST callback job representing a downstream summary/report step

Their dependencies are:

- **hello-10s** depends on **hello-5s**
- **reportJob** depends on **hello-5s**
- **reportJob** depends on **hello-10s**

This creates a small DAG containing two common patterns:

- **Sequential dependency:** `hello-5s → hello-10s`
- **Fan-in dependency:** `hello-5s` and `hello-10s` both feed into `reportJob`

In other words, `reportJob` sits downstream of both upstream jobs and requires multiple predecessors to complete before it can run successfully.

```mermaid
graph TD
    H5[hello-5s<br/>REST callback<br/>5-second cadence]
    H10[hello-10s<br/>REST callback<br/>10-second cadence]
    R[reportJob<br/>REST callback<br/>downstream report step]

    H5 -->|depends on upstream 5s success| H10
    H5 -->|parent| R
    H10 -->|parent| R
```

Typical log output from the Python service looks like this:

```text
python-service-1  | INFO:     Started server process [1]
python-service-1  | INFO:     Waiting for application startup.
python-service-1  | INFO:     Application startup complete.
python-service-1  | INFO:     Uvicorn running on http://0.0.0.0:8090 (Press CTRL+C to quit)
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-5s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-5s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-10s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/reportJob HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-5s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-5s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-10s HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/reportJob HTTP/1.1" 200 OK
python-service-1  | INFO:     <IP:PORT> - "POST /jobs/hello-5s HTTP/1.1" 200 OK
python-service-1  | INFO:     Shutting down
python-service-1  | INFO:     Waiting for application shutdown.
python-service-1  | INFO:     Application shutdown complete.
python-service-1  | INFO:     Finished server process [1]
```

## Minimal end-to-end example

The example below recreates the **same fan-out / fan-in DAG used by the Python demo service**, but runs it manually through the API so the full dependency behavior can be observed immediately.

The workflow contains three nodes:

- **hello5** → executes the `hello-5s` job definition
- **hello10** → executes the `hello-10s` job definition
- **reportJob** → executes the `reportJob` job definition

with the following dependencies:

- `hello5 → hello10`
- `hello5 → reportJob`
- `hello10 → reportJob`

This creates a **fan-out / fan-in DAG** where `hello5` fans out to two downstream nodes, and `reportJob` fans in from both upstream jobs.

When triggered as a **manual run**, Chronosched materializes the **entire DAG** for the run.  
All nodes are created as jobs immediately, and execution proceeds according to dependency relationships.

> These commands are bash-oriented for Linux, macOS, or WSL. On native Windows `curl`, JSON quoting differs.

### 1. Create a namespace

```bash
BASE=http://localhost:8080

NS=$(curl -s -X POST "$BASE/api/v1/namespaces" \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo"}' | jq -r '.id')

echo "$NS"
```

### 2. Create job definitions

```bash
HELLO5=$(curl -s -X POST "$BASE/api/v1/job-definitions" \
  -H 'Content-Type: application/json' \
  -d '{
    "namespace_id":"'"$NS"'",
    "name":"hello-5s",
    "description":"callback demo",
    "kind":"rest",
    "payload_template":{"url":"http://python-service:8090/jobs/hello-5s"},
    "is_enabled":true
  }' | jq -r '.id')

HELLO10=$(curl -s -X POST "$BASE/api/v1/job-definitions" \
  -H 'Content-Type: application/json' \
  -d '{
    "namespace_id":"'"$NS"'",
    "name":"hello-10s",
    "description":"callback demo",
    "kind":"rest",
    "payload_template":{"url":"http://python-service:8090/jobs/hello-10s"},
    "is_enabled":true
  }' | jq -r '.id')

REPORT=$(curl -s -X POST "$BASE/api/v1/job-definitions" \
  -H 'Content-Type: application/json' \
  -d '{
    "namespace_id":"'"$NS"'",
    "name":"reportJob",
    "description":"callback demo",
    "kind":"rest",
    "payload_template":{"url":"http://python-service:8090/jobs/reportJob"},
    "is_enabled":true
  }' | jq -r '.id')
```

### 3. Create a DAG

```bash
DAG=$(curl -s -X POST "$BASE/api/v1/namespaces/$NS/dags" \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo-dag","description":"callback workflow"}' | jq -r '.id')

echo "$DAG"
```

### 4. Publish a DAG version

```bash
VER=$(curl -s -X POST "$BASE/api/v1/dags/$DAG/versions" \
  -H 'Content-Type: application/json' \
  -d '{
    "version_note":"initial version",
    "nodes":[
      {"node_key":"hello5","display_name":"Hello 5s","job_definition_id":"'"$HELLO5"'"},
      {"node_key":"hello10","display_name":"Hello 10s","job_definition_id":"'"$HELLO10"'"},
      {"node_key":"reportJob","display_name":"Report Job","job_definition_id":"'"$REPORT"'"}
    ],
    "edges":[
      {"from":"hello5","to":"hello10"},
      {"from":"hello5","to":"reportJob"},
      {"from":"hello10","to":"reportJob"}
    ]
  }' | jq -r '.id')

echo "$VER"
```

### 5. Activate the DAG version

```bash
curl -i -X POST "$BASE/api/v1/dag-versions/$VER/activate"
```

### 6. Trigger a manual run

```bash
RUN=$(curl -s -X POST "$BASE/api/v1/dags/$DAG/runs" \
  -H 'Content-Type: application/json' \
  -d '{}' | jq -r '.id')

echo "$RUN"
```

### 7. Inspect runtime state

```bash
curl -s "$BASE/api/v1/runs/$RUN" | jq
curl -s "$BASE/api/v1/runs/$RUN/jobs" | jq
curl -s "$BASE/api/v1/runs/$RUN/graph" | jq
```

## Python client

The repository includes a small Python client in `client/python/chronosched_client.py`.

It covers the main operations for:

- namespaces
- job definitions
- DAGs
- DAG versions
- runs
- job events

## Debug mode

For source-mounted debug containers:

```bash
docker compose -f docker-compose.yml -f docker-compose.debug.yml up --build
```

Helper scripts are also available under `internal/scripts/`.

## Restart and recovery

Jobs can be restarted within a run to recover from failures without creating a new run.

Endpoint:

POST /api/v1/namespaces/{namespace_id}/jobs/{job_id}/restart

Request:

{
  "cascade": true,
  "mode": "run_now"
}

Parameters:

- cascade (default true)
- mode: run_now | next_schedule

Behavior:

- Resets the selected job
- Optionally cascades to downstream dependent jobs
- Recomputes dependency readiness
- Scheduler resumes execution automatically

### Resume modes

- run_now — job becomes immediately eligible
- next_schedule — job resumes at next scheduled time

This is useful when:

- a transient failure is corrected outside the scheduler
- a downstream blocked job should be retried after its dependency succeeds
- an operator wants to recover an in-flight workflow instead of rerunning the whole DAG

Exact restart behavior depends on the current job state and dependency readiness.

## Events

Chronosched uses explicit job events to move runtime state forward.

Typical callback-driven events include:

- `started`
- `heartbeat`
- `succeeded`
- `failed`

The worker dispatches work to an external callback target, and the callback service posts events back to Chronosched with `POST /api/v1/jobs/{job_id}/events`.

This makes the execution model visible, durable, and easy to integrate with external services.

## Problem jobs API

GET /api/v1/namespaces/{namespace_id}/jobs/problems

Optional filters:
- dag_id
- status

### Default problem states

The namespace problems view and recovery flows commonly surface these problem states:

| State     | Meaning | Typical cause | Retry guidance |
|-----------|---------|---------------|----------------|
| `failed`  | The job ran and reported failure. | Application error, bad payload, exception, remote callback failure. | Inspect the failure reason and logs, correct the issue, then restart or rerun. |
| `lost`    | The job started or was dispatched, but completion was never recorded. | Worker crash, callback never returned, heartbeat timeout, infrastructure interruption. | Investigate worker and callback logs first; retry only after confirming the work is safe to re-run. |
| `missed`  | A scheduled execution window was not materialized or was not picked up in time. | Scheduler downtime, backlog, clock issues, or intentionally minimal recovery semantics. | Review scheduler health and timing guarantees before replaying work. |
| `blocked` | The job could not run because an upstream dependency failed or never became ready. | A parent node failed, was lost, or otherwise prevented readiness. | Fix the upstream problem first; then restart the blocked job or rerun the affected portion of the workflow. |

## AI-assisted summaries

Chronosched can generate an AI-assisted summary for a run with:

```bash
GET /api/v1/runs/{run_id}/summary
```

The endpoint builds a compact run context from the run graph, failed node status, upstream successes, downstream impacted nodes, and recorded reason fields.

When `OPENAI_API_KEY` is configured, the server sends that context to OpenAI and returns a structured response containing:

- `cause`
- `impact`
- `next_steps`
- `confidence`
- `retry`
- `source`
- `model`
- `generated_at`

Without `OPENAI_API_KEY`, the endpoint still returns a deterministic heuristic summary built from the same run context.

### Secure local key setup

Create a local `.env` file from `.env.example` and store your API key there.  
The `.env` file is ignored by Git and will not be committed.

```bash
cp .env.example .env
# edit .env and set OPENAI_API_KEY
```
Docker Compose automatically loads .env from the project root.

You can also specify an environment file explicitly:

```bash
docker compose --env-file .env up --build
```
For one-off runs, you can override via the command line:

```bash
OPENAI_API_KEY=your_key_here docker compose up --build
```
Avoid hard-coding API keys in source code, Dockerfiles, or checked-in compose files.
For production environments, use a proper secret manager instead of .env files.

## Callback demo service output

The demo callback service also logs AI summary output for the intentionally failing secondary workflow.

This section is placed near the end of the README on purpose: by this point, the scheduler model, dependency semantics, problem states, and AI summary endpoint have already been introduced, so the log output is easier to interpret.

The secondary workflow intentionally produces two related summaries during a failure cycle:

1. a summary for the **root failed node** (`hello_10s`)
2. a summary for the **downstream blocked node** (`reportJob`)

That is why the logs may show more than one AI summary for what looks like a single workflow failure.

A simplified example looks like this:

```text
python-service-1  | 2026-03-23 08:41:05,777 INFO chronosched-python-demo Failure summary for run 11 (secondary): {
python-service-1  |   'failed_node': 'reportJob',
python-service-1  |   'type': 'dependency',
python-service-1  |   'cause': 'reportJob was blocked by a failed upstream dependency: hello_10s failed with an intentional demo_failure error.',
python-service-1  |   'impact': 'The focus run reportJob did not execute. hello_5s succeeded, but the failure in hello_10s prevented reportJob from starting.',
python-service-1  |   'next_steps': [
python-service-1  |     'Inspect hello_10s logs and failure path for the intentional demo_failure.',
python-service-1  |     'Rerun reportJob only after hello_10s succeeds, or adjust the dependency if reportJob should not wait on hello_10s.'
python-service-1  |   ],
python-service-1  |   'confidence': 0.98,
python-service-1  |   'retry': 'requires_manual_review',
python-service-1  |   'source': 'openai'
python-service-1  | }
python-service-1  | 2026-03-23 08:41:07,592 INFO chronosched-python-demo Failure summary for run 9 (secondary): {
python-service-1  |   'failed_node': 'hello_10s',
python-service-1  |   'type': 'dependency',
python-service-1  |   'cause': 'The focus node hello_10s failed with demo_failure: Intentional demo failure in 10-second workflow.',
python-service-1  |   'impact': 'reportJob was blocked due to a failed dependency; hello_5s succeeded and did not appear impacted.',
python-service-1  |   'next_steps': [
python-service-1  |     'Inspect hello_10s logs and the demo_failure condition.',
python-service-1  |     'Once hello_10s succeeds, retry reportJob to clear the blocked dependency.'
python-service-1  |   ],
python-service-1  |   'confidence': 0.99,
python-service-1  |   'retry': 'requires_manual_review',
python-service-1  |   'source': 'openai'
python-service-1  | }
```

The full output is verbose because it includes the structured response plus the context used to build it.

If you want to inspect the full raw payloads during the demo, check the Python service logs directly. The most important fields to read first are usually:

- `failed_node`
- `cause`
- `impact`
- `next_steps`
- `retry`
- `source`

## Project scope

Chronosched is designed as a reference implementation of a database-backed DAG scheduler architecture.

To preserve a consistent and opinionated design, the repository is not currently accepting external contributions.

## License

See `LICENSE`.
