# chronosched

Chronosched is a lightweight, namespaced, DAG-driven job scheduler with:

- Immutable job definitions (versioned)
- Namespace-scoped DAGs
- Dependency graphs with cycle protection
- Frontier-driven job activation
- Scheduler supporting cron_spec on definitions
- Workers executing jobs independently
- PostgreSQL as the source of truth
- Complete REST API for orchestration, inspection, and testing

This README is fully updated to reflect the current (2025) codebase.

---

## Table of Contents
1. Architecture Overview
2. System Components
3. DAG Model
4. Dependency Frontier
5. Scheduler (cron_spec)
6. Worker Execution Model
7. Database Schema Summary
8. API Endpoints
9. PlantUML Diagrams
10. Running Chronosched
11. Python Demo Client
12. License

---

## Architecture Overview

Chronosched consists of:

- **API Server (Go)** — creates and manages namespaces, DAGs, definitions, jobs, and dependencies.
- **Workers** — dequeue and run jobs, update job status, and unlock frontier nodes.
- **Scheduler** — automatically enqueues jobs based on cron_spec in job definitions.
- **PostgreSQL** — authoritative state storage.

The architecture is simple, transparent, and fully deterministic.

---

## PlantUML Architecture Diagram

```plantuml
@startuml
skinparam backgroundColor #ffffff

rectangle "API Server" {
  [Handlers]
  [Router]
  [Scheduler]
}

rectangle "PostgreSQL" {
  [namespaces]
  [dags]
  [job_definitions]
  [jobs]
  [job_dependencies]
  [frontier]
  [jobs_history]
}

rectangle "Worker" {
  [Dequeuer]
  [Executor]
}

API Server --> PostgreSQL : SQL (pgx)
Worker --> PostgreSQL : poll/update
Scheduler --> API Server : Tick(def_id)
@enduml
```

---

## DAG Model

A DAG belongs to a **namespace** and contains:

- Jobs instantiated from job_definitions
- Directed parent→child dependencies
- A frontier of runnable jobs

DAGs are immutable. Create a new DAG to change its topology.

---

## Dependency Frontier

Frontier tracks which jobs can run:

- Jobs with no dependencies enter the frontier initially.
- Completing a job may unlock downstream nodes.
- Workers dequeue only frontier jobs.

This ensures correctness without global recalculation.

---

## Scheduler (cron_spec)

If a job definition includes `cron_spec: */5 * * * * *`, Chronosched:

1. Registers it on startup
2. Fires Tick(def_id) on schedule
3. Inserts jobs with dag_id = null
4. Workers run them like DAG jobs

Scheduler jobs inherit the definition’s namespace.

---

## Worker Execution Model

Workers:

1. Dequeue frontier jobs (or scheduler jobs)
2. Lock rows
3. Run their associated action
4. Update status (succeeded/failed)
5. Unlock dependent nodes

Workers are stateless and horizontally scalable.

---

## API Endpoints

All API paths are prefixed with:

```
/api/v1
```

### Namespaces

- POST /namespaces
- GET /namespaces
- GET /namespaces/{namespace_id}

### DAGs

- POST /dags/{namespace_id}
- GET /dags/{namespace_id}
- GET /dags/{namespace_id}/{dag_id}
- DELETE /dags/{namespace_id}/{dag_id}

### Job Definitions

- POST /definitions/{namespace_id}
- GET /definitions/{namespace_id}
- GET /definitions/{namespace_id}/{def_id}

### Jobs

#### DAG Jobs
- POST /dags/{namespace_id}/{dag_id}/jobs
- GET /dags/{namespace_id}/{dag_id}/jobs

#### Jobs by ID
- GET /jobs/{namespace_id}/{job_id}

#### All jobs in namespace
- GET /jobs/{namespace_id}

#### Job state
- POST /jobs/{job_id}/complete
- POST /jobs/{job_id}/fail

### Dependencies

- POST /dags/{namespace_id}/{dag_id}/dependencies/bulk
- POST /dags/{namespace_id}/{dag_id}/dependencies
- GET /dags/{namespace_id}/{dag_id}/dependencies

### Admin

- POST /admin/prune

### Health

- GET /healthz

---

## Running Chronosched

### Via Docker Compose

```bash
docker compose up --build
```

API will be available at:

```
http://localhost:8080/api/v1
```

### Running Server Locally

```bash
go run ./cmd/server
```

### Running Worker Locally

```bash
go run ./cmd/worker
```

---

## Python Demo Client

Demonstrates:
- Namespace creation
- DAG creation
- Job definition creation
- Job insertion
- Dependency wiring
- Scheduler testing

Run inside Docker:
```bash
docker compose run client
```

Or locally:
```bash
python3 demo_client.py
```

---

## License

All Rights Reserved. No permission is granted to use, copy, modify, or distribute this software without explicit written consent from the author (Edward Kuperman).
