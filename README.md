# 🕒 Chronosched — Distributed DAG-Aware Job Scheduler

Chronosched is a lightweight, distributed job scheduler designed around **directed acyclic graphs (DAGs)**.  
It manages job definitions, dependencies, and execution order with strong guarantees against cycles — enabling reliable orchestration of multi-step workflows.

---

## Features

- **DAG-based dependency management** — safely model and enforce job order.
- **Local + global cycle detection** to prevent invalid edge insertions.
- **PostgreSQL persistence** with transactional inserts and triggers.
- **REST API** built with Go + Chi router and auto-generated Swagger UI.
- **Containerized demo** (Postgres, API server, worker, Python client) via Docker Compose.
- **Schema auto-migration** through `/docker-entrypoint-initdb.d` SQL scripts.

---

## Architecture Overview

| Component | Description |
|------------|-------------|
| **server** | REST API built in Go (manages DAGs, jobs, dependencies). |
| **worker** | Background executor (simulated for demo). |
| **db** | PostgreSQL 18 with schema initialized from `/migrate/initdb`. |
| **demo** | Python client script (`demo_client.py`) that drives the end-to-end example. |

**Key Entities**

- **DAG** — a workflow namespace.
- **Job Definition** — reusable template (e.g., a command or function).
- **Job** — concrete instance of a definition within a DAG.
- **Dependency** — directed edge enforcing `parent → child` ordering.

---

## Setup and Usage

### Prerequisites
- Docker and Docker Compose (v2+)
- (Optional) Python 3.10+ for running `demo_client.py`

### 1. Build and Start Chronosched
From the project root:

```bash
docker compose up -d --build
```

This will:
- Start PostgreSQL (`chronosched-db`) and apply all migrations.
- Start the Go API server (`chronosched-server`) and worker.
- Expose:
  - API on [http://localhost:8080](http://localhost:8080)
  - Swagger UI at [http://localhost:8080/](http://localhost:8080/)

To verify DB health:
```bash
docker compose ps
docker compose exec db psql -U postgres -d chronosched -c '\d job_definitions'
```

You should see the `kind` column in the output.

---

### 2. Run the Demo Client

Run the included Python demo to exercise the full workflow:

```bash
docker compose run --rm demo
```

The demo performs:

1. **Create a DAG**  
2. **Register three job definitions** (`Wait`, `Wait5s`, `Wait10s`)  
3. **Add three jobs** to the DAG  
4. **Wire dependencies** (`Wait → Wait5s → Wait10s`)  
5. **Simulate job completion**  
6. **Query job details** back via REST

You’ll see output like:

```
POST /dags -> 201
POST /definitions -> 201
POST /jobs -> 201
POST /dependencies -> 201
GET /jobs/1 -> 200
{
  "id": 1,
  "kind": "cmd",
  "payload": { "cmd": "echo running Wait" }
}
```

---

## Directory Structure

```
chronosched/
├── cmd/
│   ├── server/           # main.go for API server
│   ├── worker/           # worker entrypoint
│   └── demo_client.py    # Python demonstration script
├── internal/
│   ├── api/              # Chi routers & HTTP handlers
│   ├── db/               # PostgreSQL repositories (pgx)
│   ├── dag/              # DAG utilities (cycle detection)
│   └── queue/            # Job queue primitives
├── migrate/
│   └── initdb/           # SQL initialization scripts (executed by Postgres)
│       ├── 000_create_db_if_not_exists.sql
│       ├── 001_run_in_chronosched.sql
│       ├── 002_init.sql
│       └── 003_seed_wait_jobs.sql
├── docker-compose.yml
└── README.md
```

---

## Development Notes

- Schema migrations are **idempotent** (`CREATE TABLE IF NOT EXISTS`).
- Healthchecks ensure DB initializes fully before API startup.
- Local cycle detection is performed in Go (`internal/dag/cycle_local.go`) when adding dependencies.
- Each `AddDependency` runs a lightweight in-memory DAG validation before committing.

---

## Example API Calls

```bash
# Create a DAG
curl -X POST http://localhost:8080/dags      -H "Content-Type: application/json"      -d '{"id":"demo-dag","namespace":"core","name":"daily_etl"}'

# Create a job definition
curl -X POST http://localhost:8080/definitions      -H "Content-Type: application/json"      -d '{"namespace":"core","name":"Wait","kind":"cmd","payloadTemplate":{"cmd":"echo Hello"}}'

# Add a job to the DAG
curl -X POST http://localhost:8080/dags/demo-dag/jobs      -H "Content-Type: application/json"      -d '{"defId":"<def_uuid>","priority":1,"payload":{"cmd":"echo running"}}'
```

---

## Common Commands

| Task | Command |
|------|----------|
| Rebuild everything | `docker compose down -v && docker compose up --build` |
| Run only server & worker | `docker compose up -d db server worker` |
| Tail logs | `docker compose logs -f server` |
| Access DB shell | `docker compose exec db psql -U postgres -d chronosched` |

---

## Demo Completion

After running `demo_client.py`, you’ll see:

```
GET /jobs/3 -> 200
{
  "id": 3,
  "kind": "cmd",
  "payload": { "cmd": "echo running Wait10s" }
}
Demo completed
```

This confirms:
- Schema loaded properly.
- Server handled all endpoints.
- Worker and DB synchronization succeeded.

---

## Author

**Edward Kuperman**  
Principal Software Engineer • Distributed Systems & FinTech  
GitHub: [@edkuperman](https://github.com/edkuperman)

---

## License

MIT License © 2025 Edward Kuperman  
See [`LICENSE`](LICENSE) for details.
