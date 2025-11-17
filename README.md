# Chronosched

Chronosched is a lightweight, namespaced, versioned DAG scheduler with:

- Immutable job definitions (versioned)
- Namespace-scoped DAGs (versioned)
- Job dependency graphs with cycle protection
- Frontier-driven execution scheduling
- Optional cron scheduling via `cron_spec`
- Independent worker process (leases jobs, executes payloads)
- PostgreSQL as the single source of truth
- REST API for all management operations

This README matches the current `router.go`, handlers, and the working demo client.

--- 

## Project Layout

```
chronosched/
│
├── cmd/
│   ├── server/
│   └── worker/
│
├── internal/
│   ├── api/
│   ├── db/
│   ├── scheduler/
│   └── util/
│
├── client/
│   └── python/
│
├── scripts/
│
├── docker-compose.yml
├── docker-compose.debug.yml
└── Dockerfile-server / Dockerfile-worker
```

---

## Running Chronosched

### Production

Windows PowerShell:
```
./scripts/run-prod.ps1
```

Linux/macOS/WSL:
```
./scripts/run-prod.sh
```

Equivalent:
```
docker compose up --build
```

---

## Debug Mode

Windows:
```
./scripts/run-debug.ps1
```

Linux/macOS/WSL:
```
./scripts/run-debug.sh
```

Equivalent:
```
docker compose -f docker-compose.yml -f docker-compose.debug.yml up --build
```

### Debug Ports
- Server: 40000
- Worker: 40001
- Python demo: 5678

---

## API Overview

All API paths except `/healthz` live under `/api/v1`.

### Health
```
GET /healthz
```

### Namespaces
```
GET  /api/v1/namespaces
POST /api/v1/namespaces
GET  /api/v1/namespace/{name}
PUT  /api/v1/namespace/{name}
DELETE /api/v1/namespace/{name}
```

### DAGs
```
GET  /api/v1/dags/{namespace_id}
POST /api/v1/dags/{namespace_id}
GET  /api/v1/dags/{namespace_id}/{dag_id}
PUT  /api/v1/dags/{namespace_id}/{dag_id}
DELETE /api/v1/dags/{namespace_id}/{dag_id}
```

### Definitions
```
GET  /api/v1/definitions/{namespace_id}
POST /api/v1/definitions/{namespace_id}
GET  /api/v1/definitions/{namespace_id}/{def_id}
PUT  /api/v1/definitions/{namespace_id}/{def_id}
DELETE /api/v1/definitions/{namespace_id}/{def_id}
```

Response shape:
```json
{
  "results": [ ... ]
}
```

### Jobs
```
GET  /api/v1/dags/{namespace_id}/{dag_id}/jobs
POST /api/v1/dags/{namespace_id}/{dag_id}/jobs
GET  /api/v1/dags/{namespace_id}/{dag_id}/jobs/{job_id}
PUT  /api/v1/dags/{namespace_id}/{dag_id}/jobs/{job_id}
DELETE /api/v1/dags/{namespace_id}/{dag_id}/jobs/{job_id}
```

### Worker Lifecycle
```
POST   /api/v1/jobs/{namespace_id}/{job_id}/complete
POST   /api/v1/jobs/{namespace_id}/{job_id}/fail
DELETE /api/v1/jobs/{namespace_id}/{job_id}
```

### Dependencies
```
GET    /api/v1/dags/{namespace_id}/{dag_id}/dependencies
POST   /api/v1/dags/{namespace_id}/{dag_id}/dependencies
PUT    /api/v1/dags/{namespace_id}/{dag_id}/dependencies
PATCH  /api/v1/dags/{namespace_id}/{dag_id}/dependencies?parent_id=X&child_id=Y
DELETE /api/v1/dags/{namespace_id}/{dag_id}/dependencies?parent_id=X&child_id=Y
```

### Admin
```
GET  /api/v1/admin/check/global-cycles
POST /api/v1/admin/prune
```

---

## Cron Scheduling

Definitions may include:

```json
{
  "cron_spec": "*/5 * * * * *"
}
```

Scheduler reloads cron definitions:
- At startup
- Whenever new definitions are created (`createDefinitions` triggers reload)

---

## Python Demo Client

Located at:
```
client/python/demo_client.py
```

Run with:
```
python3 demo_client.py
```

Demo steps:
1. `/healthz`
2. Create namespace
3. Non-cron DAG test
4. Worker executes jobs
5. Cron definition test
6. Scheduler enqueues jobs
7. Demo verifies >= 2 cron jobs

---

# License

All rights reserved.  
Use, reproduction, or distribution requires prior written permission from the author.
