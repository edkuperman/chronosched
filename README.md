# Chronosched

Chronosched is a lightweight, namespaced, DAG‑driven job scheduler with:

- Immutable, versioned job definitions
- Namespace-scoped DAGs
- Job dependency graphs with cycle protection
- Frontier-driven scheduling
- Optional cron scheduling via `cron_spec`
- Independent workers for job execution
- PostgreSQL as the source of truth
- Fully observable via REST API

This README matches the current codebase and router.go exactly.

---

## Project Structure

```
chronosched/
│
├── cmd/
│   ├── server/         # Go API server
│   └── worker/         # Go worker
│
├── client/
│   └── python/         # Python demo client
│
├── scripts/
│   ├── run-prod.ps1
│   ├── run-debug.ps1
│   ├── run-prod.sh
│   ├── run-debug.sh
│
├── docker-compose.yml
├── docker-compose.debug.yml
└── Dockerfile.*        # Server, Worker, Debug versions
```

---

# API Endpoints (Router‑Accurate)

/healthz is unversioned.

All other endpoints live under:

```
/api/v1
```

---

# Namespaces

### Collection
- **GET**  `/api/v1/namespaces/`
- **POST** `/api/v1/namespaces/`

### Single namespace (by name)
- **GET**    `/api/v1/namespace/{name}/`
- **PUT**    `/api/v1/namespace/{name}/`
- **DELETE** `/api/v1/namespace/{name}/`

---

# DAGs

### DAG collection (per namespace)
- **GET**  `/api/v1/dags/{namespace_id}/`
- **POST** `/api/v1/dags/{namespace_id}/`
- **PUT**  `/api/v1/dags/{namespace_id}/`

### Single DAG
- **GET**    `/api/v1/dags/{namespace_id}/{id}/`
- **PUT**    `/api/v1/dags/{namespace_id}/{id}/`
- **DELETE** `/api/v1/dags/{namespace_id}/{id}/`

---

# Job Definitions

### Collection
- **GET**  `/api/v1/definitions/{namespace_id}/`
- **POST** `/api/v1/definitions/{namespace_id}/`
- **PUT**  `/api/v1/definitions/{namespace_id}/`

### Single definition
- **GET**    `/api/v1/definitions/{namespace_id}/{id}/`
- **PUT**    `/api/v1/definitions/{namespace_id}/{id}/`
- **DELETE** `/api/v1/definitions/{namespace_id}/{id}/`

Each job definition may include:

- `name`
- `version`
- `kind`
- `payload_template`
- `cron_spec` (optional, enabled)

---

# Jobs (Inside a DAG)

### Collection
- **GET**  `/api/v1/dags/{namespace_id}/{dag_id}/jobs/`
- **POST** `/api/v1/dags/{namespace_id}/{dag_id}/jobs/`
- **PUT**  `/api/v1/dags/{namespace_id}/{dag_id}/jobs/`

### Single job
- **GET**    `/api/v1/dags/{namespace_id}/{dag_id}/jobs/{id}/`
- **PUT**    `/api/v1/dags/{namespace_id}/{dag_id}/jobs/{id}/`
- **DELETE** `/api/v1/dags/{namespace_id}/{dag_id}/jobs/{id}/`

---

# Global Job Lifecycle Endpoints

Base path:

```
/api/v1/jobs/{namespace_id}/{jobId}
```

Operations:

- **POST** `/complete`
- **POST** `/fail`
- **DELETE** `/`

---

# Dependencies

Path:

```
/api/v1/dags/{namespace_id}/{dag_id}/dependencies
```

### Bulk operations
- **GET**  `/`
- **POST** `/`   (bulk create)
- **PUT**  `/`   (bulk upsert)

### Single dependency
- **PATCH** `/`
- **DELETE** `/`
(requires `parent_id` and `child_id` query params)

---

# Admin

- **GET**  `/api/v1/admin/check/global-cycles`
- **POST** `/api/v1/admin/prune`

---

# Running Chronosched

Scripts in `./scripts` provide easy workflows.

---

## Production Mode

### Windows
```
./scripts/run-prod.ps1
```

### Linux/macOS/WSL
```
./scripts/run-prod.sh
```

Equivalent:
```
docker compose up --build
```

---

## Debug Mode (Delve + debugpy)

### Windows
```
./scripts/run-debug.ps1
```

### Linux/macOS/WSL
```
./scripts/run-debug.sh
```

Equivalent:
```
docker compose -f docker-compose.yml -f docker-compose.debug.yml up --build
```

### Debug Ports

- Go Server: **40000**
- Go Worker: **40001**
- Python demo: **5678**

Use VS Code launch configurations to attach.

---

# Python Demo Client

Location:

```
client/python/
```

Environment variable:

```
CHRONOSCHED_BASE=http://server:8080
```

Run locally:

```
cd client/python
pip install -r requirements.txt
python3 demo_client.py
```

---

# License

All rights reserved.  
Use, reproduction, or distribution requires prior written permission from the author.
