# Execution Lifecycle and Failure Recovery

Chronosched models execution as a series of explicit runtime states. This allows the system to distinguish between different failure windows and preserve operational visibility when worker or target-service crashes occur.

---

# Execution Lifecycle

```text
queued
  worker leases
dispatching
  worker crashes before result      -> stale dispatching -> lost
  dispatch fails and is reported    -> failed
  target accepts                    -> dispatched

dispatched
  target never reports started      -> stale dispatched -> lost
  target reports started            -> running

running
  target stops heartbeating         -> stale running -> lost
  target reports success/failure    -> succeeded/failed
```

---

# State Definitions

| State | Meaning |
|---|---|
| `queued` | Job is ready for worker leasing. |
| `dispatching` | A worker leased the job and is attempting to dispatch it to the target service. |
| `dispatched` | The target service accepted the request, but execution has not yet been confirmed as started. |
| `running` | The target service reported that execution has started. |
| `succeeded` | Execution completed successfully. |
| `failed` | Execution definitively reported failure. |
| `lost` | Execution outcome became unknown due to timeout, worker crash, or heartbeat loss. |

---

# Failure Windows

Chronosched explicitly models different execution ambiguity windows rather than collapsing all failures into a single state.

## Worker crashes before dispatch result

```text
queued
  -> dispatching
       X worker crashes here
```

The worker leased the job but never reported whether dispatch succeeded or failed.

The sweeper detects stale `dispatching` jobs whose lease expired without a dispatch result and marks them as:

```text
dispatching -> lost
```

This represents:

> "The scheduler no longer knows whether dispatch occurred."

---

## Dispatch accepted but execution never started

```text
queued
  -> dispatching
  -> dispatched
       X target never reports started
```

The target service accepted the dispatch request, but Chronosched never received confirmation that execution actually started.

The sweeper detects stale `dispatched` jobs and marks them:

```text
dispatched -> lost
```

---

## Running job stops heartbeating

```text
queued
  -> dispatching
  -> dispatched
  -> running
       X heartbeat expires
```

Chronosched previously confirmed execution started, but later lost execution visibility.

The sweeper detects stale `running` jobs whose heartbeat expired and marks them:

```text
running -> lost
```

This is typically the most operationally significant failure mode because the target service may have already performed external side effects.

---

# Lost vs Failed

Chronosched intentionally distinguishes between `failed` and `lost`.

## Failed

A `failed` job means:

> The target service explicitly reported execution failure.

Example:

```text
dispatching -> failed
```

## Lost

A `lost` job means:

> Chronosched lost visibility into execution outcome.

Examples include:

- worker crash before dispatch result
- dispatch accepted but execution never confirmed
- heartbeat timeout
- network interruption during status reporting

This distinction is important because, in distributed systems, the absence of a success response does not necessarily mean the underlying operation did not occur.

---

# Recovery Model

Chronosched does not automatically retry `lost` jobs.

Instead, it preserves the failure state and exposes it through inspection APIs so operators or client systems can investigate before deciding whether rerun is safe.

Typical flow:

```text
lost
  -> investigate
  -> explicit restart/rerun if appropriate
```

This avoids unsafe automatic retries for operations that may have already produced external side effects, such as:

- payments
- trades
- notifications
- file transfers
- external API mutations

---

# Operational Investigation Flow

A typical investigation flow is:

## 1. Find problem jobs

```http
GET /api/v1/namespaces/{name}
GET /api/v1/namespaces/{namespace_id}/jobs/problems
```

This exposes jobs in states such as:

- `failed`
- `lost`
- `blocked`

---

## 2. Inspect the run

```http
GET /api/v1/runs/{run_id}
GET /api/v1/runs/{run_id}/jobs
GET /api/v1/runs/{run_id}/graph
```

This allows operators to inspect:

- upstream failures
- downstream blocked jobs
- execution ordering
- current DAG state

---

## 3. Determine readiness/blocking causes

```http
GET /api/v1/jobs/{job_id}/readiness
```

This identifies which upstream jobs are preventing execution.

---

## 4. Optionally retrieve an execution summary

```http
GET /api/v1/runs/{run_id}/summary
```

If AI summarization is enabled, Chronosched can generate a higher-level operational explanation of root cause and downstream impact.

---

## 5. Explicitly restart work

```http
POST /api/v1/namespaces/{namespace_id}/jobs/{job_id}/restart
```

Restart behavior may optionally cascade to downstream jobs.

---

# Idempotency and Distributed Systems Correctness

Chronosched can detect ambiguous execution outcomes, but it cannot determine whether rerunning a business operation is semantically safe.

For example:

```text
target charged customer
worker crashed before reporting success
```

The scheduler may only know:

```text
running -> lost
```

Whether rerunning the operation is safe depends on the target system.

For this reason, target services are expected to implement idempotency using identifiers such as:

- job ID
- run ID
- execution attempt ID

This separation is intentional:

- Chronosched owns orchestration correctness
- Target services own business correctness

---

# Execution History Preservation

Chronosched preserves execution history rather than overwriting ambiguous attempts.

Example:

```text
Attempt 1 -> lost (heartbeat_timeout)
Attempt 2 -> succeeded
```

This preserves operational visibility, auditability, and debugging context.

Chronosched intentionally avoids hiding distributed-system ambiguity behind simplified state transitions.
