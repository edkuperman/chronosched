import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request

from chronosched_client import ChronoschedClient

BASE_URL = os.getenv("CHRONOSCHED_BASE_URL", "http://server:8080")
SERVICE_PORT = int(os.getenv("PORT", "8090"))
SERVICE_HOST = os.getenv("SERVICE_HOST", "python-service")
FIRST_DAG_COMPLETED_TARGET = int(os.getenv("FIRST_DAG_COMPLETED_TARGET", "4"))
TOTAL_COMPLETED_TARGET = int(os.getenv("TOTAL_COMPLETED_TARGET", "10"))
SECOND_DAG_FAIL_COUNT = int(os.getenv("SECOND_DAG_FAIL_COUNT", "2"))
SUMMARY_POLL_SECONDS = float(os.getenv("SUMMARY_POLL_SECONDS", "2.0"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("chronosched-python-demo")


@dataclass
class DemoDag:
    label: str
    namespace: dict[str, Any] | None = None
    dag: dict[str, Any] | None = None
    dag_version: dict[str, Any] | None = None
    hello5: dict[str, Any] | None = None
    hello10: dict[str, Any] | None = None
    reportJob: dict[str, Any] | None = None
    started_hello5: int = 0
    started_hello10: int = 0
    started_report: int = 0
    successful_hello5: int = 0
    successful_hello10: int = 0
    successful_report: int = 0
    failed_hello10: int = 0
    completed_runs_seen: int = 0


@dataclass
class DemoState:
    client: ChronoschedClient
    primary: DemoDag | None = None
    secondary: DemoDag | None = None
    active_demo_label: str = "primary"
    cleanup_done: bool = False
    callback_log: list[dict[str, Any]] = field(default_factory=list)
    summary_log: list[dict[str, Any]] = field(default_factory=list)
    processed_failed_runs: set[int] = field(default_factory=set)
    second_dag_started: bool = False
    second_dag_failures_remaining: int = SECOND_DAG_FAIL_COUNT
    stop_server: Any = None
    monitor_task: asyncio.Task | None = None

    async def cleanup(self):
        if self.cleanup_done:
            return
        self.cleanup_done = True
        for demo in [self.primary, self.secondary]:
            if not demo:
                continue
            for definition in [demo.hello5, demo.hello10, demo.reportJob]:
                if definition:
                    try:
                        self.client.disable_job_definition(definition["id"])
                    except Exception:
                        pass


async def wait_for_health(client: ChronoschedClient, timeout: float = 30.0):
    deadline = asyncio.get_running_loop().time() + timeout
    last_error = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            client.healthz()
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(1)
    raise RuntimeError(f"Chronosched did not become healthy: {last_error}")


def rest_payload(path: str, message: str) -> dict[str, Any]:
    return {
        "method": "POST",
        "url": f"http://{SERVICE_HOST}:{SERVICE_PORT}{path}",
        "body": {"message": message},
    }


async def create_demo_dag(state: DemoState, *, label: str, failing_hello10: bool) -> DemoDag:
    suffix = uuid.uuid4().hex[:8]
    demo = DemoDag(label=label)
    demo.namespace = state.client.create_namespace(f"dispatcher-demo-{label}-{suffix}")
    demo.hello5 = state.client.create_job_definition(
        namespace_id=demo.namespace["id"],
        name=f"hello-5s-{label}-{suffix}",
        description=f"{label} REST callback demo job every 5 seconds",
        kind="rest",
        payload_template=rest_payload("/jobs/hello-5s", f"Hello from the 5-second job ({label})"),
        schedule={"type": "cron", "cron": "*/5 * * * * *", "timezone": "UTC", "on_failure": "continue"},
    )
    hello10_path = "/jobs/hello-10s-fail" if failing_hello10 else "/jobs/hello-10s"
    demo.hello10 = state.client.create_job_definition(
        namespace_id=demo.namespace["id"],
        name=f"hello-10s-{label}-{suffix}",
        description=f"{label} REST callback demo job every 10 seconds; depends on latest 5-second success",
        kind="rest",
        payload_template=rest_payload(hello10_path, f"Hello from the 10-second job ({label})"),
        schedule={"type": "cron", "cron": "*/10 * * * * *", "timezone": "UTC", "on_failure": "continue"},
    )
    demo.reportJob = state.client.create_job_definition(
        namespace_id=demo.namespace["id"],
        name=f"reportJob-{label}-{suffix}",
        description=f"{label} REST callback demo fan-in job; depends on latest 5-second success and latest 10-second success",
        kind="rest",
        payload_template=rest_payload("/jobs/reportJob", f"Hello from report job ({label})"),
        schedule={"type": "cron", "cron": "*/10 * * * * *", "timezone": "UTC", "on_failure": "continue"},
    )
    demo.dag = state.client.create_dag(
        demo.namespace["id"],
        name=f"dispatcher-demo-dag-{label}-{suffix}",
        description=f"Dispatcher/callback demo ({label})",
    )
    demo.dag_version = state.client.publish_dag_version(
        demo.dag["id"],
        version_note=f"Dispatcher/callback demo ({label})",
        nodes=[
            {"node_key": "hello_5s", "display_name": "Hello every 5s", "job_definition_id": demo.hello5["id"]},
            {"node_key": "hello_10s", "display_name": "Hello every 10s", "job_definition_id": demo.hello10["id"]},
            {"node_key": "reportJob", "display_name": "The report job", "job_definition_id": demo.reportJob["id"]},
        ],
        edges=[
            {"from": "hello_5s", "to": "hello_10s"},
            {"from": "hello_5s", "to": "reportJob"},
            {"from": "hello_10s", "to": "reportJob"},
        ],
    )
    state.client.activate_dag_version(demo.dag_version["id"])
    logger.info("Activated %s DAG %s", label, demo.dag["id"])
    return demo


async def bootstrap(state: DemoState):
    await wait_for_health(state.client)
    state.primary = await create_demo_dag(state, label="primary", failing_hello10=False)
    state.active_demo_label = "primary"


def completed_runs_for(demo: DemoDag | None, client: ChronoschedClient) -> list[dict[str, Any]]:
    if not demo or not demo.dag:
        return []
    runs = client.list_runs(demo.dag["id"])
    terminal_statuses = {"succeeded", "failed", "missed", "cancelled", "blocked", "lost"}
    return [r for r in runs if r.get("status") in terminal_statuses]


def run_identifier(run: dict[str, Any]) -> int | None:
    raw = run.get("run_id", run.get("id"))
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


async def maybe_start_second_dag(state: DemoState):
    if state.second_dag_started:
        return
    primary_completed = completed_runs_for(state.primary, state.client)
    if len(primary_completed) < FIRST_DAG_COMPLETED_TARGET:
        return
    state.secondary = await create_demo_dag(state, label="secondary", failing_hello10=True)
    state.second_dag_started = True
    state.active_demo_label = "secondary"
    logger.info("Started secondary DAG after %s completed primary runs", len(primary_completed))


async def poll_failed_run_summaries(state: DemoState):
    terminal_statuses = {"succeeded", "failed", "missed", "cancelled", "blocked", "lost"}
    for demo in [state.primary, state.secondary]:
        if not demo or not demo.dag:
            continue
        runs = state.client.list_runs(demo.dag["id"])
        demo.completed_runs_seen = len([r for r in runs if r.get("status") in terminal_statuses])
        for run in runs:
            if run.get("status") != "failed":
                continue
            run_id = run_identifier(run)
            if run_id is None:
                logger.warning("Skipping failed run without id for %s DAG: %s", demo.label, run)
                continue
            if run_id in state.processed_failed_runs:
                continue
            try:
                summary = state.client.get_run_summary(run_id)
                record = {
                    "run_id": run_id,
                    "dag_label": demo.label,
                    "status": run.get("status"),
                    "summary": summary,
                }
                state.summary_log.append(record)
                state.processed_failed_runs.add(run_id)
                logger.info("Failure summary for run %s (%s): %s", run_id, demo.label, summary)
            except Exception as exc:
                logger.warning("Could not fetch summary for failed run %s yet: %s", run_id, exc)


async def monitor_runs(state: DemoState):
    while not state.cleanup_done:
        try:
            await maybe_start_second_dag(state)
            await poll_failed_run_summaries(state)
            primary_completed = completed_runs_for(state.primary, state.client)
            secondary_completed = completed_runs_for(state.secondary, state.client)
            if state.second_dag_started and len(secondary_completed) >= TOTAL_COMPLETED_TARGET:
                await state.cleanup()
                if state.stop_server:
                    state.stop_server()
                return
            if (not state.second_dag_started) and len(primary_completed) >= TOTAL_COMPLETED_TARGET:
                await state.cleanup()
                if state.stop_server:
                    state.stop_server()
                return
        except Exception as exc:
            logger.warning("monitor loop error: %s", exc)
        await asyncio.sleep(SUMMARY_POLL_SECONDS)


async def emit_job_lifecycle(job_id: int, external_execution_id: str, succeed: bool, message: str, delay: float = 0.5):
    await asyncio.sleep(0.1)
    client.post_job_event(job_id, "started", external_execution_id=external_execution_id)
    await asyncio.sleep(delay)
    client.post_job_event(job_id, "heartbeat", external_execution_id=external_execution_id, reason_detail="still running")
    await asyncio.sleep(delay)
    if succeed:
        client.post_job_event(job_id, "succeeded", external_execution_id=external_execution_id, reason_detail=message)
    else:
        client.post_job_event(job_id, "failed", external_execution_id=external_execution_id, reason_code="demo_failure", reason_detail=message)


client = ChronoschedClient(BASE_URL)
state = DemoState(client=client)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await bootstrap(state)
    state.monitor_task = asyncio.create_task(monitor_runs(state))
    try:
        yield
    finally:
        if state.monitor_task:
            state.monitor_task.cancel()
            try:
                await state.monitor_task
            except asyncio.CancelledError:
                pass
        await state.cleanup()


app = FastAPI(title="Chronosched Dispatcher Demo", lifespan=lifespan)
app.state.demo = state


@app.get("/status")
async def status():
    primary_runs = state.client.list_runs(state.primary.dag["id"]) if state.primary and state.primary.dag else []
    secondary_runs = state.client.list_runs(state.secondary.dag["id"]) if state.secondary and state.secondary.dag else []
    return {
        "active_demo_label": state.active_demo_label,
        "cleanup_done": state.cleanup_done,
        "second_dag_started": state.second_dag_started,
        "second_dag_failures_remaining": state.second_dag_failures_remaining,
        "primary": {
            "namespace": state.primary.namespace if state.primary else None,
            "dag": state.primary.dag if state.primary else None,
            "dag_version": state.primary.dag_version if state.primary else None,
            "hello5": state.primary.hello5 if state.primary else None,
            "hello10": state.primary.hello10 if state.primary else None,
            "reportJob": state.primary.reportJob if state.primary else None,
            "started_hello5": state.primary.started_hello5 if state.primary else 0,
            "started_hello10": state.primary.started_hello10 if state.primary else 0,
            "started_report": state.primary.started_report if state.primary else 0,
            "successful_hello5": state.primary.successful_hello5 if state.primary else 0,
            "successful_hello10": state.primary.successful_hello10 if state.primary else 0,
            "successful_report": state.primary.successful_report if state.primary else 0,
            "failed_hello10": state.primary.failed_hello10 if state.primary else 0,
            "runs": primary_runs,
        },
        "secondary": {
            "namespace": state.secondary.namespace if state.secondary else None,
            "dag": state.secondary.dag if state.secondary else None,
            "dag_version": state.secondary.dag_version if state.secondary else None,
            "hello5": state.secondary.hello5 if state.secondary else None,
            "hello10": state.secondary.hello10 if state.secondary else None,
            "reportJob": state.secondary.reportJob if state.secondary else None,
            "started_hello5": state.secondary.started_hello5 if state.secondary else 0,
            "started_hello10": state.secondary.started_hello10 if state.secondary else 0,
            "started_report": state.secondary.started_report if state.secondary else 0,
            "successful_hello5": state.secondary.successful_hello5 if state.secondary else 0,
            "successful_hello10": state.secondary.successful_hello10 if state.secondary else 0,
            "successful_report": state.secondary.successful_report if state.secondary else 0,
            "failed_hello10": state.secondary.failed_hello10 if state.secondary else 0,
            "runs": secondary_runs,
        },
        "summary_log": state.summary_log[-20:],
        "callbacks": state.callback_log[-50:],
    }


async def accept_job(request: Request, job_name: str, success_counter: str, start_counter: str, *, succeed: bool = True, failure_message: str | None = None):
    payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    chronosched = payload.get("chronosched", {})
    job_id = int(chronosched["job_id"])
    external_execution_id = f"{job_name}-{uuid.uuid4().hex[:8]}"
    dag_label = chronosched.get("dag_name", "")
    demo = state.secondary if state.secondary and state.secondary.dag and state.secondary.dag.get("name") == dag_label else state.primary
    if demo:
        setattr(demo, start_counter, getattr(demo, start_counter) + 1)
        if succeed:
            setattr(demo, success_counter, getattr(demo, success_counter) + 1)
        elif job_name == "hello_10s":
            demo.failed_hello10 += 1
    state.callback_log.append({
        "job": job_name,
        "event": "accepted",
        "payload": payload,
        "external_execution_id": external_execution_id,
        "succeed": succeed,
    })
    asyncio.create_task(emit_job_lifecycle(job_id, external_execution_id, succeed, failure_message or f"{job_name} completed"))
    return {"accepted": True, "external_execution_id": external_execution_id}


@app.post("/jobs/hello-5s")
async def hello_5s(request: Request):
    return await accept_job(request, "hello_5s", "successful_hello5", "started_hello5")


@app.post("/jobs/hello-10s")
async def hello_10s(request: Request):
    return await accept_job(request, "hello_10s", "successful_hello10", "started_hello10")


@app.post("/jobs/hello-10s-fail")
async def hello_10s_fail(request: Request):
    should_fail = state.second_dag_failures_remaining > 0
    if should_fail:
        state.second_dag_failures_remaining -= 1
    return await accept_job(
        request,
        "hello_10s",
        "successful_hello10",
        "started_hello10",
        succeed=not should_fail,
        failure_message="Intentional demo failure in 10-second workflow" if should_fail else "hello_10s completed after prior demo failures",
    )


@app.post("/jobs/reportJob")
async def report(request: Request):
    return await accept_job(request, "reportJob", "successful_report", "started_report")
