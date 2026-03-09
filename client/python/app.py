import asyncio
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


@dataclass
class DemoState:
    client: ChronoschedClient
    namespace: dict[str, Any] | None = None
    dag: dict[str, Any] | None = None
    dag_version: dict[str, Any] | None = None
    hello5: dict[str, Any] | None = None
    hello10: dict[str, Any] | None = None
    cleanup_done: bool = False
    successful_hello5: int = 0
    successful_hello10: int = 0
    started_hello5: int = 0
    started_hello10: int = 0
    callback_log: list[dict[str, Any]] = field(default_factory=list)
    stop_server: Any = None
    monitor_task: asyncio.Task | None = None

    async def cleanup(self):
        if self.cleanup_done:
            return
        self.cleanup_done = True
        if self.hello5:
            try:
                self.client.disable_job_definition(self.hello5["id"])
            except Exception:
                pass
        if self.hello10:
            try:
                self.client.disable_job_definition(self.hello10["id"])
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


async def bootstrap(state: DemoState):
    await wait_for_health(state.client)
    suffix = uuid.uuid4().hex[:8]
    state.namespace = state.client.create_namespace(f"dispatcher-demo-{suffix}")
    state.hello5 = state.client.create_job_definition(
        namespace_id=state.namespace["id"],
        name=f"hello-5s-{suffix}",
        description="REST callback demo job every 5 seconds",
        kind="rest",
        payload_template=rest_payload("/jobs/hello-5s", "Hello from the 5-second job"),
        schedule={"type": "cron", "cron": "*/5 * * * * *", "timezone": "UTC", "on_failure": "continue"},
    )
    state.hello10 = state.client.create_job_definition(
        namespace_id=state.namespace["id"],
        name=f"hello-10s-{suffix}",
        description="REST callback demo job every 10 seconds; depends on latest 5-second success",
        kind="rest",
        payload_template=rest_payload("/jobs/hello-10s", "Hello from the 10-second job"),
        schedule={"type": "cron", "cron": "*/10 * * * * *", "timezone": "UTC", "on_failure": "continue"},
    )
    state.dag = state.client.create_dag(
        state.namespace["id"],
        name=f"dispatcher-demo-dag-{suffix}",
        description="Dispatcher/callback demo",
    )
    state.dag_version = state.client.publish_dag_version(
        state.dag["id"],
        version_note="Dispatcher/callback demo",
        nodes=[
            {"node_key": "hello_5s", "display_name": "Hello every 5s", "job_definition_id": state.hello5["id"]},
            {"node_key": "hello_10s", "display_name": "Hello every 10s", "job_definition_id": state.hello10["id"]},
        ],
        edges=[{"from": "hello_5s", "to": "hello_10s"}],
    )
    state.client.activate_dag_version(state.dag_version["id"])


async def monitor_runs(state: DemoState):
    while not state.cleanup_done:
        try:
            if not state.dag:
                await asyncio.sleep(1)
                continue
            runs = state.client.list_runs(state.dag["id"])
            completed = [r for r in runs if r.get("status") in {"succeeded", "failed", "missed", "cancelled"}]
            if len(completed) >= 4:
                await state.cleanup()
                if state.stop_server:
                    state.stop_server()
                return
        except Exception:
            pass
        await asyncio.sleep(1)


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
    runs = state.client.list_runs(state.dag["id"]) if state.dag else []
    return {
        "namespace": state.namespace,
        "dag": state.dag,
        "dag_version": state.dag_version,
        "hello5": state.hello5,
        "hello10": state.hello10,
        "successful_hello5": state.successful_hello5,
        "successful_hello10": state.successful_hello10,
        "started_hello5": state.started_hello5,
        "started_hello10": state.started_hello10,
        "cleanup_done": state.cleanup_done,
        "runs": runs,
        "callbacks": state.callback_log[-20:],
    }


async def accept_job(request: Request, job_name: str, success_counter: str, start_counter: str):
    payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    chronosched = payload.get("chronosched", {})
    job_id = int(chronosched["job_id"])
    external_execution_id = f"{job_name}-{uuid.uuid4().hex[:8]}"
    setattr(state, start_counter, getattr(state, start_counter) + 1)
    state.callback_log.append({"job": job_name, "event": "accepted", "payload": payload, "external_execution_id": external_execution_id})
    asyncio.create_task(emit_job_lifecycle(job_id, external_execution_id, True, f"{job_name} completed"))
    setattr(state, success_counter, getattr(state, success_counter) + 1)
    return {"accepted": True, "external_execution_id": external_execution_id}


@app.post("/jobs/hello-5s")
async def hello_5s(request: Request):
    return await accept_job(request, "hello_5s", "successful_hello5", "started_hello5")


@app.post("/jobs/hello-10s")
async def hello_10s(request: Request):
    return await accept_job(request, "hello_10s", "successful_hello10", "started_hello10")
