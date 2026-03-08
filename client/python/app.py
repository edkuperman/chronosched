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
        if self.dag:
            try:
                self.client.delete_dag(self.dag["id"])
            except Exception:
                pass
        for definition in (self.hello5, self.hello10):
            if definition:
                try:
                    self.client.delete_job_definition(definition["id"])
                except Exception:
                    pass


async def wait_for_health(client: ChronoschedClient, timeout: float = 30.0):
    deadline = asyncio.get_running_loop().time() + timeout
    last_error = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            client.healthz()
            return
        except Exception as exc:  # pragma: no cover - startup retry path
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
    state.namespace = state.client.create_namespace(f"rest-demo-{suffix}")
    state.hello5 = state.client.create_job_definition(
        namespace_id=state.namespace["id"],
        name=f"hello-5s-{suffix}",
        description="REST callback demo job every 5 seconds",
        kind="rest",
        payload_template=rest_payload("/jobs/hello-5s", "Hello from the 5-second job"),
        schedule={"cron": "*/5 * * * * *", "timezone": "UTC", "on_failure": "continue"},
    )
    state.hello10 = state.client.create_job_definition(
        namespace_id=state.namespace["id"],
        name=f"hello-10s-{suffix}",
        description="REST callback demo job every 10 seconds; only allowed after latest 5-second success",
        kind="rest",
        payload_template=rest_payload("/jobs/hello-10s", "Hello from the 10-second job"),
        schedule={"cron": "*/10 * * * * *", "timezone": "UTC", "on_failure": "continue"},
    )
    state.dag = state.client.create_dag(
        state.namespace["id"],
        name=f"rest-demo-dag-{suffix}",
        description="Independent schedules with latest-success gating",
    )
    state.dag_version = state.client.publish_dag_version(
        state.dag["id"],
        version_note="REST callback demo",
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
            if not state.dag or not state.hello10:
                await asyncio.sleep(1)
                continue
            runs = state.client.list_runs(state.dag["id"])
            completed = [
                r for r in runs
                if r.get("trigger", {}).get("definition_id") == state.hello10["id"]
                and r.get("status") in {"succeeded", "failed", "missed", "cancelled"}
            ]
            if len(completed) >= 2:
                await state.cleanup()
                if state.stop_server:
                    state.stop_server()
                return
        except Exception:
            pass
        await asyncio.sleep(1)


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


app = FastAPI(title="Chronosched REST Callback Demo", lifespan=lifespan)
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
        "cleanup_done": state.cleanup_done,
        "runs": runs,
        "callbacks": state.callback_log[-20:],
    }


@app.post("/jobs/hello-5s")
async def hello_5s(request: Request):
    payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    state.successful_hello5 += 1
    entry = {"job": "hello_5s", "payload": payload, "count": state.successful_hello5}
    state.callback_log.append(entry)
    return {"success": True, "message": f"hello_5s invocation #{state.successful_hello5}"}


@app.post("/jobs/hello-10s")
async def hello_10s(request: Request):
    payload = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    state.successful_hello10 += 1
    entry = {"job": "hello_10s", "payload": payload, "count": state.successful_hello10}
    state.callback_log.append(entry)
    return {"success": True, "message": f"hello_10s invocation #{state.successful_hello10}"}
