"""The worker runs on the shared worker host.

``Worker.run`` starts the native worker host from the worker's configuration
and pulls work from it, one loop per kind: it takes room to run one more,
pulls the next piece, runs it and hands back its outcome. These tests drive
the loops with a stand-in for the host.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from orcher import TaskContext, WorkflowContext, task, workflow
from orcher._proto import host_pb2, kernel_pb2
from orcher.decorators import GlobalRegistry
from orcher.worker import worker as worker_module
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker, WorkerState, _task_request
from tests.support.kernel import activation
from tests.support.messages import answer_of, message, task_message


@task(name="host-double")
async def double(ctx: TaskContext, n: int) -> dict[str, Any]:
    return {"n": n * 2, "attempt_timeout": ctx.start_to_close_timeout.total_seconds()}


async def waits_for_a_task(ctx: WorkflowContext) -> Any:
    return await ctx.execute_task(double, n=1)


class FakeHost:
    """The worker host, as the worker's pull loops use it."""

    def __init__(self) -> None:
        self.activations: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.tasks: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.operations: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.answers: list[dict[str, Any]] = []
        self.results: list[tuple[bytes, Any]] = []
        self.answered = asyncio.Event()
        self.stopped = False

    async def next_activation(self) -> bytes | None:
        return await self.activations.get()

    async def next_task(self) -> bytes | None:
        return await self.tasks.get()

    async def next_actor_operation(self) -> bytes | None:
        return await self.operations.get()

    async def complete_activation(self, result: bytes) -> None:
        self.answers.append(answer_of(kernel_pb2.RunActivationResult.FromString(result)))
        self.answered.set()

    async def complete_task(self, outcome: bytes) -> None:
        reported = host_pb2.TaskOutcome.FromString(outcome)
        if reported.HasField("value"):
            self.results.append((reported.token, json.loads(reported.value.data)))
        else:
            failure = reported.failure
            self.results.append(
                (reported.token, failure.message, failure.error_type, failure.non_retryable)
            )
        self.answered.set()

    def heartbeat(self, heartbeat: bytes) -> bytes:
        return b""  # an encoded HeartbeatAnswer asking nothing

    async def task_cancelled(self, token: bytes) -> bool:
        await asyncio.Event().wait()
        return False

    def wire_metrics(self) -> str:
        return '{"methods": {}, "work": {}}'

    async def shutdown(self) -> None:
        # Pulls end once the host has stopped.
        self.stopped = True
        for queue in (self.activations, self.tasks, self.operations):
            queue.put_nowait(None)


def make_worker(**config: Any) -> Worker:
    registry = GlobalRegistry.get_instance()
    if registry.get_workflow("host-waits") is None:
        workflow(name="host-waits")(waits_for_a_task)
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q", **config))
    worker._workflow_handlers["host-waits"] = registry.get_workflow("host-waits")
    worker._task_handlers["host-double"] = double.__orcher_task__
    return worker


async def started(worker: Worker, host: FakeHost) -> tuple[asyncio.Task[None], list[str]]:
    """Run ``worker`` on ``host``; the configuration it started the host with."""
    configs: list[str] = []

    async def start(config_json: str, registrations_json: str) -> FakeHost:
        configs.extend([config_json, registrations_json])
        return host

    stub = SimpleNamespace(WorkerHost=SimpleNamespace(start=start))
    patches = [
        mock.patch.object(worker_module, "is_native_available", return_value=True),
        mock.patch.object(worker_module, "get_native_module", return_value=stub),
    ]
    for patch in patches:
        patch.start()
    running = asyncio.create_task(worker.run())
    running.add_done_callback(lambda _: [patch.stop() for patch in patches])
    while worker.state != WorkerState.RUNNING:
        await asyncio.sleep(0.01)
    return running, configs


async def answered(host: FakeHost) -> None:
    await asyncio.wait_for(host.answered.wait(), timeout=5)
    host.answered.clear()


async def test_the_host_is_started_with_the_workers_configuration() -> None:
    worker = make_worker(
        namespace="shop",
        identity="worker-1",
        max_concurrent_workflow_executions=3,
        max_concurrent_task_executions=4,
        max_concurrent_actor_operations=5,
        workflow_poller_count=6,
        task_poller_count=7,
        actor_poller_count=8,
        shutdown_grace_time_ms=2_500,
        version_id="v7",
    )
    host = FakeHost()
    running, (config_json, registrations_json) = await started(worker, host)
    await worker.shutdown()
    await running

    config = json.loads(config_json)
    from orcher import __version__

    assert config == {
        "server_url": "http://localhost:50051",
        "task_queue": "q",
        "namespace": "shop",
        "identity": "worker-1",
        "api_key": None,
        "organization_id": None,
        "version_id": "v7",
        "tls_ca_cert_path": None,
        "tls_client_cert_path": None,
        "tls_client_key_path": None,
        "max_concurrent_workflows": 3,
        "max_concurrent_tasks": 4,
        "max_concurrent_actor_operations": 5,
        "workflow_pollers": 6,
        "task_pollers": 7,
        "actor_pollers": 8,
        "shutdown_grace_ms": 2_500,
        "sdk_name": "python",
        "sdk_version": __version__,
    }
    registrations = json.loads(registrations_json)
    assert "host-waits" in registrations["workflow_types"]
    assert "host-double" in registrations["task_types"]
    assert host.stopped


async def test_activations_and_tasks_are_pulled_run_and_answered() -> None:
    worker = make_worker()
    host = FakeHost()
    running, _ = await started(worker, host)

    host.activations.put_nowait(message(activation("host-waits")).SerializeToString())
    await answered(host)
    (answer,) = host.answers
    (command,) = answer["commands"]
    assert command["step_id"] == "host-double_1"
    assert command["run_task"]["task_type"] == "host-double"

    host.tasks.put_nowait(
        task_message(
            {
                "token": list(b"tok"),
                "task_id": "host-double_1",
                "task_type": "host-double",
                "workflow_id": "wf-1",
                "run_id": "run-1",
                "task_queue": "q",
                "namespace": "default",
                "input": list(b'{"n": 21}'),
                "attempt": 1,
                "attempt_timeout_ms": 30_000,
                "heartbeat_timeout_ms": None,
            }
        ).SerializeToString()
    )
    await answered(host)
    # The task sees the attempt timeout the engine gave it.
    assert host.results == [(b"tok", {"n": 42, "attempt_timeout": 30.0})]

    await worker.shutdown()
    await asyncio.wait_for(running, timeout=5)
    assert host.stopped
    assert worker.state == WorkerState.STOPPED


async def test_a_full_worker_pulls_no_more_until_it_has_room() -> None:
    worker = make_worker(max_concurrent_task_executions=1)
    host = FakeHost()
    release = asyncio.Event()
    pulled: list[str] = []

    async def blocked(request: dict[str, Any]) -> Any:
        pulled.append(request["task_id"])
        await release.wait()
        return None

    worker._execute_task = blocked  # type: ignore[method-assign]
    running, _ = await started(worker, host)
    for n in range(2):
        host.tasks.put_nowait(task_message({"token": [n], "task_id": f"t{n}"}).SerializeToString())
    await asyncio.sleep(0.1)
    # The second task is left with the host while the first one runs.
    assert pulled == ["t0"]
    assert host.tasks.qsize() == 1

    release.set()
    await answered(host)
    await asyncio.sleep(0.1)
    assert pulled == ["t0", "t1"]

    await worker.shutdown()
    await asyncio.wait_for(running, timeout=5)


def test_a_task_crosses_with_its_limits_and_input() -> None:
    request = _task_request(
        task_message(
            {
                "token": [1, 2],
                "task_id": "t_1",
                "task_type": "t",
                "workflow_id": "wf",
                "run_id": "run",
                "task_queue": "q",
                "namespace": "ns",
                "input": list(b'{"a": 1}'),
                "attempt": 2,
                "attempt_timeout_ms": 1_500,
                "heartbeat_timeout_ms": 500,
            }
        )
    )
    assert request["task_token"] == b"\x01\x02"
    assert request["input"] == {"a": 1}
    assert request["execution_id"] == "run"
    assert request["start_to_close_timeout_ms"] == 1_500
    assert request["heartbeat_timeout_ms"] == 500
    # No input, or input that is not JSON, is no arguments.
    assert _task_request(task_message({"input": []}))["input"] == {}
    assert _task_request(task_message({"input": list(b"{nope")}))["input"] == {}


async def test_shutdown_with_a_timeout_says_the_timeout_is_ignored() -> None:
    worker = make_worker()
    with pytest.warns(DeprecationWarning, match="timeout_ms"):
        await worker.shutdown(timeout_ms=5)
