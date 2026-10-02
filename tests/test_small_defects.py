"""Smaller defects, each one a value that was read, passed or picked wrongly."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

import pytest

from orcher import TaskContext, task


@task(name="defects-capture-limits")
async def capture_limits(ctx: TaskContext) -> list[float | None]:
    limits = (ctx.info.start_to_close_timeout, ctx.info.heartbeat_timeout)
    return [None if t is None else t.total_seconds() for t in limits]


async def test_the_task_executor_reports_the_start_to_close_timeout() -> None:
    # It used to fill start_to_close_timeout from the schedule-to-close one.
    from orcher.decorators.registry import GlobalRegistry
    from orcher.worker.executor import ExecutorConfig, TaskExecutor
    from orcher.worker.poller import TaskTask

    # Other tests reset the global registry.
    registry = GlobalRegistry.get_instance()
    if registry.get_task("defects-capture-limits") is None:
        registry.register_task(capture_limits.__orcher_task__)
    executor = TaskExecutor(ExecutorConfig())
    polled = TaskTask(
        task_token=b"t",
        task_id="defects-capture-limits_1",
        workflow_id="wf",
        run_id="run",
        task_type="defects-capture-limits",
        task_queue="q",
        input={},
        heartbeat_timeout_seconds=3.0,
        schedule_to_close_timeout_seconds=600.0,
        start_to_close_timeout_seconds=60.0,
    )
    result = await executor._execute_internal(polled)
    assert result.result == [60.0, 3.0]


async def test_run_sync_refuses_to_run_inside_an_event_loop() -> None:
    # The guard raised RuntimeError and then caught it itself.
    from orcher._internal.async_utils import run_sync

    async def nothing() -> None:
        return None

    coro = nothing()
    with pytest.raises(RuntimeError, match="async context"):
        run_sync(coro)
    coro.close()


def test_run_sync_runs_outside_an_event_loop() -> None:
    from orcher._internal.async_utils import run_sync

    async def answer() -> int:
        return 42

    assert run_sync(answer()) == 42


async def test_actor_send_invokes_the_operation_without_waiting_for_it() -> None:
    # send() called a bridge method that does not exist, and only logged.
    from orcher.actor.client import ActorInvocationClient

    invoked: list[dict[str, Any]] = []
    release = asyncio.Event()

    class Bridge:
        async def invoke_actor_operation(self, **kwargs: Any) -> bytes:
            invoked.append(kwargs)
            await release.wait()
            return b"null"

    client = ActorInvocationClient(Bridge(), "Cart", "user-1")
    await asyncio.wait_for(client.send("add", {"sku": "a"}), timeout=1)
    for _ in range(3):
        await asyncio.sleep(0)

    assert invoked == [
        {
            "actor_name": "Cart",
            "key": "user-1",
            "operation": "add",
            "payload": b'{"sku": "a"}',
            "timeout_ms": None,
        }
    ]
    release.set()


async def test_the_client_hands_its_connection_timeout_to_the_native_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orcher import _native
    from orcher.client.client import Client
    from orcher.client.config import ClientConfig

    configs: list[Any] = []

    class FakeNativeClient:
        async def connect(self, config: Any) -> None:
            configs.append(config)

    monkeypatch.setattr(_native, "Client", FakeNativeClient)
    client = Client(
        ClientConfig(server_url="http://localhost:1", connection_timeout=timedelta(seconds=2.5))
    )
    await client.connect()

    assert configs[0].connect_timeout_ms == 2500


def test_the_next_timer_is_the_earliest_pending_one() -> None:
    # It returned the first pending timer in heap-list order, which is not
    # the earliest once the heap's head is cancelled.
    from orcher.testing.time_controller import TimeController

    controller = TimeController(datetime(2026, 1, 1))
    first = controller.create_timer(10_000, lambda: None)
    controller.create_timer(30_000, lambda: None)
    controller.create_timer(20_000, lambda: None)
    controller.cancel_timer(first)

    assert controller.get_next_timer_time() == datetime(2026, 1, 1, 0, 0, 20)


def test_a_description_reads_its_times_like_a_listing() -> None:
    from orcher.types import WorkflowExecutionDescription, WorkflowExecutionInfo

    started = {"secs_since_epoch": 1_700_000_000, "nanos_since_epoch": 500_000_000}
    description = WorkflowExecutionDescription._from_dict({"startTime": started})
    listing = WorkflowExecutionInfo._from_dict({"startTime": started})

    assert description.start_time == listing.start_time
    assert description.start_time is not None
    assert description.close_time is None
