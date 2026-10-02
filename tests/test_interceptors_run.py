"""Interceptors registered on a worker run around the work it executes.

The worker used to call only the on_enter/on_exit/on_success/on_error hooks and
never an interceptor's intercept_execute or on_retry, where the built-in
logging, metrics and tracing interceptors do their work, so registering them
had no effect.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from orcher import TaskContext, WorkflowContext, task, workflow
from orcher.interceptors.base import (
    ExecutionInfo,
    InterceptorContext,
    NextFn,
    TaskInterceptor,
    WorkflowInterceptor,
)
from orcher.interceptors.builtin.logging import LoggingInterceptor
from orcher.interceptors.builtin.metrics import InMemoryMetricsCollector, MetricsInterceptor
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker
from tests.support.fake_engine import FakeEngine


@task(name="intercepted-double", retry_policy={"max_attempts": 4})
async def double(ctx: TaskContext, n: int) -> int:
    return n * 2


class FakeBridge:
    """The bridge calls a task execution makes: no cancellation ever comes."""

    def heartbeat_task(self, token: str, details: Any) -> bool:
        return False

    async def wait_task_cancelled(self, token: str) -> bool:
        await asyncio.Event().wait()
        return False


class RecordingTaskInterceptor(TaskInterceptor):
    def __init__(self) -> None:
        self.seen: list[tuple[str, Any, Any]] = []
        self.retries: list[tuple[int, int]] = []

    async def intercept_execute(
        self, context: InterceptorContext, input_data: Any, next_fn: NextFn
    ) -> Any:
        result = await next_fn(input_data)
        self.seen.append((context.task_name, input_data, result))
        return result

    async def on_retry(
        self, context: InterceptorContext, info: ExecutionInfo, attempt: int, max_attempts: int
    ) -> None:
        self.retries.append((attempt, max_attempts))


def _worker(**kwargs: Any) -> Worker:
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"), **kwargs)
    worker._bridge_worker = FakeBridge()
    worker._task_handlers["intercepted-double"] = double.__orcher_task__
    return worker


def _task_request(attempt: int = 1) -> dict[str, Any]:
    return {
        "task_id": "intercepted-double_1",
        "task_type": "intercepted-double",
        "task_token": "dG9rZW4=",
        "workflow_id": "wf",
        "execution_id": "run",
        "task_queue": "q",
        "input": {"n": 21},
        "attempt": attempt,
    }


async def test_a_task_interceptor_wraps_the_task() -> None:
    recording = RecordingTaskInterceptor()
    worker = _worker(task_interceptors=[recording])

    assert await worker._execute_task(_task_request()) == 42
    assert recording.seen == [("intercepted-double", {"n": 21}, 42)]
    assert recording.retries == []


async def test_a_retried_task_is_reported_to_on_retry() -> None:
    recording = RecordingTaskInterceptor()
    worker = _worker(task_interceptors=[recording])

    await worker._execute_task(_task_request(attempt=3))
    assert recording.retries == [(3, 4)]


async def test_the_builtin_logging_interceptor_logs_a_task(
    caplog: pytest.LogCaptureFixture,
) -> None:
    worker = _worker(task_interceptors=[LoggingInterceptor().task()])

    with caplog.at_level(logging.INFO, logger="orcher"):
        await worker._execute_task(_task_request())

    messages = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("Task started: intercepted-double") for m in messages)
    assert any(m.startswith("Task completed: intercepted-double") for m in messages)


@workflow(name="intercepted-workflow")
async def intercepted_workflow(ctx: WorkflowContext, n: int) -> int:
    return await ctx.execute_task(double, n=n)


class RecordingWorkflowInterceptor(WorkflowInterceptor):
    def __init__(self) -> None:
        self.seen: list[tuple[str, Any]] = []

    async def intercept_execute(
        self, context: InterceptorContext, input_data: Any, next_fn: NextFn
    ) -> Any:
        self.seen.append((context.workflow_type, input_data))
        return await next_fn(input_data)


async def test_workflow_interceptors_wrap_each_activation() -> None:
    recording = RecordingWorkflowInterceptor()
    collector = InMemoryMetricsCollector()
    worker = _worker(
        workflow_interceptors=[recording, MetricsInterceptor(collector).workflow()]
    )
    engine = FakeEngine(intercepted_workflow, 5, worker=worker)

    await engine.activate()
    engine.complete_task("intercepted-double_1", 10)
    await engine.activate()

    assert engine.completed == 10
    assert recording.seen == [("intercepted-workflow", 5), ("intercepted-workflow", 5)]
    # The first activation suspended: it neither completed nor failed.
    assert collector.get_counter_total("workflow.started") == 2
    assert collector.get_counter_total("workflow.completed") == 1
    assert collector.get_counter_total("workflow.failed") == 0
