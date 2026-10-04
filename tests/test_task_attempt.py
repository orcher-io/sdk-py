"""A task's context says which attempt it is, counted from 1 (#3).

`ctx.attempt` read 0 on every attempt, retries included: the engine sent 0,
and the worker passed it through. It is now the attempt the engine numbers,
and an attempt the engine does not number reads as 1, never 0.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from orcher import TaskContext, task
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker

seen: list[tuple[int, bool]] = []


@task(name="attempt-reporting", retry_policy={"max_attempts": 5})
async def report_attempt(ctx: TaskContext) -> int:
    seen.append((ctx.attempt, ctx.is_retry))
    return ctx.attempt


class FakeBridge:
    """The bridge calls a task execution makes: no cancellation ever comes."""

    def heartbeat_task(self, token: str, details: Any) -> bool:
        return False

    async def wait_task_cancelled(self, token: str) -> bool:
        await asyncio.Event().wait()
        return False


def _worker() -> Worker:
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._bridge_worker = FakeBridge()
    worker._task_handlers["attempt-reporting"] = report_attempt.__orcher_task__
    return worker


def _request(**attempt: Any) -> dict[str, Any]:
    return {
        "task_id": "attempt-reporting_1",
        "task_type": "attempt-reporting",
        "task_token": "dG9rZW4=",
        "workflow_id": "wf",
        "execution_id": "run",
        "task_queue": "q",
        "input": {},
        **attempt,
    }


@pytest.mark.parametrize(
    ("request_attempt", "expected"),
    [
        ({"attempt": 1}, (1, False)),
        ({"attempt": 2}, (2, True)),
        ({"attempt": 3}, (3, True)),
        # What an engine that does not number attempts sends.
        ({"attempt": 0}, (1, False)),
        ({"attempt": None}, (1, False)),
        ({}, (1, False)),
    ],
)
async def test_a_task_sees_its_attempt_counted_from_one(
    request_attempt: dict[str, Any], expected: tuple[int, bool]
) -> None:
    seen.clear()
    result = await _worker()._execute_task(_request(**request_attempt))
    assert seen == [expected]
    assert result == expected[0]
