"""A task receives its input as a workflow does.

A task's input becomes call arguments the way a workflow's does: an object is
spread as keyword arguments, several positional arguments arrive packed, and
any other value, a list included, is the one argument. The worker used to
spread only objects, so a task given a list or a scalar, as a workflow written
in another language can give it, failed before its code ran.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from orcher import TaskContext, task
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker


@task(name="takes-one-value")
async def takes_one_value(ctx: TaskContext, value: Any) -> Any:
    return {"value": value}


@task(name="takes-several")
async def takes_several(ctx: TaskContext, name: str, count: int, gift: bool) -> Any:
    return {"name": name, "count": count, "gift": gift}


class FakeHost:
    """The worker host calls a task execution makes: no cancellation ever comes."""

    def heartbeat(self, heartbeat: bytes) -> bytes:
        return b""  # an encoded HeartbeatAnswer asking nothing

    async def task_cancelled(self, token: bytes) -> bool:
        await asyncio.Event().wait()
        return False


def _worker() -> Worker:
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._host = FakeHost()
    worker._task_handlers["takes-one-value"] = takes_one_value.__orcher_task__
    worker._task_handlers["takes-several"] = takes_several.__orcher_task__
    return worker


def _request(task_type: str, task_input: Any) -> dict[str, Any]:
    return {
        "task_id": f"{task_type}_1",
        "task_type": task_type,
        "task_token": b"token",
        "workflow_id": "wf",
        "execution_id": "run",
        "task_queue": "q",
        "input": task_input,
        "attempt": 1,
    }


@pytest.mark.parametrize(
    "value",
    [["a", 1, 0, False, "", None, [2], {}], [], [1], 0, False, "", "text", 3.5],
)
async def test_a_value_that_is_not_an_object_is_the_one_argument(value: Any) -> None:
    result = await _worker()._execute_task(_request("takes-one-value", value))
    assert result == {"value": value}


async def test_an_object_is_spread_as_keyword_arguments() -> None:
    result = await _worker()._execute_task(
        _request("takes-several", {"name": "widget", "count": 0, "gift": False})
    )
    assert result == {"name": "widget", "count": 0, "gift": False}


async def test_several_positional_arguments_arrive_one_to_one() -> None:
    result = await _worker()._execute_task(
        _request("takes-several", {"__orcher_args__": ["widget", 0, False]})
    )
    assert result == {"name": "widget", "count": 0, "gift": False}
