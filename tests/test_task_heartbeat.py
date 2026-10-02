"""A task's own heartbeats reach the worker, which heartbeats every task on its own.

``TaskContext.heartbeat`` must actually send. Otherwise a task's progress details
never leave the process and the engine's request to stop a task never reaches it.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from orcher import TaskContext, TaskInfo


def _info() -> TaskInfo:
    return TaskInfo(
        task_id="task",
        task_type="task",
        workflow_id="wf",
        run_id="run",
        task_queue="q",
        namespace="ns",
        attempt=1,
        scheduled_at=datetime.now(),
        started_at=datetime.now(),
    )


@pytest.mark.asyncio
async def test_details_reach_the_worker_serialized() -> None:
    sent: list[bytes | None] = []
    ctx = TaskContext(_info(), heartbeat_sender=lambda d: sent.append(d) or False)

    await ctx.heartbeat({"processed": 3})
    await ctx.heartbeat()

    assert sent == [json.dumps({"processed": 3}).encode(), None]
    assert not ctx.cancellation_token.is_cancelled


@pytest.mark.asyncio
async def test_an_answer_asking_the_task_to_stop_cancels_it() -> None:
    ctx = TaskContext(_info(), heartbeat_sender=lambda _d: True)

    await ctx.heartbeat()

    assert ctx.cancellation_token.is_cancelled


@pytest.mark.asyncio
async def test_without_a_worker_a_heartbeat_sends_nothing() -> None:
    ctx = TaskContext(_info())
    await ctx.heartbeat({"processed": 1})
    assert not ctx.cancellation_token.is_cancelled
