"""A request to cancel a workflow reaches its code once, at the right wait.

The first wait whose result the journal did not record before the request
raises the cancellation; waits after it run normally, so cleanup is scheduled
and completes. Which wait is decided by the journal's recorded times, so a
replay delivers it at the same place as the original run.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from orcher import TaskContext, WorkflowContext, WorkflowInfo, task
from orcher.errors import ErrorCode, WorkflowError, WorkflowSuspendedError


@task(name="cancel-work")
async def work(ctx: TaskContext, n: int) -> int:
    return n


@task(name="cancel-refund")
async def refund(ctx: TaskContext, n: int) -> int:
    return n


def make_ctx() -> WorkflowContext:
    info = WorkflowInfo(
        workflow_id="wf",
        run_id="run",
        workflow_type="T",
        task_queue="q",
        namespace="default",
        attempt=1,
        started_at=datetime(2026, 1, 1),
    )
    return WorkflowContext(info, replaying=True)


def asked_to_cancel() -> WorkflowContext:
    """The first step, `cancel-work_1`, completed at 1000 ms; the request came at
    2000 ms."""
    ctx = make_ctx()
    ctx._cached_results["task:cancel-work_1"] = 1
    ctx._resolved_at["cancel-work_1"] = 1_000
    ctx._note_cancel_requested(2_000)
    return ctx


def is_cancellation(error: BaseException) -> bool:
    return isinstance(error, WorkflowError) and error.code == ErrorCode.WORKFLOW_CANCELLED


async def test_a_result_from_before_the_request_is_received() -> None:
    ctx = asked_to_cancel()
    assert await ctx.execute_task(work, n=0) == 1
    assert not ctx.is_cancel_requested()


async def test_the_first_wait_without_an_earlier_result_is_told_once() -> None:
    ctx = asked_to_cancel()
    await ctx.execute_task(work, n=0)

    with pytest.raises(WorkflowError) as told:
        await ctx.execute_task(work, n=1)
    assert is_cancellation(told.value)
    assert ctx.is_cancel_requested()

    # Cleanup: the next wait is scheduled as usual, not cancelled again.
    with pytest.raises(WorkflowSuspendedError):
        await ctx.execute_task(refund, n=2)


async def test_the_same_wait_is_told_on_every_replay() -> None:
    """The interrupted step may have a result by now, recorded after the
    request; it is still where the request is delivered."""
    ctx = asked_to_cancel()
    ctx._cached_results["task:cancel-work_2"] = 2
    ctx._resolved_at["cancel-work_2"] = 3_000
    await ctx.execute_task(work, n=0)

    with pytest.raises(WorkflowError) as told:
        await ctx.execute_task(work, n=1)
    assert is_cancellation(told.value)


async def test_a_result_recorded_with_the_request_is_received() -> None:
    ctx = make_ctx()
    ctx._cached_results["task:cancel-work_1"] = 1
    ctx._resolved_at["cancel-work_1"] = 2_000
    ctx._note_cancel_requested(2_000)
    assert await ctx.execute_task(work, n=0) == 1


async def test_without_a_request_nothing_is_cancelled() -> None:
    ctx = make_ctx()
    with pytest.raises(WorkflowSuspendedError):
        await ctx.execute_task(work, n=0)
    assert not ctx.is_cancel_requested()


async def test_a_sleep_whose_timer_had_not_fired_is_told() -> None:
    ctx = make_ctx()
    ctx._note_cancel_requested(2_000)
    with pytest.raises(WorkflowError) as told:
        await ctx.sleep(timedelta(minutes=10))
    assert is_cancellation(told.value)


async def test_an_event_that_came_before_the_request_is_received() -> None:
    ctx = make_ctx()
    ctx._cached_results["event_buffer:go"] = [list(json.dumps(7).encode())]
    ctx._cached_results["event_positions:go"] = [0]
    ctx._event_times["go"] = [1_000]
    ctx._note_cancel_requested(2_000)

    assert await ctx.wait_for_event("go") == 7
    with pytest.raises(WorkflowError) as told:
        await ctx.wait_for_event("go")
    assert is_cancellation(told.value)
