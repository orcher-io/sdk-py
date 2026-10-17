"""A request to cancel a workflow reaches its code once, at the right wait.

The first wait whose result does not come before the request raises the
cancellation; waits after it run normally, so cleanup is scheduled and
completes. Which wait is decided by the run's order of outcomes (the kernel's
resolution sequence), never by recorded times, which tie within a millisecond,
so a replay delivers it at the same place as the original run.
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
    """The first step, `cancel-work_1`, completed (the run's second outcome);
    then the request came (its third)."""
    ctx = make_ctx()
    ctx._cached_results["task:cancel-work_1"] = 1
    ctx._resolved_order["cancel-work_1"] = 2
    ctx._note_cancel_requested(3)
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
    ctx._resolved_order["cancel-work_2"] = 4
    await ctx.execute_task(work, n=0)

    with pytest.raises(WorkflowError) as told:
        await ctx.execute_task(work, n=1)
    assert is_cancellation(told.value)


def asked_in_the_same_millisecond(result_order: int, request_order: int) -> WorkflowContext:
    ctx = make_ctx()
    ctx._cached_results["task:cancel-work_1"] = 1
    ctx._resolved_at["cancel-work_1"] = 2_000
    ctx._resolved_order["cancel-work_1"] = result_order
    ctx._note_cancel_requested(request_order)
    return ctx


async def test_a_result_in_the_requests_millisecond_is_ordered_by_sequence() -> None:
    """A result and the request recorded in the same millisecond: the run's
    order of outcomes says which came first, never their times."""
    before = asked_in_the_same_millisecond(2, 3)
    assert await before.execute_task(work, n=0) == 1
    assert not before.is_cancel_requested()

    after = asked_in_the_same_millisecond(3, 2)
    with pytest.raises(WorkflowError) as told:
        await after.execute_task(work, n=0)
    assert is_cancellation(told.value)
    # Cleanup runs.
    with pytest.raises(WorkflowSuspendedError):
        await after.execute_task(refund, n=1)


async def test_a_result_with_no_known_place_is_received() -> None:
    ctx = make_ctx()
    ctx._cached_results["task:cancel-work_1"] = 1
    ctx._note_cancel_requested(2)
    assert await ctx.execute_task(work, n=0) == 1


async def test_a_second_request_does_not_move_the_first() -> None:
    ctx = make_ctx()
    ctx._cached_results["task:cancel-work_1"] = 1
    ctx._resolved_order["cancel-work_1"] = 3
    ctx._note_cancel_requested(2)
    ctx._note_cancel_requested(4)
    with pytest.raises(WorkflowError) as told:
        await ctx.execute_task(work, n=0)
    assert is_cancellation(told.value)


async def test_without_a_request_nothing_is_cancelled() -> None:
    ctx = make_ctx()
    with pytest.raises(WorkflowSuspendedError):
        await ctx.execute_task(work, n=0)
    assert not ctx.is_cancel_requested()


async def test_a_sleep_whose_timer_had_not_fired_is_told() -> None:
    ctx = make_ctx()
    ctx._note_cancel_requested(2)
    with pytest.raises(WorkflowError) as told:
        await ctx.sleep(timedelta(minutes=10))
    assert is_cancellation(told.value)


async def test_an_event_that_came_before_the_request_is_received() -> None:
    ctx = make_ctx()
    ctx._cached_results["event_buffer:go"] = [list(json.dumps(7).encode())]
    ctx._cached_results["event_positions:go"] = [0]
    ctx._event_times["go"] = [1_000]
    ctx._event_orders["go"] = [2]
    ctx._note_cancel_requested(3)

    assert await ctx.wait_for_event("go") == 7
    with pytest.raises(WorkflowError) as told:
        await ctx.wait_for_event("go")
    assert is_cancellation(told.value)


async def test_an_event_that_came_after_the_request_is_where_it_is_told() -> None:
    ctx = make_ctx()
    ctx._cached_results["event_buffer:go"] = [list(json.dumps(7).encode())]
    ctx._cached_results["event_positions:go"] = [2]
    ctx._event_times["go"] = [2_000]
    ctx._event_orders["go"] = [3]
    ctx._note_cancel_requested(2)

    with pytest.raises(WorkflowError) as told:
        await ctx.wait_for_event("go")
    assert is_cancellation(told.value)
