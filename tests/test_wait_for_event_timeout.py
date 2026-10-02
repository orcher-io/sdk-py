"""wait_for_event_with_timeout: the deadline is a durable timer raced against
the event.

The SDK starts a durable timer beside the wait and, on replay, lets whichever
the journal shows first decide. This guarantees that a workflow waiting for an
event that never comes is not parked forever, and that a replay takes the same
branch as the original run.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from orcher import WorkflowContext, WorkflowInfo
from orcher.errors import WorkflowSuspendedError


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
    return WorkflowContext(info)


def payload(value: object) -> list[int]:
    """An event payload as the worker receives it from the native layer."""
    return list(json.dumps(value).encode())


def buffer_event(ctx: WorkflowContext, name: str, value: object, position: int) -> None:
    ctx._cached_results.setdefault(f"event_buffer:{name}", []).append(payload(value))
    ctx._cached_results.setdefault(f"event_positions:{name}", []).append(position)


def fire_timer(ctx: WorkflowContext, timer_id: str, position: int) -> None:
    ctx._cached_results[f"timer:{timer_id}"] = {"fired_at": position}


async def test_parks_on_the_event_and_starts_a_deadline_timer() -> None:
    ctx = make_ctx()
    with pytest.raises(WorkflowSuspendedError):
        await ctx.wait_for_event_with_timeout("status", timedelta(seconds=5))

    kinds = [next(iter(c)) for c in ctx._commands]
    assert kinds == ["WaitForEvent", "StartTimer"]
    timer = ctx._commands[1]["StartTimer"]
    assert timer["timer_id"] == "event_timeout_status_1"
    assert timer["duration"] == {"secs": 5, "nanos": 0}


async def test_raises_timeout_once_the_deadline_fired() -> None:
    ctx = make_ctx()
    fire_timer(ctx, "event_timeout_status_1", position=3)

    with pytest.raises(TimeoutError):
        await ctx.wait_for_event_with_timeout("status", timedelta(seconds=5))
    assert ctx._commands == []


async def test_takes_an_event_that_arrived_before_the_deadline() -> None:
    ctx = make_ctx()
    buffer_event(ctx, "status", {"early": True}, position=2)
    fire_timer(ctx, "event_timeout_status_1", position=5)

    got = await ctx.wait_for_event_with_timeout("status", timedelta(seconds=5))
    assert got == {"early": True}


async def test_timed_out_wait_stays_timed_out_when_the_event_arrives_later() -> None:
    # Journal: the deadline fired (3), then the event (7). An earlier
    # activation took the timeout branch; the replay must too, and the event
    # belongs to the next wait.
    ctx = make_ctx()
    fire_timer(ctx, "event_timeout_status_1", position=3)
    buffer_event(ctx, "status", {"late": True}, position=7)

    with pytest.raises(TimeoutError):
        await ctx.wait_for_event_with_timeout("status", timedelta(seconds=1))
    got = await ctx.wait_for_event_with_timeout("status", timedelta(seconds=1))
    assert got == {"late": True}


async def test_a_wait_takes_no_step_number_whether_the_event_was_there_or_not() -> None:
    # A wait is not an engine step: it draws no number from the step counter
    # on any path, so the ids of the steps around it do not depend on whether
    # the event had arrived.
    live = make_ctx()
    with pytest.raises(WorkflowSuspendedError):
        await live.wait_for_event_with_timeout("status", timedelta(seconds=1))

    replay = make_ctx()
    buffer_event(replay, "status", {"ok": True}, position=1)
    await replay.wait_for_event_with_timeout("status", timedelta(seconds=1))

    assert live._step_sequence == replay._step_sequence == 0


async def test_events_of_one_name_are_consumed_oldest_first() -> None:
    ctx = make_ctx()
    buffer_event(ctx, "status", {"round": 1}, position=1)
    buffer_event(ctx, "status", {"round": 2}, position=2)

    assert await ctx.wait_for_event_with_timeout("status", timedelta(seconds=1)) == {"round": 1}
    assert await ctx.wait_for_event_with_timeout("status", timedelta(seconds=1)) == {"round": 2}


async def test_rejects_a_zero_timeout() -> None:
    ctx = make_ctx()
    with pytest.raises(ValueError):
        await ctx.wait_for_event_with_timeout("status", timedelta(0))
    assert ctx._commands == []


async def test_wait_without_timeout_starts_no_timer() -> None:
    ctx = make_ctx()
    with pytest.raises(WorkflowSuspendedError):
        await ctx.wait_for_event("status")
    assert [next(iter(c)) for c in ctx._commands] == ["WaitForEvent"]
