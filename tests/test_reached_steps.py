"""The steps an activation reports it reached.

sdk-core checks them against the steps the journal recorded: a recorded step
the code did not reach, in an activation that issues new work or ends the
workflow, means the code no longer replays the run, and the activation is
tried again instead of applied. The fake engine holds every activation to
that rule, so a step reached and not reported fails any test that replays it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from orcher import TaskContext, WorkflowContext, task, workflow
from tests.support.fake_engine import FakeEngine, NonDeterministicError


@task(name="applyStatus")
async def apply_status(ctx: TaskContext, round: int, state: str | None) -> str:
    return "applied"


@task(name="escalate")
async def escalate(ctx: TaskContext, round: int) -> str:
    return "escalated"


def status_watch(name: str, quiet_cap: int) -> Any:
    """Wait for a `status` event with a deadline, apply it (or nothing) with
    a task, and escalate after `quiet_cap` rounds in a row with no event."""

    @workflow(name=name)
    async def watch(ctx: WorkflowContext) -> str:
        quiet = 0
        round = 0
        while True:
            round += 1
            try:
                ev = await ctx.wait_for_event_with_timeout("status", timedelta(seconds=4))
            except TimeoutError:
                ev = None
            state = ev.get("state") if isinstance(ev, dict) else None
            await ctx.execute_task(apply_status, round=round, state=state)
            if state == "paid":
                return f"paid at round {round}"
            quiet = 0 if ev else quiet + 1
            if quiet >= quiet_cap:
                await ctx.execute_task(escalate, round=round)
                return f"escalated at round {round}"

    return watch


async def quiet_rounds(engine: FakeEngine, rounds: int) -> None:
    """Journal `rounds` rounds whose deadline passed with no event."""
    for n in range(1, rounds + 1):
        await engine.activate()
        engine.fire_timer(f"event_timeout_status_{n}")
        await engine.activate()
        engine.complete_task(f"applyStatus_{n}", "applied")


async def test_every_step_reached_is_reported_in_order() -> None:
    engine = FakeEngine(status_watch("watch-report", 5))
    await quiet_rounds(engine, 2)
    activation = await engine.activate()
    assert activation.result["reached_steps"] == [
        "event_timeout_status_1",
        "applyStatus_1",
        "event_timeout_status_2",
        "applyStatus_2",
        "event_timeout_status_3",
    ]


async def test_a_loop_cut_short_is_refused_where_the_journal_goes_on() -> None:
    old = FakeEngine(status_watch("watch-old", 5))
    await quiet_rounds(old, 3)

    # The same journal, replayed by code that gives up after one quiet round:
    # it takes round one from the journal and schedules the escalation where
    # the journal holds round two.
    new = FakeEngine(status_watch("watch-new", 1), worker=old.worker)
    new.journal = list(old.journal)
    new.issued = dict(old.issued)
    with pytest.raises(NonDeterministicError) as refused:
        await new.activate()
    assert str(refused.value) == (
        "recorded event_timeout_status_2, applyStatus_2, event_timeout_status_3, "
        "applyStatus_3 not reached, and the code issued escalate_2"
    )

    # The old code carries the same run on.
    activation = await old.activate()
    assert activation.steps == [("StartTimer", "event_timeout_status_4")]


async def test_events_left_unread_and_deadlines_beaten_are_not_held_against_the_code() -> None:
    engine = FakeEngine(status_watch("watch-events", 1))
    await engine.activate()
    # The event beats the deadline; two more arrive than the code will read.
    engine.send_event("status", {"state": "pending"})
    engine.send_event("status", {"state": "pending"})
    engine.send_event("other", {"anything": True})
    await engine.activate()
    engine.complete_task("applyStatus_1", "applied")
    # The first round's deadline fires late, after its event had won.
    engine.fire_timer("event_timeout_status_1")
    await engine.activate()
    engine.complete_task("applyStatus_2", "applied")
    await engine.activate()
    engine.send_event("status", {"state": "paid"})
    await engine.activate()
    engine.complete_task("applyStatus_3", "applied")
    await engine.activate()
    assert engine.completed == "paid at round 3"
    assert engine.drift == []
