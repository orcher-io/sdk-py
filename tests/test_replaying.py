"""``ctx.replaying`` says whether the code is re-running what an earlier
activation of the run already ran.

It is False throughout a run's first activation, and in a later one it turns
False where the code reaches work the history has no outcome for, so a point in
the code reads as new exactly once: on the activation that first reaches it.
"""

from __future__ import annotations

from datetime import timedelta

from orcher import TaskContext, WorkflowContext, task, workflow
from orcher.workflow.context import WorkflowContext as Context
from orcher.workflow.context import WorkflowInfo
from tests.support.fake_engine import FakeEngine

readings: list[tuple[str, bool]] = []


@task(name="replaying-step-a")
async def step_a(ctx: TaskContext) -> str:
    return "a"


@workflow(name="replaying-points")
async def points(ctx: WorkflowContext) -> bool:
    readings.append(("start", ctx.replaying))
    await ctx.execute_task(step_a)
    readings.append(("after task", ctx.replaying))
    await ctx.sleep(timedelta(seconds=60))
    readings.append(("after timer", ctx.replaying))
    await ctx.execute("closure_c", lambda: 1)
    readings.append(("after closure", ctx.replaying))
    return True


async def test_replaying_reports_each_point_as_new_exactly_once() -> None:
    readings.clear()
    engine = FakeEngine(points)

    # Activation 1 starts the run; 2 delivers the task's result; 3 the timer's.
    first = await engine.activate()
    task_id = next(step for kind, step in first.steps if kind == "ScheduleTask")
    engine.complete_task(task_id, "a")
    second = await engine.activate()
    timer_id = next(step for kind, step in second.steps if kind == "StartTimer")
    engine.fire_timer(timer_id)
    await engine.activate()

    assert engine.completed is True
    assert readings == [
        ("start", False),
        ("start", True),
        ("after task", False),
        ("start", True),
        ("after task", True),
        ("after timer", False),
        ("after closure", False),
    ]


def _context() -> Context:
    from datetime import UTC, datetime

    info = WorkflowInfo(
        workflow_id="wf",
        run_id="run",
        workflow_type="t",
        task_queue="q",
        namespace="ns",
        attempt=1,
        started_at=datetime.now(UTC),
    )
    return Context(info=info, replaying=False)


def test_unmarked_outcomes_take_the_newest_as_new() -> None:
    ctx = _context()
    ctx._note_outcome(2, False)
    ctx._note_outcome(5, False)
    assert ctx.replaying is True
    ctx._received_at_order(2)
    assert ctx.replaying is True
    ctx._received_at_order(5)
    assert ctx.replaying is False


def test_marked_outcomes_replay_until_the_first_new_one() -> None:
    ctx = _context()
    ctx._note_outcome(1, True)
    ctx._note_outcome(3, False)
    ctx._note_outcome(4, False)
    ctx._received_at_order(1)
    assert ctx.replaying is True
    # Two outcomes new together: new from the first of them.
    ctx._received_at_order(3)
    assert ctx.replaying is False


def test_an_activation_with_nothing_new_replays_throughout() -> None:
    # An activation for a query: every outcome was seen before.
    ctx = _context()
    ctx._note_outcome(1, True)
    ctx._note_outcome(2, True)
    ctx._received_at_order(2)
    assert ctx.replaying is True


def test_a_context_built_by_hand_says_itself() -> None:
    assert _context().replaying is False
