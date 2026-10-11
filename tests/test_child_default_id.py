"""A child started without an id is named after its parent.

The default id was ``child_{sequence}``, which every parent shared: the first
child of every parent was ``child_1``, so an event sent to a child by its
workflow id could reach another parent's child. It is now
``{parent workflow id}-child-{sequence}``, the same on every replay. A run
that started a child under the old default keeps that child: the journal
holds its id, and the replay finds it there rather than starting another.
"""

from __future__ import annotations

from orcher import TaskContext, WorkflowContext, task, workflow
from tests.support.fake_engine import FakeEngine


@task(name="child-id-step")
async def step(ctx: TaskContext) -> str:
    return "step"


@workflow(name="child-id-one-child")
async def one_child(ctx: WorkflowContext) -> object:
    child = await ctx.start_child_workflow("charge")
    await child.send_event("go", {"ok": True})
    return await child.result()


@workflow(name="child-id-task-then-child")
async def task_then_child(ctx: WorkflowContext) -> object:
    await ctx.execute_task(step)
    return await ctx.execute_child_workflow("charge")


@workflow(name="child-id-two-children-then-a-task")
async def two_children_then_a_task(ctx: WorkflowContext) -> object:
    first = await ctx.execute_child_workflow("charge")
    second = await ctx.execute_child_workflow("charge")
    task_result = await ctx.execute_task(step)
    return [first, second, task_result]


async def test_children_of_different_parents_get_different_default_ids() -> None:
    a = FakeEngine(one_child, workflow_id="order-a")
    b = FakeEngine(one_child, workflow_id="order-b")

    first_a = await a.activate()
    first_b = await b.activate()

    assert first_a.steps == [("StartChildWorkflow", "order-a-child-1")]
    assert first_b.steps == [("StartChildWorkflow", "order-b-child-1")]
    # Each parent's event goes to its own child.
    assert [e["workflow_id"] for e in a.sent_events] == ["order-a-child-1"]
    assert [e["workflow_id"] for e in b.sent_events] == ["order-b-child-1"]


async def test_a_default_child_id_is_the_same_on_every_replay() -> None:
    engine = FakeEngine(task_then_child, workflow_id="order-a")

    await engine.activate()
    engine.complete_task("child-id-step_1", "step")
    started = await engine.activate()
    assert started.steps == [("StartChildWorkflow", "order-a-child-2")]

    # Replayed while the child runs: the same id, nothing new.
    again = await engine.activate()
    assert again.result["reached_steps"] == ["child-id-step_1", "order-a-child-2"]
    assert {step_id for _, step_id in again.steps} <= {"order-a-child-2"}

    engine.complete_child("order-a-child-2", "charged")
    await engine.activate()
    assert engine.completed == "charged"
    assert engine.drift == []


async def test_a_run_started_under_the_old_default_child_id_replays_to_its_child() -> None:
    # A run an earlier SDK started: its first child is still running under the
    # id every parent shared.
    engine = FakeEngine(two_children_then_a_task, workflow_id="order-a")
    engine.start_child("child_1", "charge")

    # Replayed by this SDK, the run reaches that child rather than starting
    # another under the new default, which sdk-core would hold as
    # non-deterministic.
    resumed = await engine.activate()
    assert resumed.result["reached_steps"] == ["child_1"]
    assert {step_id for _, step_id in resumed.steps} <= {"child_1"}

    # The old child completes; the next child gets the new default.
    engine.complete_child("child_1", "first")
    next_child = await engine.activate()
    assert next_child.steps == [("StartChildWorkflow", "order-a-child-2")]

    engine.complete_child("order-a-child-2", "second")
    await engine.activate()
    engine.complete_task("child-id-step_3", "step")
    await engine.activate()

    assert engine.completed == ["first", "second", "step"]
    assert engine.drift == []
    assert engine.failure is None
