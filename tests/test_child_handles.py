"""Child handles send events and cancel; a child that ends without a result
fails the parent's wait for it.

Sends and cancellations are journaled as steps of the parent, so a replay does
not repeat them. A canceled, terminated or timed-out child used to leave its
parent waiting for a result that would never come.
"""

from __future__ import annotations

import pytest

from orcher import TaskContext, WorkflowContext, task, workflow
from orcher.errors import WorkflowError
from tests.support.fake_engine import FakeEngine


@workflow(name="children-send-and-cancel")
async def send_and_cancel(ctx: WorkflowContext) -> str:
    approver = await ctx.start_child_workflow("approver", args=({"order": 1},))
    await approver.send_event("approve", {"by": "ops"})
    reviewer = await ctx.start_child_workflow("reviewer")
    await reviewer.cancel()
    return await approver.result()


async def test_a_child_handle_sends_an_event_and_cancels_once_across_replays() -> None:
    engine = FakeEngine(send_and_cancel)

    first = await engine.activate()
    assert engine.failure is None, engine.failure
    assert first.steps == [
        ("StartChildWorkflow", "child_1"),
        ("StartChildWorkflow", "child_3"),
    ]

    engine.complete_child("child_1", "approved")
    await engine.activate()

    assert engine.completed == "approved"
    assert engine.sent_events == [
        {
            "workflow_id": "child_1",
            "run_id": None,
            "event_name": "approve",
            "payload": {"data": list(b'{"by": "ops"}'), "metadata": {}},
            "headers": [],
        }
    ]
    assert engine.cancelled_children == ["child_3"]
    assert engine.drift == []


@pytest.mark.parametrize(
    ("how", "reason", "message"),
    [
        ("canceled", "", "child workflow was canceled"),
        ("terminated", "operator", "child workflow was terminated: operator"),
        ("timed_out", "", "child workflow timed out"),
    ],
)
async def test_a_child_that_ends_without_a_result_fails_the_wait(
    how: str, reason: str, message: str
) -> None:
    @workflow(name=f"children-ended-{how}")
    async def parent(ctx: WorkflowContext) -> str:
        child = await ctx.start_child_workflow("worker")
        try:
            return await child.result()
        except WorkflowError as e:
            return f"failed: {e.message}"

    engine = FakeEngine(parent)
    await engine.activate()
    engine.end_child("child_1", how, reason)
    await engine.activate()

    assert engine.completed == f"failed: {message}"


async def test_execute_child_workflow_fails_for_a_child_that_ended_without_a_result() -> None:
    @workflow(name="children-execute-canceled")
    async def parent(ctx: WorkflowContext) -> str:
        try:
            return await ctx.execute_child_workflow("worker")
        except WorkflowError as e:
            return f"failed: {e.message}"

    engine = FakeEngine(parent)
    await engine.activate()
    engine.end_child("child_1", "canceled")
    await engine.activate()

    assert engine.completed == "failed: child workflow was canceled"


@task(name="children-step")
async def step(ctx: TaskContext) -> str:
    return "ok"


@workflow(name="children-send-to-any-workflow")
async def send_to_any(ctx: WorkflowContext) -> str:
    await ctx.send_event("billing-7", "invoice", {"n": 1})
    await ctx.execute_task(step)
    return await ctx.wait_for_event("paid")


async def test_an_event_to_another_workflow_is_sent_once_across_replays() -> None:
    engine = FakeEngine(send_to_any)
    await engine.activate()
    engine.complete_task("children-step_2", "ok")
    await engine.activate()
    engine.send_event("paid", "yes")
    await engine.activate()

    assert engine.completed == "yes"
    assert [e["workflow_id"] for e in engine.sent_events] == ["billing-7"]


@workflow(name="children-final-activation-commands")
async def final_activation(ctx: WorkflowContext) -> str:
    await ctx.send_event("audit", "closed", None)
    return "done"


async def test_what_the_final_activation_issues_reaches_the_engine() -> None:
    engine = FakeEngine(final_activation)
    activation = await engine.activate()

    kinds = [next(iter(c)) for c in activation.commands]
    assert kinds[-1] == "CompleteWorkflow"
    assert [e["workflow_id"] for e in engine.sent_events] == ["audit"]
