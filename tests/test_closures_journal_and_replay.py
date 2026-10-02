"""ctx.execute() closures are journaled in the shape sdk-core reads, and run once.

Three things kept a closure from being durable. Its RecordStepResult carried a
step type name and a bare value where sdk-core deserializes an enum number,
bytes and an attempt, so the whole activation was refused. Its replay looked
for the result under a key the journal never fills. And a closure still
running when a step beside it suspended the workflow was dropped, so the next
activation ran it again.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from orcher import TaskContext, WorkflowContext, task, workflow
from tests.support.fake_engine import FakeEngine


@task(name="closures-echo")
async def echo(ctx: TaskContext, value: object) -> object:
    return value


calls: dict[str, int] = {}


def counted(name: str, value: object, delay: float = 0.0) -> Callable[[], Awaitable[object]]:
    async def run() -> object:
        calls[name] = calls.get(name, 0) + 1
        if delay:
            await asyncio.sleep(delay)
        return value

    return run


@workflow(name="closures-then-task")
async def closure_then_task(ctx: WorkflowContext) -> object:
    user = await ctx.execute("fetch_user", counted("fetch_user", {"id": 7}))
    echoed = await ctx.execute_task(echo, value=user)
    return {"user": user, "echoed": echoed}


async def test_a_closure_is_journaled_and_not_run_again_on_replay() -> None:
    calls.clear()
    engine = FakeEngine(closure_then_task)

    first = await engine.activate()
    assert engine.failure is None, engine.failure
    assert engine.closure_records == ["fetch_user_1"]
    assert first.steps == [("ScheduleTask", "closures-echo_2")]

    engine.complete_task("closures-echo_2", {"id": 7})
    await engine.activate()

    assert engine.is_completed
    assert engine.completed == {"user": {"id": 7}, "echoed": {"id": 7}}
    assert calls["fetch_user"] == 1
    assert engine.drift == []


@workflow(name="closures-beside-a-suspending-step")
async def closure_beside_task(ctx: WorkflowContext) -> object:
    slow, echoed = await asyncio.gather(
        ctx.execute("slow", counted("slow", "done", delay=0.05)),
        ctx.execute_task(echo, value="x"),
    )
    return [slow, echoed]


async def test_a_closure_running_when_the_workflow_suspends_is_journaled_with_it() -> None:
    calls.clear()
    engine = FakeEngine(closure_beside_task)

    await engine.activate()
    assert engine.closure_records == ["slow_1"]

    engine.complete_task("closures-echo_2", "x")
    await engine.activate()

    assert engine.completed == ["done", "x"]
    assert calls["slow"] == 1
    assert engine.drift == []


@workflow(name="closures-in-a-loop")
async def closures_in_a_loop(ctx: WorkflowContext) -> object:
    seen = []
    for i in range(3):
        seen.append(await ctx.execute("draw", counted(f"draw-{i}", i)))
    await ctx.execute_task(echo, value=seen)
    return seen


async def test_each_run_of_a_named_closure_has_its_own_step() -> None:
    calls.clear()
    engine = FakeEngine(closures_in_a_loop)

    await engine.activate()
    assert engine.closure_records == ["draw_1", "draw_2", "draw_3"]

    engine.complete_task("closures-echo_4", [0, 1, 2])
    await engine.activate()

    assert engine.completed == [0, 1, 2]
    assert [calls[f"draw-{i}"] for i in range(3)] == [1, 1, 1]
