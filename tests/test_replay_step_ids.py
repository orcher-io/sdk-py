"""A replay issues every step under the id the run first gave it.

Step ids come from one per-run counter that every step (task, timer, child,
closure, session) draws exactly one number from on every path. Waits for
events are not engine steps and draw none: when a wait drew one, a step
issued beside it was numbered differently once the event had arrived, and the
replay issued it again under a new id.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from orcher import TaskContext, WorkflowContext, task, workflow
from tests.support.fake_engine import FakeEngine


@task(name="ids-first")
async def first(ctx: TaskContext) -> str:
    return "first"


@task(name="ids-second")
async def second(ctx: TaskContext) -> str:
    return "second"


@workflow(name="ids-wait-beside-a-timer")
async def wait_beside_timer(ctx: WorkflowContext) -> object:
    async def approval() -> object:
        await ctx.execute_task(first)
        return await ctx.wait_for_event("go")

    async def cooling_off() -> str:
        await ctx.sleep(timedelta(minutes=5))
        return await ctx.execute_task(second)

    return list(await asyncio.gather(approval(), cooling_off()))


async def test_a_wait_takes_no_step_number() -> None:
    engine = FakeEngine(wait_beside_timer)

    first_activation = await engine.activate()
    assert first_activation.steps == [("ScheduleTask", "ids-first_1"), ("StartTimer", "timer_2")]

    engine.complete_task("ids-first_1", "first")
    await engine.activate()
    engine.fire_timer("timer_2")
    await engine.activate()
    engine.complete_task("ids-second_3", "second")
    await engine.activate()
    engine.send_event("go", {"ok": True})
    await engine.activate()

    assert engine.completed == [{"ok": True}, "second"]
    assert engine.drift == []
    assert set(engine.issued) == {"ids-first_1", "timer_2", "ids-second_3"}


async def test_a_timed_wait_names_its_deadline_by_event_and_count() -> None:
    @workflow(name="ids-timed-waits")
    async def timed_waits(ctx: WorkflowContext) -> object:
        got = []
        for _ in range(2):
            try:
                got.append(await ctx.wait_for_event_with_timeout("ping", timedelta(seconds=30)))
            except TimeoutError:
                got.append("timed out")
        got.append(await ctx.execute_task(first))
        return got

    engine = FakeEngine(timed_waits)
    first_activation = await engine.activate()
    assert first_activation.steps == [("StartTimer", "event_timeout_ping_1")]

    engine.fire_timer("event_timeout_ping_1")
    second_activation = await engine.activate()
    assert second_activation.steps == [("StartTimer", "event_timeout_ping_2")]

    engine.send_event("ping", 1)
    third = await engine.activate()
    # The waits drew no number, so the task is the run's first step.
    assert third.steps == [("ScheduleTask", "ids-first_1")]
    engine.complete_task("ids-first_1", "first")
    await engine.activate()
    assert engine.completed == ["timed out", 1, "first"]
    assert engine.drift == []
