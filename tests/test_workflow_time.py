"""Workflow time is read from the journal, never from the machine's clock.

It starts when the engine journaled the workflow's start and moves, as the
workflow receives a task result, a fired timer, a child's outcome or an event,
to when the engine journaled that. A closure does not move it: the closure ran
inside the activation, before its result was journaled. A replay therefore
reads the same time at each point in the code, on any machine and however
much later it runs.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from orcher import TaskContext, WorkflowContext, task, workflow
from tests.support.fake_engine import START_MS, FakeEngine


@task(name="time-step")
async def step(ctx: TaskContext) -> str:
    return "ok"


def at(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


# What each activation read, point by point.
readings: list[list[tuple[str, datetime]]] = []


@workflow(name="time-readings")
async def time_readings(ctx: WorkflowContext) -> str:
    seen: list[tuple[str, datetime]] = []
    readings.append(seen)
    seen.append(("start", ctx.time.now()))
    await ctx.execute_task(step)
    seen.append(("after task", ctx.time.now()))
    await ctx.execute("closure", lambda: _value("c"))
    seen.append(("after closure", ctx.time.now()))
    await ctx.sleep(timedelta(minutes=1))
    seen.append(("after timer", ctx.time.now()))
    await ctx.wait_for_event("go")
    seen.append(("after event", ctx.time.now()))
    return "done"


async def _value(v: str) -> str:
    return v


async def test_time_comes_from_the_journal_and_replays_identically() -> None:
    readings.clear()
    engine = FakeEngine(time_readings)

    await engine.activate()
    engine.advance(7_000)
    engine.complete_task("time-step_1", "ok")
    engine.advance(5_000)  # the activation runs later than the result was journaled
    await engine.activate()
    engine.advance(60_000)
    engine.fire_timer("timer_3")
    await engine.activate()
    engine.advance(9_000)
    engine.send_event("go", None)
    engine.advance(100_000)
    await engine.activate()
    assert engine.completed == "done"

    task_at, timer_at, event_at = START_MS + 7_000, START_MS + 72_000, START_MS + 81_000
    assert readings[-1] == [
        ("start", at(START_MS)),
        ("after task", at(task_at)),
        ("after closure", at(task_at)),
        ("after timer", at(timer_at)),
        ("after event", at(event_at)),
    ]
    # Every activation read the same time at each point it reached.
    for seen in readings:
        assert seen == readings[-1][: len(seen)]


@workflow(name="time-never-backwards")
async def never_backwards(ctx: WorkflowContext) -> list[datetime]:
    seen = []
    await ctx.execute_task(step)
    seen.append(ctx.time.now())
    await ctx.execute_task(step)
    seen.append(ctx.time.now())
    return [s.isoformat() for s in seen]


async def test_time_never_moves_backwards() -> None:
    engine = FakeEngine(never_backwards)
    await engine.activate()
    engine.advance(10_000)
    engine.complete_task("time-step_1", "ok")
    await engine.activate()
    # The second result was journaled with an earlier time than the first:
    # receiving it leaves the clock where it was.
    engine.journal.append(
        ("step", {"step_name": "time-step_2", "step_type": 1, "result": list(b'"ok"'),
                  "failure": None}, START_MS + 3_000)
    )
    await engine.activate()
    assert engine.completed == [at(START_MS + 10_000).isoformat()] * 2


async def test_the_start_time_and_elapsed_are_journal_times() -> None:
    @workflow(name="time-elapsed")
    async def elapsed(ctx: WorkflowContext) -> list[float]:
        await ctx.execute_task(step)
        return [ctx.time.started_at().timestamp(), ctx.time.elapsed().total_seconds()]

    engine = FakeEngine(elapsed)
    await engine.activate()
    engine.advance(42_000)
    engine.complete_task("time-step_1", "ok")
    engine.advance(1_000_000)
    await engine.activate()
    assert engine.completed == [START_MS / 1000, 42.0]
