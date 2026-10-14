"""State an update handler set survives the next activation.

The workflow runs from the start on every activation, so an update's change to
its state must be made again on replay. The worker runs each update the journal
holds again, at the first wait whose result the journal recorded after the
update: where the code was waiting when the update first came.
"""

from __future__ import annotations

import json
from typing import Any

from orcher import TaskContext, WorkflowContext, task, workflow
from orcher.decorators import GlobalRegistry
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker
from tests.support.kernel import activation, payload, query, resolution, run, update


@task(name="update-replay-step")
async def step(ctx: TaskContext) -> int:
    return 1


def set_value(ctx: WorkflowContext, value: Any) -> Any:
    ctx.set_state("value", value)
    return value


def refuse(ctx: WorkflowContext, value: Any) -> None:
    raise ValueError("refused")


async def tracks_value(ctx: WorkflowContext) -> dict[str, Any]:
    ctx.register_update_handler("set_value", set_value)
    ctx.register_update_handler("refuse", refuse)
    ctx.register_query_handler("get_value", lambda c: c.get_state("value"))
    ctx.set_state("value", "initial")
    await ctx.wait_for_event("go")
    after_go = ctx.get_state("value")
    await ctx.execute_task(step)
    return {"after_go": after_go, "at_end": ctx.get_state("value")}


WORKFLOW = "update-replay"


def make_worker() -> Worker:
    # Registered here, not at import: other tests reset the registry.
    registry = GlobalRegistry.get_instance()
    if registry.get_workflow(WORKFLOW) is None:
        workflow(name=WORKFLOW)(tracks_value)
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._workflow_handlers[WORKFLOW] = registry.get_workflow(WORKFLOW)
    return worker


def journaled(update_id: str, value: Any, at_ms: int) -> dict[str, Any]:
    """An update the engine answered already, recorded at ``at_ms``."""
    return update(update_id, "set_value", value, at_ms=at_ms)


def request(*resolutions: dict[str, Any], queries: bool = False) -> dict[str, Any]:
    return activation(
        WORKFLOW,
        None,
        *resolutions,
        queries=[query("q1", "get_value")] if queries else None,
        start_ms=0,
    )


def answer(response: dict[str, Any]) -> Any:
    (q,) = response["query_responses"]
    return json.loads(bytes(q["result"]["Success"]["output"]["data"]))


def event(name: str, at_ms: int) -> dict[str, Any]:
    return resolution("event", {"name": name, "payload": {"data": list(b"1")}}, at_ms=at_ms)


def task_done(value: Any, at_ms: int) -> dict[str, Any]:
    outcome = {"value": payload(value), "end": "completed", "attempts": 1}
    return resolution("step", {"outcome": outcome}, step_id="update-replay-step_1", at_ms=at_ms)


async def test_a_query_after_an_update_sees_its_state() -> None:
    """The update came while the workflow waited for "go", which has not come."""
    response = await run(make_worker(), request(journaled("u1", 42, 1_000), queries=True))
    assert answer(response) == 42


async def test_the_code_sees_an_update_from_before_the_result_it_waited_for() -> None:
    """The update came before "go", so the code reading state after "go" sees it;
    one that came after the last result is there by the end."""
    response = await run(
        make_worker(),
        request(
            journaled("u1", 42, 1_000),
            event("go", 2_000),
            journaled("u2", "x", 3_000),
            task_done(1, 4_000),
        ),
    )
    assert response["successful"], response["error"]
    (done,) = [c["CompleteWorkflow"] for c in response["commands"] if "CompleteWorkflow" in c]
    assert json.loads(bytes(done["result"]["data"])) == {"after_go": 42, "at_end": "x"}


async def test_an_update_after_a_result_is_not_seen_before_it() -> None:
    """The update came after "go": the code read the state before the update
    on the original run, and reads the same on replay."""
    response = await run(
        make_worker(),
        request(event("go", 2_000), journaled("u1", 42, 3_000), task_done(1, 4_000)),
    )
    (done,) = [c["CompleteWorkflow"] for c in response["commands"] if "CompleteWorkflow" in c]
    assert json.loads(bytes(done["result"]["data"])) == {"after_go": "initial", "at_end": 42}


async def test_a_new_update_applies_after_the_journaled_ones() -> None:
    response = await run(
        make_worker(),
        # Not yet in history: no time.
        request(journaled("u1", 42, 1_000), update("u2", "set_value", "new"), queries=True),
    )
    # The query is answered before the new update runs, from the replayed state.
    assert answer(response) == 42
    (new,) = response["update_results"]
    assert json.loads(bytes(new["result"]["Completed"]["output"]["data"])) == "new"


async def test_an_update_that_fails_again_on_replay_does_not_fail_the_workflow() -> None:
    response = await run(
        make_worker(),
        request(
            update("u0", "unknown", at_ms=400),
            update("u1", "refuse", 1, at_ms=500),
            journaled("u2", 42, 1_000),
            queries=True,
        ),
    )
    assert response["successful"]
    assert answer(response) == 42
