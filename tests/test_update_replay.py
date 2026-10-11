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


def journaled(update_id: str, value: Any, at_ms: int | None) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "update_name": "set_value",
        "args": list(json.dumps(value).encode()),
        "at_ms": at_ms,
    }


def request(
    *jobs: dict[str, Any], updates: list[dict[str, Any]], times: dict[str, Any]
) -> dict[str, Any]:
    start = {
        "StartWorkflow": {
            "workflow_type": WORKFLOW,
            "workflow_id": "wf-1",
            "task_queue": "q",
            "input": None,
        }
    }
    return {
        "run_id": "run-1",
        "jobs": [start, *jobs],
        "journal_times": times,
        "journaled_updates": updates,
    }


def query(query_id: str) -> dict[str, Any]:
    return {"ProcessQuery": {"query_id": query_id, "query_type": "get_value", "arguments": []}}


def answer(response: dict[str, Any]) -> Any:
    (q,) = response["query_responses"]
    return json.loads(bytes(q["result"]["Success"]["output"]["data"]))


def event(name: str) -> dict[str, Any]:
    return {"HandleEvent": {"event_name": name, "payload": {"data": list(b"1")}}}


def task_done(value: Any) -> dict[str, Any]:
    return {
        "CompleteStep": {
            "step_name": "update-replay-step_1",
            "result": list(json.dumps(value).encode()),
        }
    }


async def test_a_query_after_an_update_sees_its_state() -> None:
    """The update came while the workflow waited for "go", which has not come."""
    response = await make_worker()._execute_workflow(
        request(query("q1"), updates=[journaled("u1", 42, 1_000)], times={"started_at_ms": 0})
    )
    assert answer(response) == 42


async def test_the_code_sees_an_update_from_before_the_result_it_waited_for() -> None:
    """The update came before "go", so the code reading state after "go" sees it;
    one that came after the last result is there by the end."""
    response = await make_worker()._execute_workflow(
        request(
            event("go"),
            task_done(1),
            updates=[journaled("u1", 42, 1_000), journaled("u2", "x", 3_000)],
            times={
                "started_at_ms": 0,
                "events": {"go": [2_000]},
                "resolved_at": {"update-replay-step_1": 4_000},
            },
        )
    )
    assert response["successful"], response["error"]
    (done,) = [c["CompleteWorkflow"] for c in response["commands"] if "CompleteWorkflow" in c]
    assert json.loads(bytes(done["result"]["data"])) == {"after_go": 42, "at_end": "x"}


async def test_an_update_after_a_result_is_not_seen_before_it() -> None:
    """The update came after "go": the code read the state before the update
    on the original run, and reads the same on replay."""
    response = await make_worker()._execute_workflow(
        request(
            event("go"),
            task_done(1),
            updates=[journaled("u1", 42, 3_000)],
            times={
                "started_at_ms": 0,
                "events": {"go": [2_000]},
                "resolved_at": {"update-replay-step_1": 4_000},
            },
        )
    )
    (done,) = [c["CompleteWorkflow"] for c in response["commands"] if "CompleteWorkflow" in c]
    assert json.loads(bytes(done["result"]["data"])) == {"after_go": "initial", "at_end": 42}


async def test_a_new_update_applies_after_the_journaled_ones() -> None:
    new = {
        "UpdateState": {
            "update_id": "u2",
            "update_name": "set_value",
            "payload": {"data": list(b'"new"')},
        }
    }
    response = await make_worker()._execute_workflow(
        request(new, query("q1"), updates=[journaled("u1", 42, 1_000)], times={})
    )
    # The query is answered before the new update runs, from the replayed state.
    assert answer(response) == 42
    (update,) = response["update_results"]
    assert json.loads(bytes(update["result"]["Completed"]["output"]["data"])) == "new"


async def test_an_update_that_fails_again_on_replay_does_not_fail_the_workflow() -> None:
    response = await make_worker()._execute_workflow(
        request(
            query("q1"),
            updates=[
                {"update_id": "u0", "update_name": "unknown", "args": [], "at_ms": 400},
                {"update_id": "u1", "update_name": "refuse", "args": list(b"1"), "at_ms": 500},
                journaled("u2", 42, 1_000),
            ],
            times={},
        )
    )
    assert response["successful"]
    assert answer(response) == 42
