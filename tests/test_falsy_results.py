"""A falsy result is a result.

A workflow, query or update that returns ``0``, ``False``, ``""``, ``[]`` or
``{}`` hands that value back, not ``None``; only ``None`` itself is sent as
JSON ``null``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from orcher import WorkflowContext, workflow
from orcher.decorators import GlobalRegistry
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker

FALSY = [0, False, "", [], {}, 0.0, None]


def make_worker() -> Worker:
    return Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))


def start_request(workflow_type: str, data: Any = None) -> dict[str, Any]:
    return {
        "run_id": "run-1",
        "jobs": [
            {
                "StartWorkflow": {
                    "workflow_type": workflow_type,
                    "workflow_id": "wf-1",
                    "task_queue": "q",
                    "input": data,
                }
            }
        ],
    }


def completed_with(response: dict[str, Any]) -> Any:
    (complete,) = [c["CompleteWorkflow"] for c in response["commands"] if "CompleteWorkflow" in c]
    return json.loads(bytes(complete["result"]["data"]))


@pytest.mark.parametrize("value", FALSY, ids=repr)
async def test_a_workflow_result_comes_back_as_returned(value: Any) -> None:
    name = f"falsy-result-{type(value).__name__}"

    @workflow(name=name)
    async def returns_it(ctx: WorkflowContext) -> Any:
        return value

    worker = make_worker()
    worker._workflow_handlers[name] = GlobalRegistry.get_instance().get_workflow(name)

    response = await worker._execute_workflow(start_request(name))

    assert response["successful"], response["error"]
    result = completed_with(response)
    assert result == value
    assert type(result) is type(value)


@pytest.mark.parametrize("value", FALSY, ids=repr)
async def test_a_query_and_an_update_answer_as_returned(value: Any) -> None:
    name = f"falsy-handlers-{type(value).__name__}"

    @workflow(name=name)
    async def waits(ctx: WorkflowContext) -> None:
        ctx.register_query_handler("q", lambda _ctx: value)
        ctx.register_update_handler("u", lambda _ctx: value)
        await ctx.wait_for_event("never")

    worker = make_worker()
    worker._workflow_handlers[name] = GlobalRegistry.get_instance().get_workflow(name)
    request = start_request(name)
    request["jobs"] += [
        {"ProcessQuery": {"query_id": "q1", "query_type": "q", "arguments": []}},
        {"UpdateState": {"update_id": "u1", "update_name": "u", "payload": {"data": []}}},
    ]

    response = await worker._execute_workflow(request)

    (query,) = response["query_responses"]
    (update,) = response["update_results"]
    assert json.loads(bytes(query["result"]["Success"]["output"]["data"])) == value
    assert json.loads(bytes(update["result"]["Completed"]["output"]["data"])) == value


@pytest.mark.parametrize("value", [0, False, "", []], ids=repr)
def test_a_child_result_is_read_back_as_returned(value: Any) -> None:
    jobs = [
        {
            "ChildWorkflowCompleted": {
                "workflow_id": "child_1",
                "result": {"data": list(json.dumps(value).encode())},
            }
        }
    ]
    cached = make_worker()._extract_cached_results_from_jobs(jobs)
    assert cached["child:child_1"] == value
    assert type(cached["child:child_1"]) is type(value)
