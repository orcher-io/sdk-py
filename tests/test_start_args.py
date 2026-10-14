"""Start arguments reach the workflow one to one.

Several ``args`` given to a client start or a child start arrive as the
workflow's positional parameters, not as one list. One argument still arrives
as itself, a dict still spreads as keyword arguments, and none passes nothing.
Each test sends through the real packing, a JSON round trip as on the wire, and
the worker's own call of the workflow.
"""

from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

from orcher import Client, ClientConfig, WorkflowContext, workflow
from orcher.client.workflow_handle import WorkflowHandle
from orcher.decorators import GlobalRegistry
from orcher.errors import WorkflowSuspendedError
from orcher.worker.activation import AskedQuery, AskedUpdate
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker
from orcher.workflow.info import WorkflowInfo


async def echo(ctx: WorkflowContext, *args: Any, **kwargs: Any) -> dict[str, Any]:
    return {"args": list(args), "kwargs": kwargs}


async def three(ctx: WorkflowContext, a: int, b: str, c: list[int] | None = None) -> list[Any]:
    return [a, b, c]


WORKFLOWS = {"start-args-echo": echo, "start-args-three": three}

CASES = [
    ((), {"args": [], "kwargs": {}}),
    ((7,), {"args": [7], "kwargs": {}}),
    (([1, 2],), {"args": [[1, 2]], "kwargs": {}}),
    (({"x": 1},), {"args": [], "kwargs": {"x": 1}}),
    ((1, "b", [3]), {"args": [1, "b", [3]], "kwargs": {}}),
    (([1], [2]), {"args": [[1], [2]], "kwargs": {}}),
    (({"x": 1}, {"y": 2}), {"args": [{"x": 1}, {"y": 2}], "kwargs": {}}),
]


async def run_on_worker(workflow_type: str, sent: Any) -> Any:
    """Run ``workflow_type`` on a worker given ``sent`` as its JSON input."""
    # Registered here, not at import: other tests reset the registry.
    registry = GlobalRegistry.get_instance()
    if registry.get_workflow(workflow_type) is None:
        workflow(name=workflow_type)(WORKFLOWS[workflow_type])
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._workflow_handlers[workflow_type] = registry.get_workflow(workflow_type)
    response = await worker._execute_workflow(
        {
            "run_id": "run-1",
            "jobs": [
                {
                    "StartWorkflow": {
                        "workflow_type": workflow_type,
                        "workflow_id": "wf-1",
                        "task_queue": "q",
                        "input": json.loads(sent),
                    }
                }
            ],
        }
    )
    assert response["successful"], response["error"]
    (done,) = [c["CompleteWorkflow"] for c in response["commands"] if "CompleteWorkflow" in c]
    return json.loads(bytes(done["result"]["data"]))


async def sent_by_client(args: tuple[Any, ...]) -> bytes:
    """The input a client start sends for ``args``."""
    sent: list[bytes] = []

    async def start_workflow(workflow_id: str, workflow_type: str, queue: str, data: Any) -> Any:
        sent.append(json.dumps(data).encode())
        return SimpleNamespace(workflow_id=workflow_id, run_id="run-1")

    client = Client(ClientConfig(server_url="http://localhost:50051"))
    client._connected = True
    client._native_client = SimpleNamespace(start_workflow=start_workflow)
    await client.start_workflow(workflow_type="t", workflow_id="w", task_queue="q", args=args)
    (data,) = sent
    return data


def child_context() -> WorkflowContext:
    info = WorkflowInfo(
        workflow_id="parent",
        run_id="run",
        workflow_type="parent",
        task_queue="q",
        namespace="default",
        attempt=1,
        started_at=datetime(2026, 1, 1),
    )
    return WorkflowContext(info)


async def sent_to_child(args: tuple[Any, ...], *, wait: bool) -> bytes:
    """The input a parent sends a child for ``args``."""
    ctx = child_context()
    if wait:
        with pytest.raises(WorkflowSuspendedError):
            await ctx.execute_child_workflow("t", args=args)
    else:
        await ctx.start_child_workflow("t", args=args)
    (start,) = [c["StartChildWorkflow"] for c in ctx._take_commands()]
    (payload,) = start["input"]
    return bytes(payload["data"])


@pytest.mark.parametrize(("args", "received"), CASES, ids=repr)
async def test_a_client_start_passes_its_args_one_to_one(args: Any, received: Any) -> None:
    assert await run_on_worker("start-args-echo", await sent_by_client(args)) == received


@pytest.mark.parametrize("wait", [True, False], ids=["execute", "start"])
@pytest.mark.parametrize(("args", "received"), CASES, ids=repr)
async def test_a_child_start_passes_its_args_one_to_one(
    args: Any, received: Any, wait: bool
) -> None:
    sent = await sent_to_child(args, wait=wait)
    assert await run_on_worker("start-args-echo", sent) == received


async def test_several_args_fill_named_parameters_in_order() -> None:
    sent = await sent_by_client((1, "two", [3]))
    assert await run_on_worker("start-args-three", sent) == [1, "two", [3]]
    sent = await sent_to_child((1, "two"), wait=True)
    assert await run_on_worker("start-args-three", sent) == [1, "two", None]


async def test_a_single_arg_is_sent_as_itself() -> None:
    """Unchanged on the wire, so workers from before still read it."""
    assert json.loads(await sent_by_client(([1, 2],))) == [1, 2]
    assert json.loads(await sent_by_client(({"x": 1},))) == {"x": 1}
    assert json.loads(await sent_to_child((5,), wait=True)) == 5


async def test_query_and_update_args_are_passed_one_to_one() -> None:
    """A handle's query and update pack their arguments the same way."""
    sent: list[Any] = []

    async def capture(name: str, data: Any) -> None:
        sent.append(data)

    client = Client(ClientConfig(server_url="http://localhost:50051"))
    native = SimpleNamespace(workflow_id="w", run_id="r", query=capture, update=capture)
    handle = WorkflowHandle._from_native(client, native)
    await handle.query("q", 1, [2])
    await handle.update("u", 1, [2])

    seen: list[Any] = []

    def handler(ctx: Any, *args: Any) -> None:
        seen.append(list(args))

    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    ctx = child_context()
    ctx.register_query_handler("q", handler)
    ctx.register_update_handler("u", handler)
    query_args, update_args = (list(json.dumps(data).encode()) for data in sent)
    worker._answer_queries([AskedQuery("1", "q", bytes(query_args))], ctx)
    await worker._answer_updates([AskedUpdate("2", "u", bytes(update_args))], ctx)
    assert seen == [[1, [2]], [1, [2]]]
