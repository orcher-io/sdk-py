"""A child workflow's and a restart-fresh's retry policy reach the kernel.

``execute_child_workflow``, ``start_child_workflow`` and ``restart_fresh``
take a ``retry_policy`` with the same meaning as a workflow start's:
``max_attempts`` counts the first run, 0 retries without limit, and a policy
that leaves it alone allows 3. Left out, no policy is sent.
"""

from __future__ import annotations

import itertools
import json
from datetime import timedelta
from typing import Any

import pytest

from orcher import RetryPolicy, WorkflowContext, workflow
from orcher.decorators import GlobalRegistry
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker

_names = itertools.count()


def _payload(value: Any) -> dict[str, Any]:
    return {"data": list(json.dumps(value).encode()), "metadata": {"encoding": list(b"json")}}


def _activation(workflow_type: str) -> dict[str, Any]:
    return {
        "activation_id": [1],
        "run": {
            "workflow_id": "order-7",
            "run_id": "run-1",
            "workflow_type": workflow_type,
            "task_queue": "q",
            "namespace": "ns",
        },
        "resolutions": [
            {
                "step_id": "",
                "at_ms": 1_700_000_000_000,
                "replaying": False,
                "started": {"input": [_payload({})], "attempt": 1},
            }
        ],
        "queries": [],
    }


async def _issued(
    *,
    execute: RetryPolicy | None = None,
    start: RetryPolicy | None = None,
    restart: RetryPolicy | None = None,
) -> dict[str, Any]:
    """The kernel commands a workflow answers with when it starts a child with
    ``start_child_workflow``, then restarts fresh, each with the given policy;
    or, with ``execute`` given, when it starts a child with
    ``execute_child_workflow`` under that policy."""
    name = f"child-retry-policy-{next(_names)}"

    async def flow(ctx: WorkflowContext) -> None:
        if execute is not None:
            await ctx.execute_child_workflow("ship", retry_policy=execute)
        await ctx.start_child_workflow("ship", retry_policy=start)
        ctx.restart_fresh({"next": 1}, retry_policy=restart)

    workflow(name=name)(flow)
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._workflow_handlers[name] = GlobalRegistry.get_instance().get_workflow(name)
    answer = await worker.run_activation(_activation(name))
    assert "protocol_error" not in answer, answer
    return answer


def _policies(answer: dict[str, Any]) -> list[tuple[str, Any]]:
    out = []
    for command in answer["commands"]:
        for kind in ("start_child", "restart_fresh"):
            if kind in command:
                out.append((kind, command[kind].get("retry_policy")))
    return out


async def test_a_child_and_restart_fresh_retry_policy_reach_the_kernel_command() -> None:
    policy = RetryPolicy(
        max_attempts=2,
        initial_interval=timedelta(milliseconds=1500),
        max_interval=timedelta(seconds=9),
        backoff_coefficient=1.5,
        non_retryable_error_types=["Declined"],
    )
    expected = {
        "max_attempts": 2,
        "first_delay": {"secs": 1, "nanos": 500_000_000},
        "max_delay": {"secs": 9, "nanos": 0},
        "delay_multiplier": 1.5,
        "never_retry_types": ["Declined"],
    }
    answer = await _issued(start=policy, restart=policy)
    assert _policies(answer) == [("start_child", expected), ("restart_fresh", expected)]

    # The kernel takes both as the commands' retry policy.
    native = pytest.importorskip("orcher._native")
    result = json.loads(native._kernel_execution_result("run-1", json.dumps(answer)))
    kinds = [next(iter(c)) for c in result["commands"]]
    assert kinds == ["StartChildWorkflow", "RestartFresh"]
    for command, kind in zip(result["commands"], kinds, strict=True):
        sent = command[kind]["retry_policy"]
        assert sent["max_attempts"] == 2
        assert sent["initial_interval"] == {"secs": 1, "nanos": 500_000_000}
        assert sent["max_interval"] == {"secs": 9, "nanos": 0}
        assert sent["backoff_coefficient"] == 1.5
        assert sent["non_retryable_errors"] == ["Declined"]


async def test_execute_child_workflow_carries_its_retry_policy() -> None:
    answer = await _issued(execute=RetryPolicy(max_attempts=4))
    (kind, policy) = _policies(answer)[0]
    assert kind == "start_child"
    assert policy["max_attempts"] == 4


async def test_a_policy_left_at_its_count_sends_three_and_zero_sends_zero() -> None:
    answer = await _issued(
        start=RetryPolicy(initial_interval=timedelta(seconds=2)),
        restart=RetryPolicy(initial_interval=timedelta(seconds=2)),
    )
    assert [p["max_attempts"] for _, p in _policies(answer)] == [3, 3]

    answer = await _issued(start=RetryPolicy(max_attempts=0), restart=RetryPolicy(max_attempts=0))
    assert [p["max_attempts"] for _, p in _policies(answer)] == [0, 0]


async def test_without_a_retry_policy_none_is_sent() -> None:
    answer = await _issued()
    assert _policies(answer) == [("start_child", None), ("restart_fresh", None)]

    native = pytest.importorskip("orcher._native")
    result = json.loads(native._kernel_execution_result("run-1", json.dumps(answer)))
    for command in result["commands"]:
        ((kind, fields),) = command.items()
        assert fields.get("retry_policy") is None, kind


async def test_a_retry_policy_that_is_not_a_retry_policy_is_refused() -> None:
    ctx_calls = []

    async def flow(ctx: WorkflowContext) -> None:
        try:
            await ctx.start_child_workflow("ship", retry_policy={"max_attempts": 2})  # type: ignore[arg-type]
        except TypeError as exc:
            ctx_calls.append(str(exc))

    name = f"child-retry-policy-{next(_names)}"
    workflow(name=name)(flow)
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._workflow_handlers[name] = GlobalRegistry.get_instance().get_workflow(name)
    await worker.run_activation(_activation(name))
    assert ctx_calls == ["retry_policy must be a RetryPolicy, not dict"]
