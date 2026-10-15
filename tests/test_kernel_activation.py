"""Workflow activations as the kernel protocol carries them.

``load_activation`` reads a ``RunActivation`` (its JSON, as the native module
hands it over) into a workflow context; ``answer_activation`` turns what the
workflow's code did into a ``RunActivationResult``. These tests pin both
mappings, and run a workflow through ``Worker.run_activation`` end to end.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from orcher import TaskContext, WorkflowContext, task, workflow
from orcher.decorators import GlobalRegistry
from orcher.worker.activation import (
    ProtocolRefusalError,
    answer_activation,
    kernel_commands,
    load_activation,
)
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker
from orcher.workflow.sentinels import CHILD_FAILED_SENTINEL, TASK_FAILED_SENTINEL

START_MS = 1_700_000_000_000


def payload(value: Any) -> dict[str, Any]:
    return {"data": list(json.dumps(value).encode()), "metadata": {"encoding": list(b"json")}}


def resolution(
    kind: str, what: dict[str, Any], *, step_id: str = "", at_ms: int | None = None
) -> dict[str, Any]:
    return {"step_id": step_id, "at_ms": at_ms, "replaying": False, kind: what}


def activation(
    *resolutions: dict[str, Any], workflow_type: str = "wf", queries: tuple[Any, ...] = ()
) -> dict[str, Any]:
    return {
        "activation_id": [1, 2, 3],
        "run": {
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "workflow_type": workflow_type,
            "task_queue": "q",
            "namespace": "ns",
        },
        "resolutions": [
            resolution(
                "started",
                {"input": [payload({"n": 1})], "attempt": 2, "started_at_ms": START_MS},
                at_ms=START_MS,
            ),
            *resolutions,
        ],
        "queries": list(queries),
    }


def test_the_run_and_its_start_are_loaded() -> None:
    loaded = load_activation(activation(), "fallback")
    info = loaded.context.info
    assert (info.workflow_id, info.run_id, info.workflow_type) == ("wf-1", "run-1", "wf")
    assert (info.task_queue, info.namespace, info.attempt) == ("q", "ns", 2)
    assert int(info.started_at.timestamp() * 1000) == START_MS
    assert loaded.input == {"n": 1}
    assert loaded.activation_id == [1, 2, 3]
    # Nothing settled yet: not replaying.
    assert loaded.context.replaying is False


def test_settled_steps_children_and_timers_are_cached_with_their_times() -> None:
    loaded = load_activation(
        activation(
            resolution(
                "step",
                {"outcome": {"value": payload(7), "end": "completed", "attempts": 1}},
                step_id="charge_1",
                at_ms=START_MS + 10,
            ),
            resolution(
                "step",
                {
                    "outcome": {
                        "failure": {"message": "declined", "error_type": "CardDeclined"},
                        "end": "failed",
                        "attempts": 3,
                    }
                },
                step_id="charge_2",
                at_ms=START_MS + 20,
            ),
            resolution("timer", {}, step_id="timer_3", at_ms=START_MS + 30),
            resolution(
                "child",
                {
                    "child_workflow_id": "kid",
                    "outcome": {"value": payload("ok"), "end": "completed"},
                },
                step_id="kid",
                at_ms=START_MS + 40,
            ),
            resolution(
                "child",
                {
                    "child_workflow_id": "gone",
                    "outcome": {"failure": {"message": "operator"}, "end": "terminated"},
                },
                step_id="gone",
            ),
            resolution("child_started", {"child_workflow_id": "child_9"}, step_id="child_9"),
        ),
        "ns",
    )
    ctx = loaded.context
    assert ctx.replaying is True
    cached = ctx._cached_results
    assert cached["task:charge_1"] == 7
    assert cached["task:charge_2"] == {
        TASK_FAILED_SENTINEL: True,
        "message": "declined",
        "error_type": "CardDeclined",
        "attempts": 3,
    }
    # The timer's position among the resolutions, the start included.
    assert cached["timer:timer_3"] == {"fired_at": 3}
    assert cached["child:kid"] == "ok"
    assert cached["child:gone"] == {
        CHILD_FAILED_SENTINEL: True,
        "message": "child workflow was terminated: operator",
    }
    assert ctx._resolved_at == {
        "charge_1": START_MS + 10,
        "charge_2": START_MS + 20,
        "timer:timer_3": START_MS + 30,
        "child:kid": START_MS + 40,
    }
    assert ctx._journaled_children == {"kid", "gone", "child_9"}


@pytest.mark.parametrize(
    ("outcome", "message"),
    [
        ({"end": "failed", "failure": {"message": "boom"}}, "boom"),
        ({"end": "failed"}, "Child workflow failed"),
        ({"end": "cancelled"}, "child workflow was canceled"),
        ({"end": "timed_out"}, "child workflow timed out"),
    ],
)
def test_a_child_that_ended_without_a_result_failed(outcome: dict, message: str) -> None:
    loaded = load_activation(
        activation(resolution("child", {"child_workflow_id": "kid", "outcome": outcome})), "ns"
    )
    assert loaded.context._cached_results["child:kid"] == {
        CHILD_FAILED_SENTINEL: True,
        "message": message,
    }


def test_events_updates_and_a_cancel_request_are_loaded() -> None:
    loaded = load_activation(
        activation(
            resolution("event", {"name": "go", "payload": payload(1)}, at_ms=START_MS + 5),
            resolution(
                "update",
                {"update_id": "u1", "name": "set", "args": [payload(2)]},
                at_ms=START_MS + 6,
            ),
            # The same update again is run again once.
            resolution(
                "update",
                {"update_id": "u1", "name": "set", "args": [payload(2)]},
                at_ms=START_MS + 6,
            ),
            resolution("cancel", {"reason": "", "requested_at_ms": START_MS + 7}),
            resolution("cancel", {"reason": "", "requested_at_ms": START_MS + 9}),
            # Not yet in history: waiting for this activation's answer.
            resolution("update", {"update_id": "u2", "name": "set", "args": [payload(3)]}),
        ),
        "ns",
    )
    ctx = loaded.context
    assert ctx._cached_results["event_buffer:go"] == [payload(1)["data"]]
    assert ctx._cached_results["event_positions:go"] == [1]
    assert ctx._event_times == {"go": [START_MS + 5]}
    assert loaded.journaled_updates == [
        {"update_id": "u1", "update_name": "set", "args": b"2", "at_ms": START_MS + 6}
    ]
    assert [(u.update_id, u.args) for u in loaded.updates] == [("u2", b"3")]
    # The first request is the one the run is told of.
    assert ctx._cancel_requested and ctx._cancel_requested_at == START_MS + 7


def test_a_resolution_of_an_unknown_kind_is_refused() -> None:
    with pytest.raises(ProtocolRefusalError) as refused:
        load_activation(activation(resolution("unknown", {})), "ns")
    assert refused.value.kind == "unknown_resolution"


def test_steps_answered_from_history_are_satisfied_in_their_place() -> None:
    reached = [("task", "a_1"), ("timer", "timer_2"), ("child", "kid"), ("task", "b_4")]
    issued = [
        {"RecordStepResult": {"step_name": "note_3", "result": list(b"1"), "failure": None}},
        {
            "ScheduleTask": {
                "sequence": 4,
                "task_id": "b_4",
                "task_type": "b",
                "task_queue": "q",
                "input": [payload({})],
                "timeout": {"secs": 30, "nanos": 0},
                "retry_policy": {
                    "max_attempts": 2,
                    "initial_interval": {"secs": 1, "nanos": 0},
                    "max_interval": {"secs": 9, "nanos": 0},
                    "backoff_coefficient": 3.0,
                    "non_retryable_errors": ["X"],
                },
            }
        },
    ]
    commands = kernel_commands(issued, reached)
    fields = ("seq", "satisfied", "step_id")
    kinds = [
        (c.get("satisfied", False), c.get("step_id"), next(k for k in c if k not in fields))
        for c in commands
    ]
    assert kinds == [
        (False, "note_3", "record_value"),
        (True, "a_1", "run_task"),
        (True, "timer_2", "start_timer"),
        (True, "", "start_child"),
        (False, "b_4", "run_task"),
    ]
    assert commands[3]["start_child"] == {"workflow_id": "kid"}
    task_cmd = commands[4]
    assert task_cmd["seq"] == 4
    assert task_cmd["run_task"]["attempt_timeout"] == {"secs": 30, "nanos": 0}
    assert task_cmd["run_task"]["retry_policy"] == {
        "max_attempts": 2,
        "first_delay": {"secs": 1, "nanos": 0},
        "max_delay": {"secs": 9, "nanos": 0},
        "delay_multiplier": 3.0,
        "never_retry_types": ["X"],
    }


def test_steps_reached_after_the_last_issued_one_are_still_reported() -> None:
    commands = kernel_commands([], [("task", "a_1"), ("timer", "timer_2")])
    assert [c["satisfied"] for c in commands] == [True, True]


def test_a_restart_carries_its_execution_timeout() -> None:
    answer = answer_activation(
        [9],
        commands=[
            {
                "RestartFresh": {
                    "workflow_type": "",
                    "input": [],
                    "task_queue": None,
                    "timeout": {"secs": 5, "nanos": 0},
                }
            }
        ],
        reached=[],
    )
    assert "protocol_error" not in answer
    (restart,) = answer["commands"]
    assert restart["restart_fresh"]["execution_timeout"] == {"secs": 5, "nanos": 0}


def test_queries_and_updates_are_answered_in_the_kernels_shape() -> None:
    answer = answer_activation(
        [1],
        commands=[],
        reached=[],
        query_responses=[
            {"query_id": "q1", "result": {"Success": {"output": {"data": [49], "metadata": {}}}}},
            {"query_id": "q2", "result": {"Failed": {"message": "no", "details": None}}},
        ],
        update_results=[
            {
                "update_id": "u1",
                "result": {"Completed": {"output": {"data": [50], "metadata": {}}}},
            },
            {"update_id": "u2", "result": {"Rejected": {"message": "closed"}}},
            {"update_id": "u3", "result": {"Failed": {"message": "broke", "details": None}}},
        ],
    )
    assert answer["queries"] == [
        {"query_id": "q1", "value": {"data": [49], "metadata": {}}},
        {"query_id": "q2", "failure": {"message": "no"}},
    ]
    assert answer["updates"] == [
        {"update_id": "u1", "value": {"data": [50], "metadata": {}}},
        {"update_id": "u2", "rejected": {"message": "closed"}},
        {"update_id": "u3", "failed": {"message": "broke"}},
    ]


# ── end to end through Worker.run_activation ────────────────────────────


@task(name="kernel-activation-step")
async def step(ctx: TaskContext, n: int) -> int:
    return n


async def kernel_flow(ctx: WorkflowContext, n: int) -> dict[str, Any]:
    ctx.register_query_handler("where", lambda c: c.get_state("where"))
    ctx.register_update_handler("set_where", lambda c, value: ctx.set_state("where", value))
    ctx.set_state("where", "start")
    first = await ctx.execute_task(step, n=n)
    noted = await ctx.execute("note", lambda: first + 1)
    await ctx.send_event("other", "hello", {"n": noted})
    second = await ctx.execute_task(step, n=noted)
    return {"first": first, "second": second}


def make_worker() -> Worker:
    registry = GlobalRegistry.get_instance()
    if registry.get_workflow("kernel-flow") is None:
        workflow(name="kernel-flow")(kernel_flow)
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._workflow_handlers["kernel-flow"] = registry.get_workflow("kernel-flow")
    return worker


def native() -> Any:
    return pytest.importorskip("orcher._native")


async def test_a_workflow_runs_from_a_kernel_activation_end_to_end() -> None:
    worker = make_worker()
    first = activation(
        workflow_type="kernel-flow",
        queries=[{"query_id": "q", "name": "where", "args": []}],
    )
    first["resolutions"][0]["started"]["input"] = [payload({"n": 4})]
    first["resolutions"].append(
        resolution("update", {"update_id": "u", "name": "set_where", "args": [payload(["here"])]})
    )
    answer = await worker.run_activation(first)
    assert answer["activation_id"] == [1, 2, 3]
    (task_cmd,) = answer["commands"]
    assert task_cmd["step_id"] == "kernel-activation-step_1"
    assert answer["updates"][0]["update_id"] == "u"
    # Queries are answered from the state before this activation's updates.
    assert json.loads(bytes(answer["queries"][0]["value"]["data"])) == "start"

    # The task settled: the closure, the send and the second task go out, the
    # first task answered from history.
    second = activation(workflow_type="kernel-flow")
    second["resolutions"][0]["started"]["input"] = [payload({"n": 4})]
    second["resolutions"].append(
        resolution(
            "step",
            {"outcome": {"value": payload(4), "end": "completed", "attempts": 1}},
            step_id="kernel-activation-step_1",
            at_ms=START_MS + 1,
        )
    )
    answer = await worker.run_activation(second)
    result = json.loads(
        native()._kernel_execution_result("run-1", json.dumps(answer))
    )
    assert result["successful"] is True
    # A send is journaled as a step of its own, recorded with the closure.
    assert [list(c)[0] for c in result["commands"]] == [
        "RecordStepResult",
        "RecordStepResult",
        "SendEvent",
        "ScheduleTask",
    ]
    assert result["reached_steps"] == ["kernel-activation-step_1", "kernel-activation-step_4"]
    send = result["commands"][2]["SendEvent"]
    assert (send["workflow_id"], send["event_name"]) == ("other", "hello")


async def test_a_failing_workflow_answers_its_failure() -> None:
    async def fails(ctx: WorkflowContext, n: int) -> None:
        raise RuntimeError("nope")

    registry = GlobalRegistry.get_instance()
    if registry.get_workflow("kernel-fails") is None:
        workflow(name="kernel-fails")(fails)
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    worker._workflow_handlers["kernel-fails"] = registry.get_workflow("kernel-fails")
    answer = await worker.run_activation(activation(workflow_type="kernel-fails"))
    assert answer["workflow_failed"] == {
        "message": "nope",
        "non_retryable": True,
        "code": "application",
    }


async def test_an_activation_naming_no_run_is_a_protocol_error() -> None:
    answer = await make_worker().run_activation({"activation_id": [5], "resolutions": []})
    assert answer["activation_id"] == [5]
    assert answer["protocol_error"]["kind"] == "undecodable"
