"""Workflow activations as the kernel protocol (``orcher.kernel.v1``) carries them.

The kernel hands a workflow activation over as a ``RunActivation``: the run's
history already resolved into the outcomes its code observes, in the order it
observes them, each with the time it was recorded. :func:`load_activation`
loads those into a fresh :class:`~orcher.workflow.context.WorkflowContext`; the
worker runs the workflow's code in it, and :func:`answer_activation` turns
what the code did into a ``RunActivationResult``: every durable call the code
made, in issue order, those answered from history marked satisfied, with the
queries and updates answered.

Both messages cross the native boundary as JSON, in the shapes the native
module's ``kernel`` module documents: bytes as lists of integers, times as
``*_ms``, durations as ``{"secs", "nanos"}``, a ``oneof`` as the one key
present.

Step ids are the ones this SDK has always made: a task's
``{name}_{sequence}``, a timer's ``timer_{sequence}``, a child's workflow id,
and so on. They travel in each command's ``step_id``, or in
``start_child.workflow_id`` for a child.

This module is internal.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from orcher.types import DEFAULT_MAX_ATTEMPTS
from orcher.workflow.context import WorkflowContext
from orcher.workflow.info import WorkflowInfo
from orcher.workflow.sentinels import CHILD_FAILED_SENTINEL, TASK_FAILED_SENTINEL

logger = logging.getLogger(__name__)


class ProtocolRefusalError(Exception):
    """An activation the SDK cannot process, or a command the kernel protocol
    cannot carry: the activation is answered with a protocol error, which hands
    it back to the engine rather than failing the run."""

    def __init__(self, message: str, kind: str = "") -> None:
        super().__init__(message)
        self.kind = kind


@dataclass
class AskedQuery:
    """A query the activation asks the workflow to answer."""

    query_id: str
    query_type: str
    # The query's first argument, as its JSON encoding; None when it has none.
    args: bytes | None


@dataclass
class AskedUpdate:
    """An update waiting for the workflow to apply and answer it."""

    update_id: str
    update_type: str
    # The update's arguments, as their JSON encoding; empty for none.
    args: bytes


@dataclass
class LoadedActivation:
    """What a ``RunActivation`` holds for the workflow's code, loaded."""

    activation_id: list[int]
    context: WorkflowContext
    workflow_type: str
    # The run's input, as the value the client started it with.
    input: Any
    queries: list[AskedQuery] = field(default_factory=list)
    updates: list[AskedUpdate] = field(default_factory=list)
    # Updates the engine already answered, run again where they came, as the
    # context replays them.
    journaled_updates: list[dict[str, Any]] = field(default_factory=list)


def load_activation(activation: dict[str, Any], namespace: str) -> LoadedActivation:
    """Load an activation's resolutions into a context for its run.

    ``namespace`` is the one the worker serves, for an activation that does not
    name its own.

    Raises:
        ProtocolRefusalError: The activation names no run, or holds a resolution of
            a kind this SDK does not know.
    """
    run = activation.get("run") or {}
    if not run.get("run_id"):
        raise ProtocolRefusalError("the activation names no run", "undecodable")
    resolutions: list[dict[str, Any]] = activation.get("resolutions") or []

    started_resolution = next((r for r in resolutions if "started" in r), None)
    started: dict[str, Any] = (started_resolution or {}).get("started") or {}
    started_at_ms = started.get("started_at_ms")
    if started_at_ms is None and started_resolution is not None:
        started_at_ms = started_resolution.get("at_ms")
    inputs = started.get("input") or []
    workflow_input: Any = {}
    if inputs:
        decoded = _decode_json(inputs[0].get("data") or [])
        if decoded is not _UNDECODABLE:
            workflow_input = decoded

    # A run reading back what it already did replays: a step, child or timer
    # the history settled.
    replaying = any("step" in r or "child" in r or "timer" in r for r in resolutions)
    info = WorkflowInfo(
        workflow_id=run.get("workflow_id") or run["run_id"],
        run_id=run["run_id"],
        workflow_type=run.get("workflow_type", ""),
        task_queue=run.get("task_queue", ""),
        namespace=run.get("namespace") or namespace,
        # The engine counts a run's attempt from 1 (2 for the run of its first
        # workflow-level retry) and the kernel hands it over as is. Engines
        # before orcher#356 numbered the first run 0; it reads as 1, so a first
        # run is still the first attempt there. On a current engine the floor
        # never applies.
        attempt=max(int(started.get("attempt") or 1), 1),
        # The workflow's clock starts when the engine recorded the start; an
        # activation that records none starts it now.
        started_at=(
            datetime.fromtimestamp(started_at_ms / 1000, tz=UTC)
            if isinstance(started_at_ms, int)
            else datetime.now(UTC)
        ),
    )
    ctx = WorkflowContext(info=info, replaying=replaying)

    cached: dict[str, Any] = {}
    resolved_at: dict[str, int] = {}
    event_times: dict[str, list[int]] = {}
    children: list[str] = []
    journaled_updates: list[dict[str, Any]] = []
    pending_updates: list[AskedUpdate] = []
    cancel_asked = False
    cancel_at: int | None = None

    def note_resolved_at(key: str, at_ms: Any) -> None:
        # The first outcome of a key is the one the run observes.
        if isinstance(at_ms, int) and key not in resolved_at:
            resolved_at[key] = at_ms

    for position, resolution in enumerate(resolutions):
        step_id = resolution.get("step_id") or ""
        at_ms = resolution.get("at_ms")
        if "started" in resolution:
            continue
        if "step" in resolution:
            note_resolved_at(step_id, at_ms)
            outcome = resolution["step"].get("outcome") or {}
            if "failure" in outcome:
                failure = outcome["failure"] or {}
                cached[f"task:{step_id}"] = {
                    TASK_FAILED_SENTINEL: True,
                    "message": failure.get("message") or "Task failed",
                    "error_type": failure.get("error_type", ""),
                    # How many times the task ran, so a task that exhausted
                    # its retries reports them all.
                    "attempts": outcome.get("attempts"),
                }
            elif "value" in outcome:
                value = _decode_step_result((outcome["value"] or {}).get("data") or [])
                if value is not _UNDECODABLE:
                    cached[f"task:{step_id}"] = value
        elif "child" in resolution:
            child = resolution["child"]
            workflow_id = child.get("child_workflow_id") or step_id
            key = f"child:{workflow_id}"
            if workflow_id and workflow_id not in children:
                children.append(workflow_id)
            note_resolved_at(key, at_ms)
            outcome = child.get("outcome") or {}
            end = outcome.get("end", "unspecified")
            failure = outcome.get("failure") or {}
            if end == "failed":
                cached[key] = _child_failed(failure.get("message") or "Child workflow failed")
            elif end == "cancelled":
                cached[key] = _child_failed("child workflow was canceled")
            elif end == "terminated":
                # The failure's message is the reason given for the
                # termination, empty when none was.
                reason = failure.get("message") or ""
                cached[key] = _child_failed(
                    f"child workflow was terminated: {reason}"
                    if reason
                    else "child workflow was terminated"
                )
            elif end == "timed_out":
                cached[key] = _child_failed("child workflow timed out")
            elif "value" in outcome:
                value = _decode_step_result((outcome["value"] or {}).get("data") or [])
                if value is not _UNDECODABLE:
                    cached[key] = value
        elif "child_started" in resolution:
            # A child history records as started keeps the id it was started
            # under, which a child given no id is named after again.
            workflow_id = resolution["child_started"].get("child_workflow_id") or step_id
            if workflow_id and workflow_id not in children:
                children.append(workflow_id)
        elif "timer" in resolution:
            if step_id:
                note_resolved_at(f"timer:{step_id}", at_ms)
                # Where it fired among the outcomes: a wait with a deadline
                # takes whichever of its event and its timer came first.
                cached[f"timer:{step_id}"] = {"fired_at": position}
        elif "event" in resolution:
            event = resolution["event"]
            name = event.get("name") or ""
            if not name:
                continue
            data = (event.get("payload") or {}).get("data") or []
            cached.setdefault(f"event_buffer:{name}", []).append(data)
            cached.setdefault(f"event_positions:{name}", []).append(position)
            if isinstance(at_ms, int):
                event_times.setdefault(name, []).append(at_ms)
        elif "update" in resolution:
            update = resolution["update"]
            args = update.get("args") or []
            raw = bytes((args[0] or {}).get("data") or []) if args else b""
            if isinstance(at_ms, int):
                # Recorded in history: the engine answered it already. It runs
                # again where it came, so the state it set is there.
                if not any(u["update_id"] == update.get("update_id") for u in journaled_updates):
                    journaled_updates.append(
                        {
                            "update_id": update.get("update_id", ""),
                            "update_name": update.get("name", ""),
                            "args": raw,
                            "at_ms": at_ms,
                        }
                    )
            else:
                # Not yet in history: waiting for this activation's answer.
                pending_updates.append(
                    AskedUpdate(
                        update_id=update.get("update_id", ""),
                        update_type=update.get("name", ""),
                        args=raw,
                    )
                )
        elif "cancel" in resolution:
            # The first request is the one the run is told of.
            if not cancel_asked:
                cancel_asked = True
                requested = resolution["cancel"].get("requested_at_ms")
                cancel_at = requested if isinstance(requested, int) else at_ms
        else:
            raise ProtocolRefusalError(
                f"a resolution of a kind this SDK does not know: {sorted(resolution)}",
                "unknown_resolution",
            )

    ctx._record_journal_times({"resolved_at": resolved_at, "events": event_times})
    if cancel_asked:
        ctx._note_cancel_requested(cancel_at if isinstance(cancel_at, int) else None)
    ctx._record_journaled_children(children)
    if cached:
        ctx._set_cached_results(cached)

    queries = []
    for query in activation.get("queries") or []:
        args = query.get("args") or []
        queries.append(
            AskedQuery(
                query_id=query.get("query_id", ""),
                query_type=query.get("name", ""),
                args=bytes((args[0] or {}).get("data") or []) if args else None,
            )
        )

    return LoadedActivation(
        activation_id=list(activation.get("activation_id") or []),
        context=ctx,
        workflow_type=run.get("workflow_type", ""),
        input=workflow_input,
        queries=queries,
        updates=pending_updates,
        journaled_updates=journaled_updates,
    )


def answer_activation(
    activation_id: list[int],
    *,
    commands: list[dict[str, Any]] | None = None,
    reached: list[tuple[str, str]] | None = None,
    failure: dict[str, Any] | None = None,
    query_responses: list[dict[str, Any]] | None = None,
    update_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """The answer to an activation: what the workflow's code produced, as the
    kernel protocol's commands (or ``failure``, the run's failure), with the
    queries and updates answered.

    ``commands`` and ``reached`` are as the workflow context hands them over:
    the commands issued, and every step reached, each as its kind and id.
    ``query_responses`` and ``update_results`` are as the worker's handlers
    produce them.
    """
    answered: dict[str, Any] = {
        "activation_id": list(activation_id),
        "queries": [_query_answer(r) for r in query_responses or []],
        "updates": [_update_answer(r) for r in update_results or []],
    }
    if failure is not None:
        answered["workflow_failed"] = failure
        return answered
    try:
        answered["commands"] = kernel_commands(commands or [], reached or [])
    except ProtocolRefusalError as refusal:
        return protocol_error(activation_id, str(refusal), refusal.kind)
    return answered


def protocol_error(activation_id: list[int], message: str, kind: str = "") -> dict[str, Any]:
    """An activation the SDK could not process, answered as such."""
    return {
        "activation_id": list(activation_id),
        "protocol_error": {"message": message, "kind": kind},
    }


def workflow_failure(message: str, *, non_determinism: bool = False) -> dict[str, Any]:
    """The run's failure: the workflow's code raised. Never retried, as before."""
    return {
        "message": message,
        "non_retryable": True,
        "code": "non_determinism" if non_determinism else "application",
    }


def kernel_commands(
    issued: list[dict[str, Any]], reached: list[tuple[str, str]]
) -> list[dict[str, Any]]:
    """Every durable call, in issue order: the new commands as issued, with the
    steps answered from history (reached, but not issued) marked satisfied in
    their place among them.

    Raises:
        ProtocolRefusalError: A command the kernel protocol cannot carry.
    """
    issued_steps = {step for command in issued if (step := _step_of(command)) is not None}
    out: list[dict[str, Any]] = []
    remaining = list(reached)

    def satisfied_up_to(until: tuple[str, str] | None) -> None:
        # The steps reached before `until` that were not issued were answered
        # from history; `until` itself is the issued call.
        if until is not None and until not in remaining:
            return
        while remaining:
            step = remaining.pop(0)
            if step == until:
                return
            if step not in issued_steps:
                out.append(_satisfied(step))

    for command in issued:
        step = _step_of(command)
        if step is not None:
            satisfied_up_to(step)
        out.append(_kernel_command(command))
    satisfied_up_to(None)
    return out


def _step_of(command: dict[str, Any]) -> tuple[str, str] | None:
    """The step a command issues, as its kind and id, if it issues one."""
    if "ScheduleTask" in command:
        return ("task", command["ScheduleTask"].get("task_id", ""))
    if "StartTimer" in command:
        return ("timer", command["StartTimer"].get("timer_id", ""))
    if "StartChildWorkflow" in command:
        return ("child", command["StartChildWorkflow"].get("workflow_id", ""))
    return None


def _satisfied(step: tuple[str, str]) -> dict[str, Any]:
    """A call answered from history: its kind and id, no payloads."""
    kind, step_id = step
    if kind == "timer":
        return {"seq": 0, "satisfied": True, "step_id": step_id, "start_timer": {}}
    if kind == "child":
        return {
            "seq": 0,
            "satisfied": True,
            "step_id": "",
            "start_child": {"workflow_id": step_id},
        }
    return {"seq": 0, "satisfied": True, "step_id": step_id, "run_task": {}}


def _kernel_command(command: dict[str, Any]) -> dict[str, Any]:
    """A new command, as the kernel protocol carries it. The values, and the
    defaults the context filled in, are the ones the worker sent before."""
    if len(command) != 1:
        raise ProtocolRefusalError(f"a command of no single kind: {sorted(command)}")
    ((kind, fields),) = command.items()
    if kind == "ScheduleTask":
        return {
            "seq": int(fields.get("sequence") or 0),
            "step_id": fields.get("task_id", ""),
            "run_task": {
                "task_type": fields.get("task_type", ""),
                "task_queue": fields.get("task_queue") or "",
                "input": [_payload(p) for p in fields.get("input") or []],
                "attempt_timeout": fields.get("timeout"),
                "heartbeat_timeout": fields.get("heartbeat_timeout"),
                "queue_timeout": fields.get("queue_timeout"),
                "retry_policy": _retry_policy(fields.get("retry_policy")),
            },
        }
    if kind == "StartTimer":
        return {
            "seq": int(fields.get("sequence") or 0),
            "step_id": fields.get("timer_id", ""),
            "start_timer": {"duration": fields.get("duration")},
        }
    if kind == "StartChildWorkflow":
        return {
            "seq": int(fields.get("sequence") or 0),
            "start_child": {
                "workflow_id": fields.get("workflow_id", ""),
                "workflow_type": fields.get("workflow_type", ""),
                "task_queue": fields.get("task_queue") or "",
                "input": [_payload(p) for p in fields.get("input") or []],
                "execution_timeout": fields.get("timeout"),
                "orphan_policy": str(fields.get("orphan_policy") or "cancel").lower(),
            },
        }
    if kind == "CancelChildWorkflow":
        return {"cancel_child": {"step_id": fields.get("workflow_id", "")}}
    if kind == "SendEvent":
        return {
            "send_event": {
                "workflow_id": fields.get("workflow_id", ""),
                # Any run of the target receives it.
                "run_id": fields.get("run_id") or "",
                "name": fields.get("event_name", ""),
                "payload": _payload(fields.get("payload") or {}),
            }
        }
    if kind == "RecordStepResult":
        if fields.get("failure"):
            raise ProtocolRefusalError("a recorded value that failed is never recorded by this SDK")
        return {
            "step_id": fields.get("step_name", ""),
            "record_value": {
                "outcome": {
                    "value": {"data": list(fields.get("result") or [])},
                    "end": "completed",
                    "attempts": int(fields.get("execution_attempt") or 1),
                }
            },
        }
    if kind == "WaitForEvent":
        timeout_ms = fields.get("timeout_ms")
        return {
            "step_id": fields.get("step_id", ""),
            "await_event": {
                "name": fields.get("event_name", ""),
                "timeout": (
                    {"secs": timeout_ms // 1000, "nanos": (timeout_ms % 1000) * 1_000_000}
                    if isinstance(timeout_ms, int)
                    else None
                ),
            },
        }
    if kind == "CompleteWorkflow":
        return {"complete_run": {"result": _payload(fields.get("result") or {})}}
    if kind == "CancelWorkflowExecution":
        return {"end_cancelled": {"details": ""}}
    if kind == "RestartFresh":
        return {
            "restart_fresh": {
                "workflow_type": fields.get("workflow_type") or "",
                "task_queue": fields.get("task_queue") or "",
                "input": [_payload(p) for p in fields.get("input") or []],
                "execution_timeout": fields.get("timeout"),
            }
        }
    raise ProtocolRefusalError(
        f"the workflow issued a command the kernel protocol does not carry: {kind}"
    )


def _payload(payload: dict[str, Any]) -> dict[str, Any]:
    """A payload as the context holds it (bytes as lists of integers)."""
    return {
        "data": list(payload.get("data") or []),
        "metadata": {k: list(v) for k, v in (payload.get("metadata") or {}).items()},
    }


def _retry_policy(policy: dict[str, Any] | None) -> dict[str, Any] | None:
    """A retry policy as the context normalizes it, in the kernel's names."""
    if policy is None:
        return None
    return {
        # An omitted count takes the documented default; 0 means unlimited.
        "max_attempts": int(
            DEFAULT_MAX_ATTEMPTS if policy.get("max_attempts") is None else policy["max_attempts"]
        ),
        "first_delay": policy.get("initial_interval"),
        "max_delay": policy.get("max_interval"),
        "delay_multiplier": float(policy.get("backoff_coefficient") or 0.0),
        "never_retry_types": list(policy.get("non_retryable_errors") or []),
    }


def _query_answer(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("result") or {}
    if "Success" in result:
        return {
            "query_id": response.get("query_id", ""),
            "value": _payload(result["Success"].get("output") or {}),
        }
    return {
        "query_id": response.get("query_id", ""),
        "failure": {"message": (result.get("Failed") or {}).get("message", "")},
    }


def _update_answer(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("result") or {}
    answer: dict[str, Any] = {"update_id": response.get("update_id", "")}
    if "Completed" in result:
        answer["value"] = _payload(result["Completed"].get("output") or {})
    elif "Rejected" in result:
        answer["rejected"] = {"message": result["Rejected"].get("message", "")}
    else:
        answer["failed"] = {"message": (result.get("Failed") or {}).get("message", "")}
    return answer


def _child_failed(message: str) -> dict[str, Any]:
    return {CHILD_FAILED_SENTINEL: True, "message": message}


class _Undecodable:
    pass


_UNDECODABLE: Any = _Undecodable()


def _decode_step_result(data: list[int]) -> Any:
    """A recorded result: empty is None; bytes that are not JSON are not a result."""
    if not data:
        return None
    return _decode_json(data)


def _decode_json(data: list[int]) -> Any:
    try:
        return json.loads(bytes(data).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return _UNDECODABLE
