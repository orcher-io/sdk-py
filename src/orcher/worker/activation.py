"""Workflow activations as the kernel protocol (``orcher.kernel.v1``) carries them.

The kernel hands a workflow activation over as a ``RunActivation``: the run's
history already resolved into the outcomes its code observes, in the order it
observes them, each with the time it was recorded. :func:`load_activation`
loads those into a fresh :class:`~orcher.workflow.context.WorkflowContext`; the
worker runs the workflow's code in it, and :func:`answer_activation` turns
what the code did into a ``RunActivationResult``: every durable call the code
made, in issue order, those answered from history marked satisfied, with the
queries and updates answered.

Both cross the native boundary encoded, and are read and written here with
the messages generated from the protocol (``orcher._proto``).

Step ids are the ones this SDK has always made: a task's
``{name}_{sequence}``, a timer's ``timer_{sequence}``, a child's workflow id,
and so on. They travel in each command's ``step_id``, or in
``start_child.workflow_id`` for a child.

This module is internal.
"""

from __future__ import annotations

import contextlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from google.protobuf import duration_pb2

from orcher._proto import kernel_pb2 as kernel
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

    activation_id: bytes
    context: WorkflowContext
    workflow_type: str
    # The run's input, as the value the client started it with.
    input: Any
    queries: list[AskedQuery] = field(default_factory=list)
    updates: list[AskedUpdate] = field(default_factory=list)
    # Updates the engine already answered, run again where they came, as the
    # context replays them.
    journaled_updates: list[dict[str, Any]] = field(default_factory=list)


def _order(position: int, resolution: kernel.Resolution) -> int:
    """Where a resolution comes in the order the run observes its outcomes: the
    sequence the kernel numbered it with. Which of two outcomes came first, a
    wait's result or a cancellation request, is decided by it, never by their
    times, which tie within a millisecond. A resolution the kernel did not
    number (one built by hand) keeps its place in the list, the same order."""
    if resolution.sequence > 0:
        return int(resolution.sequence)
    return position + 1


def _millis(timestamp: Any) -> int:
    return int(timestamp.seconds) * 1000 + int(timestamp.nanos) // 1_000_000


def _at_ms(resolution: kernel.Resolution) -> int | None:
    """When the resolution was recorded; None for one not yet in history."""
    return _millis(resolution.at) if resolution.HasField("at") else None


def load_activation(activation: kernel.RunActivation, namespace: str) -> LoadedActivation:
    """Load an activation's resolutions into a context for its run.

    ``namespace`` is the one the worker serves, for an activation that does not
    name its own.

    Raises:
        ProtocolRefusalError: The activation names no run, or holds a resolution of
            a kind this SDK does not know.
    """
    run = activation.run
    if not run.run_id:
        raise ProtocolRefusalError("the activation names no run", "undecodable")
    resolutions = list(activation.resolutions)

    started_resolution = next((r for r in resolutions if r.HasField("started")), None)
    started = started_resolution.started if started_resolution is not None else None
    started_at_ms: int | None = None
    if started is not None and started.HasField("started_at"):
        started_at_ms = _millis(started.started_at)
    elif started_resolution is not None:
        started_at_ms = _at_ms(started_resolution)
    workflow_input: Any = {}
    if started is not None and started.input:
        decoded = _decode_json(started.input[0].data)
        if decoded is not _UNDECODABLE:
            workflow_input = decoded

    # A run reading back what it already did replays: a step, child or timer
    # the history settled.
    replaying = any(r.WhichOneof("what") in ("step", "child", "timer") for r in resolutions)
    info = WorkflowInfo(
        workflow_id=run.workflow_id or run.run_id,
        run_id=run.run_id,
        workflow_type=run.workflow_type,
        task_queue=run.task_queue,
        namespace=run.namespace or namespace,
        # The engine counts a run's attempt from 1 (2 for the run of its first
        # workflow-level retry) and the kernel hands it over as is. Engines
        # before orcher#356 numbered the first run 0; it reads as 1, so a first
        # run is still the first attempt there. On a current engine the floor
        # never applies.
        attempt=max(int(started.attempt) if started is not None else 1, 1),
        # The workflow's clock starts when the engine recorded the start; an
        # activation that records none starts it now.
        started_at=(
            datetime.fromtimestamp(started_at_ms / 1000, tz=UTC)
            if started_at_ms is not None
            else datetime.now(UTC)
        ),
    )
    ctx = WorkflowContext(info=info, replaying=replaying)

    cached: dict[str, Any] = {}
    resolved_at: dict[str, int] = {}
    resolved_order: dict[str, int] = {}
    event_times: dict[str, list[int]] = {}
    event_orders: dict[str, list[int]] = {}
    children: list[str] = []
    journaled_updates: list[dict[str, Any]] = []
    pending_updates: list[AskedUpdate] = []
    cancel_order: int | None = None

    def note_resolved_at(key: str, at_ms: int | None, order: int) -> None:
        # The first outcome of a key is the one the run observes.
        if at_ms is not None and key not in resolved_at:
            resolved_at[key] = at_ms
        resolved_order.setdefault(key, order)

    for position, resolution in enumerate(resolutions):
        step_id = resolution.step_id
        at_ms = _at_ms(resolution)
        order = _order(position, resolution)
        what = resolution.WhichOneof("what")
        if what == "started":
            continue
        if what == "step":
            note_resolved_at(step_id, at_ms, order)
            outcome = resolution.step.outcome
            result = outcome.WhichOneof("result")
            if result == "failure":
                failure = outcome.failure
                cached[f"task:{step_id}"] = {
                    TASK_FAILED_SENTINEL: True,
                    "message": failure.message or "Task failed",
                    "error_type": failure.error_type,
                    # Why it failed: "application" for a task that raised,
                    # "cancelled" for one that was cancelled, and so on.
                    "code": _failure_code(failure, outcome),
                    # How many times the task ran, so a task that exhausted
                    # its retries reports them all.
                    "attempts": int(outcome.attempts),
                }
            elif result == "value":
                value = _decode_step_result(outcome.value.data)
                if value is not _UNDECODABLE:
                    cached[f"task:{step_id}"] = value
        elif what == "child":
            child = resolution.child
            workflow_id = child.child_workflow_id or step_id
            key = f"child:{workflow_id}"
            if workflow_id and workflow_id not in children:
                children.append(workflow_id)
            note_resolved_at(key, at_ms, order)
            outcome = child.outcome
            end = outcome.end
            message = outcome.failure.message if outcome.WhichOneof("result") == "failure" else ""
            if end == kernel.END_FAILED:
                cached[key] = _child_failed(message or "Child workflow failed")
            elif end == kernel.END_CANCELLED:
                cached[key] = _child_failed("child workflow was canceled")
            elif end == kernel.END_TERMINATED:
                # The failure's message is the reason given for the
                # termination, empty when none was.
                cached[key] = _child_failed(
                    f"child workflow was terminated: {message}"
                    if message
                    else "child workflow was terminated"
                )
            elif end == kernel.END_TIMED_OUT:
                cached[key] = _child_failed("child workflow timed out")
            elif outcome.WhichOneof("result") == "value":
                value = _decode_step_result(outcome.value.data)
                if value is not _UNDECODABLE:
                    cached[key] = value
        elif what == "child_started":
            # A child history records as started keeps the id it was started
            # under, which a child given no id is named after again.
            workflow_id = resolution.child_started.child_workflow_id or step_id
            if workflow_id and workflow_id not in children:
                children.append(workflow_id)
        elif what == "timer":
            if step_id:
                note_resolved_at(f"timer:{step_id}", at_ms, order)
                # Where it fired among the outcomes: a wait with a deadline
                # takes whichever of its event and its timer came first.
                cached[f"timer:{step_id}"] = {"fired_at": position}
        elif what == "event":
            event = resolution.event
            name = event.name
            if not name:
                continue
            cached.setdefault(f"event_buffer:{name}", []).append(event.payload.data)
            cached.setdefault(f"event_positions:{name}", []).append(position)
            if at_ms is not None:
                event_times.setdefault(name, []).append(at_ms)
            event_orders.setdefault(name, []).append(order)
        elif what == "update":
            update = resolution.update
            raw = update.args[0].data if update.args else b""
            if at_ms is not None:
                # Recorded in history: the engine answered it already. It runs
                # again where it came, so the state it set is there.
                if not any(u["update_id"] == update.update_id for u in journaled_updates):
                    journaled_updates.append(
                        {
                            "update_id": update.update_id,
                            "update_name": update.name,
                            "args": raw,
                            "at_ms": at_ms,
                        }
                    )
            else:
                # Not yet in history: waiting for this activation's answer.
                pending_updates.append(
                    AskedUpdate(update_id=update.update_id, update_type=update.name, args=raw)
                )
        elif what == "cancel":
            # The first request is the one the run is told of.
            if cancel_order is None:
                cancel_order = order
        else:
            raise ProtocolRefusalError(
                f"a resolution of a kind this SDK does not know: {what}",
                "unknown_resolution",
            )

    ctx._record_journal_times(
        {
            "resolved_at": resolved_at,
            "events": event_times,
            "resolved_order": resolved_order,
            "event_orders": event_orders,
        }
    )
    if cancel_order is not None:
        ctx._note_cancel_requested(cancel_order)
    ctx._record_journaled_children(children)
    if cached:
        ctx._set_cached_results(cached)

    queries = [
        AskedQuery(
            query_id=query.query_id,
            query_type=query.name,
            args=query.args[0].data if query.args else None,
        )
        for query in activation.queries
    ]

    return LoadedActivation(
        activation_id=activation.activation_id,
        context=ctx,
        workflow_type=run.workflow_type,
        input=workflow_input,
        queries=queries,
        updates=pending_updates,
        journaled_updates=journaled_updates,
    )


def answer_activation(
    activation_id: bytes,
    *,
    commands: list[dict[str, Any]] | None = None,
    reached: list[tuple[str, str]] | None = None,
    failure: kernel.Failure | None = None,
    query_responses: list[dict[str, Any]] | None = None,
    update_results: list[dict[str, Any]] | None = None,
) -> kernel.RunActivationResult:
    """The answer to an activation: what the workflow's code produced, as the
    kernel protocol's commands (or ``failure``, the run's failure), with the
    queries and updates answered.

    ``commands`` and ``reached`` are as the workflow context hands them over:
    the commands issued, and every step reached, each as its kind and id.
    With ``failure``, they are the calls the code made before it failed,
    which the kernel checks against history and never sends; ``reached`` left
    out (no code ran) leaves the failure unchecked. ``query_responses`` and
    ``update_results`` are as the worker's handlers produce them.
    """
    answered = kernel.RunActivationResult(
        activation_id=activation_id,
        queries=[_query_answer(r) for r in query_responses or []],
        updates=[_update_answer(r) for r in update_results or []],
    )
    if failure is not None:
        answered.workflow_failed.CopyFrom(failure)
        if reached is not None:
            # A call the protocol cannot carry leaves the failure unchecked,
            # rather than its calls reported wrong.
            with contextlib.suppress(ProtocolRefusalError):
                calls = kernel_commands(commands or [], reached)
                # Set even when empty: no calls is a checked answer, unlike
                # none given.
                answered.calls_before_failure.SetInParent()
                answered.calls_before_failure.commands.extend(calls)
        return answered
    try:
        issued = kernel_commands(commands or [], reached or [])
    except ProtocolRefusalError as refusal:
        return protocol_error(activation_id, str(refusal), refusal.kind)
    answered.commands.SetInParent()
    answered.commands.commands.extend(issued)
    return answered


_PROTOCOL_ERROR_KINDS = {
    "unknown_resolution": kernel.PROTOCOL_ERROR_KIND_UNKNOWN_RESOLUTION,
    "undecodable": kernel.PROTOCOL_ERROR_KIND_UNDECODABLE,
    "unsupported_version": kernel.PROTOCOL_ERROR_KIND_UNSUPPORTED_VERSION,
}


def protocol_error(
    activation_id: bytes, message: str, kind: str = ""
) -> kernel.RunActivationResult:
    """An activation the SDK could not process, answered as such."""
    return kernel.RunActivationResult(
        activation_id=activation_id,
        protocol_error=kernel.ProtocolError(
            message=message,
            kind=_PROTOCOL_ERROR_KINDS.get(kind, kernel.PROTOCOL_ERROR_KIND_UNSPECIFIED),
        ),
    )


def workflow_failure(
    message: str, *, non_determinism: bool = False, non_retryable: bool = False
) -> kernel.Failure:
    """The run's failure: the workflow's code raised.

    ``non_retryable`` when the code raised it as non-retryable, the way task
    code does (a truthy ``non_retryable`` attribute on the exception): the
    workflow is then not retried, whatever its retry policy allows. Any other
    failure is retried as the policy says.
    """
    return kernel.Failure(
        message=message,
        non_retryable=non_retryable,
        code=(
            kernel.FAILURE_CODE_NON_DETERMINISM
            if non_determinism
            else kernel.FAILURE_CODE_APPLICATION
        ),
    )


def kernel_commands(
    issued: list[dict[str, Any]], reached: list[tuple[str, str]]
) -> list[kernel.Command]:
    """Every durable call, in issue order: the new commands as issued, with the
    steps answered from history (reached, but not issued) marked satisfied in
    their place among them.

    Raises:
        ProtocolRefusalError: A command the kernel protocol cannot carry.
    """
    issued_steps = {step for command in issued if (step := _step_of(command)) is not None}
    out: list[kernel.Command] = []
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


def _satisfied(step: tuple[str, str]) -> kernel.Command:
    """A call answered from history: its kind and id, no payloads."""
    kind, step_id = step
    if kind == "timer":
        return kernel.Command(satisfied=True, step_id=step_id, start_timer=kernel.StartTimer())
    if kind == "child":
        return kernel.Command(satisfied=True, start_child=kernel.StartChild(workflow_id=step_id))
    return kernel.Command(satisfied=True, step_id=step_id, run_task=kernel.RunTask())


_ORPHAN_POLICIES = {
    "terminate": kernel.ORPHAN_POLICY_TERMINATE,
    "cancel": kernel.ORPHAN_POLICY_CANCEL,
    "abandon": kernel.ORPHAN_POLICY_ABANDON,
}


def _kernel_command(command: dict[str, Any]) -> kernel.Command:
    """A new command, as the kernel protocol carries it. The values, and the
    defaults the context filled in, are the ones the worker sent before."""
    if len(command) != 1:
        raise ProtocolRefusalError(f"a command of no single kind: {sorted(command)}")
    ((kind, fields),) = command.items()
    if kind == "ScheduleTask":
        return kernel.Command(
            seq=int(fields.get("sequence") or 0),
            step_id=fields.get("task_id", ""),
            run_task=kernel.RunTask(
                task_type=fields.get("task_type", ""),
                task_queue=fields.get("task_queue") or "",
                input=[_payload(p) for p in fields.get("input") or []],
                attempt_timeout=_duration(fields.get("timeout")),
                heartbeat_timeout=_duration(fields.get("heartbeat_timeout")),
                queue_timeout=_duration(fields.get("queue_timeout")),
                retry_policy=_retry_policy(fields.get("retry_policy")),
            ),
        )
    if kind == "StartTimer":
        return kernel.Command(
            seq=int(fields.get("sequence") or 0),
            step_id=fields.get("timer_id", ""),
            start_timer=kernel.StartTimer(duration=_duration(fields.get("duration"))),
        )
    if kind == "StartChildWorkflow":
        orphan_policy = str(fields.get("orphan_policy") or "cancel").lower()
        if orphan_policy not in _ORPHAN_POLICIES:
            raise ProtocolRefusalError(f"unknown orphan policy '{orphan_policy}'")
        return kernel.Command(
            seq=int(fields.get("sequence") or 0),
            start_child=kernel.StartChild(
                workflow_id=fields.get("workflow_id", ""),
                workflow_type=fields.get("workflow_type", ""),
                task_queue=fields.get("task_queue") or "",
                input=[_payload(p) for p in fields.get("input") or []],
                execution_timeout=_duration(fields.get("timeout")),
                orphan_policy=_ORPHAN_POLICIES[orphan_policy],
                retry_policy=_retry_policy(fields.get("retry_policy")),
            ),
        )
    if kind == "CancelChildWorkflow":
        return kernel.Command(
            cancel_child=kernel.CancelChild(step_id=fields.get("workflow_id", ""))
        )
    if kind == "SendEvent":
        return kernel.Command(
            send_event=kernel.SendEvent(
                workflow_id=fields.get("workflow_id", ""),
                # Any run of the target receives it.
                run_id=fields.get("run_id") or "",
                name=fields.get("event_name", ""),
                payload=_payload(fields.get("payload") or {}),
            )
        )
    if kind == "RecordStepResult":
        if fields.get("failure"):
            raise ProtocolRefusalError("a recorded value that failed is never recorded by this SDK")
        return kernel.Command(
            step_id=fields.get("step_name", ""),
            record_value=kernel.RecordValue(
                outcome=kernel.Outcome(
                    value=kernel.Payload(data=bytes(fields.get("result") or [])),
                    end=kernel.END_COMPLETED,
                    attempts=int(fields.get("execution_attempt") or 1),
                )
            ),
        )
    if kind == "WaitForEvent":
        timeout_ms = fields.get("timeout_ms")
        return kernel.Command(
            step_id=fields.get("step_id", ""),
            await_event=kernel.AwaitEvent(
                name=fields.get("event_name", ""),
                timeout=(
                    duration_pb2.Duration(
                        seconds=timeout_ms // 1000, nanos=(timeout_ms % 1000) * 1_000_000
                    )
                    if isinstance(timeout_ms, int)
                    else None
                ),
            ),
        )
    if kind == "CompleteWorkflow":
        return kernel.Command(
            complete_run=kernel.CompleteRun(result=_payload(fields.get("result") or {}))
        )
    if kind == "CancelWorkflowExecution":
        return kernel.Command(end_cancelled=kernel.EndCancelled())
    if kind == "RestartFresh":
        return kernel.Command(
            restart_fresh=kernel.RestartFresh(
                workflow_type=fields.get("workflow_type") or "",
                task_queue=fields.get("task_queue") or "",
                input=[_payload(p) for p in fields.get("input") or []],
                execution_timeout=_duration(fields.get("timeout")),
                retry_policy=_retry_policy(fields.get("retry_policy")),
            )
        )
    raise ProtocolRefusalError(
        f"the workflow issued a command the kernel protocol does not carry: {kind}"
    )


def _payload(payload: dict[str, Any]) -> kernel.Payload:
    """A payload as the context holds it (bytes as lists of integers)."""
    return kernel.Payload(
        data=bytes(payload.get("data") or []),
        metadata={k: bytes(v) for k, v in (payload.get("metadata") or {}).items()},
    )


def _duration(duration: dict[str, Any] | None) -> duration_pb2.Duration | None:
    """A duration as the context holds it, ``{"secs", "nanos"}``; None for none."""
    if duration is None:
        return None
    return duration_pb2.Duration(
        seconds=int(duration.get("secs") or 0), nanos=int(duration.get("nanos") or 0)
    )


def _retry_policy(policy: dict[str, Any] | None) -> kernel.RetryPolicy | None:
    """A retry policy as the context normalizes it, in the kernel's names."""
    if policy is None:
        return None
    return kernel.RetryPolicy(
        # An omitted count takes the documented default; 0 means unlimited.
        max_attempts=int(
            DEFAULT_MAX_ATTEMPTS if policy.get("max_attempts") is None else policy["max_attempts"]
        ),
        first_delay=_duration(policy.get("initial_interval")),
        max_delay=_duration(policy.get("max_interval")),
        delay_multiplier=float(policy.get("backoff_coefficient") or 0.0),
        never_retry_types=list(policy.get("non_retryable_errors") or []),
    )


def _query_answer(response: dict[str, Any]) -> kernel.QueryAnswer:
    result = response.get("result") or {}
    if "Success" in result:
        return kernel.QueryAnswer(
            query_id=response.get("query_id", ""),
            value=_payload(result["Success"].get("output") or {}),
        )
    return kernel.QueryAnswer(
        query_id=response.get("query_id", ""),
        failure=kernel.Failure(message=(result.get("Failed") or {}).get("message", "")),
    )


def _update_answer(response: dict[str, Any]) -> kernel.UpdateAnswer:
    result = response.get("result") or {}
    answer = kernel.UpdateAnswer(update_id=response.get("update_id", ""))
    if "Completed" in result:
        answer.value.CopyFrom(_payload(result["Completed"].get("output") or {}))
    elif "Rejected" in result:
        answer.rejected.CopyFrom(kernel.Failure(message=result["Rejected"].get("message", "")))
    else:
        answer.failed.CopyFrom(
            kernel.Failure(message=(result.get("Failed") or {}).get("message", ""))
        )
    return answer


def _child_failed(message: str) -> dict[str, Any]:
    return {CHILD_FAILED_SENTINEL: True, "message": message}


class _Undecodable:
    pass


_UNDECODABLE: Any = _Undecodable()


def _decode_step_result(data: bytes) -> Any:
    """A recorded result: empty is None; bytes that are not JSON are not a result."""
    if not data:
        return None
    return _decode_json(data)


def _decode_json(data: bytes) -> Any:
    try:
        return json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return _UNDECODABLE


def _failure_code(failure: kernel.Failure, outcome: kernel.Outcome) -> str:
    """A step failure's code; with none, taken from how the step ended.

    A step that ended cancelled or timed out reads as such when its failure
    carries no code; any other is ``"unspecified"``.
    """
    if failure.code:
        # The protocol's name for it, in lower case without the prefix, so a
        # code the protocol adds reads as the protocol names it.
        with contextlib.suppress(ValueError):
            return kernel.FailureCode.Name(failure.code).removeprefix("FAILURE_CODE_").lower()
    if outcome.end == kernel.END_CANCELLED:
        return "cancelled"
    if outcome.end == kernel.END_TIMED_OUT:
        return "timed_out"
    return "unspecified"
