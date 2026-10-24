"""Kernel protocol messages from readable specs, and back, for tests.

Tests describe activations as dicts in a compact shape: bytes as lists of
integers, times as ``*_ms``, durations as ``{"secs", "nanos"}``, an enum as its
value's name in lower case without the type's prefix (``END_TIMED_OUT`` is
``"timed_out"``), and a ``oneof`` as the one key present. ``message`` builds the
``RunActivation`` such a dict describes; ``as_dict`` turns any message back into
that shape, with only the fields that are set, so a test can compare what the
worker answered with what it expects.
"""

from __future__ import annotations

import re
from typing import Any

from google.protobuf import duration_pb2, timestamp_pb2
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message

from orcher._proto import host_pb2 as host
from orcher._proto import kernel_pb2 as kernel


def _enum(enum_type: Any, prefix: str, name: str | int | None) -> int:
    if not name:
        return 0
    if isinstance(name, int):
        # A number this release may not name, as a newer peer would send it.
        return name
    return int(enum_type.Value(f"{prefix}{name.upper()}"))


def _timestamp(ms: int) -> timestamp_pb2.Timestamp:
    return timestamp_pb2.Timestamp(seconds=ms // 1000, nanos=(ms % 1000) * 1_000_000)


def payload_message(spec: dict[str, Any]) -> kernel.Payload:
    return kernel.Payload(
        data=bytes(spec.get("data") or []),
        metadata={k: bytes(v) for k, v in (spec.get("metadata") or {}).items()},
    )


def failure_message(spec: dict[str, Any]) -> kernel.Failure:
    return kernel.Failure(
        message=spec.get("message") or "",
        error_type=spec.get("error_type") or "",
        non_retryable=bool(spec.get("non_retryable")),
        code=_enum(kernel.FailureCode, "FAILURE_CODE_", spec.get("code")),
        details=bytes(spec.get("details") or []),
    )


def _outcome(spec: dict[str, Any]) -> kernel.Outcome:
    outcome = kernel.Outcome(
        end=_enum(kernel.End, "END_", spec.get("end")), attempts=int(spec.get("attempts") or 0)
    )
    if spec.get("value") is not None:
        outcome.value.CopyFrom(payload_message(spec["value"]))
    if spec.get("failure") is not None:
        outcome.failure.CopyFrom(failure_message(spec["failure"]))
    return outcome


def _resolution(spec: dict[str, Any]) -> kernel.Resolution:
    out = kernel.Resolution(
        step_id=spec.get("step_id") or "",
        replaying=bool(spec.get("replaying")),
        sequence=int(spec.get("sequence") or 0),
    )
    if spec.get("at_ms") is not None:
        out.at.CopyFrom(_timestamp(spec["at_ms"]))
    if "started" in spec:
        started = spec["started"]
        out.started.SetInParent()
        out.started.input.extend(payload_message(p) for p in started.get("input") or [])
        out.started.attempt = int(started.get("attempt") or 0)
        if started.get("started_at_ms") is not None:
            out.started.started_at.CopyFrom(_timestamp(started["started_at_ms"]))
        out.started.step_ids = int(started.get("step_ids") or 0)
        out.started.history_lineage = started.get("history_lineage") or ""
        out.started.random_seed = bytes(started.get("random_seed") or [])
    elif "step" in spec:
        out.step.outcome.CopyFrom(_outcome(spec["step"].get("outcome") or {}))
    elif "child" in spec:
        child = spec["child"]
        out.child.child_workflow_id = child.get("child_workflow_id") or ""
        out.child.child_run_id = child.get("child_run_id") or ""
        out.child.outcome.CopyFrom(_outcome(child.get("outcome") or {}))
    elif "child_started" in spec:
        out.child_started.child_workflow_id = spec["child_started"].get("child_workflow_id") or ""
    elif "timer" in spec:
        out.timer.SetInParent()
    elif "event" in spec:
        event = spec["event"]
        out.event.name = event.get("name") or ""
        out.event.event_id = event.get("event_id") or ""
        if event.get("payload") is not None:
            out.event.payload.CopyFrom(payload_message(event["payload"]))
    elif "update" in spec:
        update = spec["update"]
        out.update.update_id = update.get("update_id") or ""
        out.update.name = update.get("name") or ""
        out.update.args.extend(payload_message(p) for p in update.get("args") or [])
    elif "cancel" in spec:
        out.cancel.reason = (spec["cancel"] or {}).get("reason") or ""
    # Any other kind is one this SDK does not know: a resolution of no kind.
    return out


def message(spec: dict[str, Any]) -> kernel.RunActivation:
    """The ``RunActivation`` a spec describes."""
    run = spec.get("run") or {}
    return kernel.RunActivation(
        activation_id=bytes(spec.get("activation_id") or []),
        run=kernel.RunRef(**{k: v for k, v in run.items() if v is not None}),
        resolutions=[_resolution(r) for r in spec.get("resolutions") or []],
        queries=[
            kernel.QueryAsked(
                query_id=q.get("query_id") or "",
                name=q.get("name") or "",
                args=[payload_message(p) for p in q.get("args") or []],
            )
            for q in spec.get("queries") or []
        ],
    )


def _prefix(enum_name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", enum_name).upper() + "_"


def _value(field: FieldDescriptor, value: Any) -> Any:
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        return as_dict(value)
    if field.type == FieldDescriptor.TYPE_BYTES:
        return list(value)
    if field.type == FieldDescriptor.TYPE_ENUM:
        name = field.enum_type.values_by_number[value].name
        return name.removeprefix(_prefix(field.enum_type.name)).lower()
    return value


def _repeated(field: FieldDescriptor) -> bool:
    # `is_repeated` replaced `label` in newer protobuf releases.
    if hasattr(field, "is_repeated"):
        return bool(field.is_repeated)
    return bool(field.label == FieldDescriptor.LABEL_REPEATED)


def as_dict(msg: Message) -> Any:
    """A message in the compact shape, with only the fields that are set."""
    if isinstance(msg, duration_pb2.Duration):
        return {"secs": msg.seconds, "nanos": msg.nanos}
    if isinstance(msg, timestamp_pb2.Timestamp):
        return msg.seconds * 1000 + msg.nanos // 1_000_000
    if isinstance(msg, kernel.Payload):
        return {"data": list(msg.data), "metadata": {k: list(v) for k, v in msg.metadata.items()}}
    out: dict[str, Any] = {}
    for field, value in msg.ListFields():
        if field.message_type is not None and field.message_type.GetOptions().map_entry:
            value_field = field.message_type.fields_by_name["value"]
            out[field.name] = {k: _value(value_field, v) for k, v in value.items()}
        elif _repeated(field):
            out[field.name] = [_value(field, v) for v in value]
        else:
            out[field.name] = _value(field, value)
    return out


class Answer(dict[str, Any]):
    """A worker's answer to an activation as a compact dict, with the message
    it was made from, for what needs the encoded answer."""

    message: kernel.RunActivationResult

    def encoded(self) -> bytes:
        return self.message.SerializeToString()


def answer_of(result: kernel.RunActivationResult) -> Answer:
    answer = Answer(as_dict(result))
    # A list of commands travels wrapped in a message of its own.
    for key in ("commands", "calls_before_failure"):
        if key in answer:
            answer[key] = answer[key].get("commands", [])
    answer.message = result
    return answer


async def answered(worker: Any, spec: dict[str, Any]) -> Answer:
    """``worker``'s answer to the activation ``spec`` describes."""
    return answer_of(await worker.run_activation(message(spec)))


def _millis_duration(ms: int) -> duration_pb2.Duration:
    return duration_pb2.Duration(seconds=ms // 1000, nanos=(ms % 1000) * 1_000_000)


def task_message(spec: dict[str, Any]) -> host.TaskAssignment:
    """The ``TaskAssignment`` a flat spec describes: the run's fields beside
    the task's, the input as the bytes of its one payload, limits in ``*_ms``."""
    out = host.TaskAssignment(
        token=bytes(spec.get("token") or []),
        task_id=spec.get("task_id") or "",
        task_type=spec.get("task_type") or "",
        run=kernel.RunRef(
            workflow_id=spec.get("workflow_id") or "",
            run_id=spec.get("run_id") or "",
            task_queue=spec.get("task_queue") or "",
            namespace=spec.get("namespace") or "",
        ),
        attempt=int(spec.get("attempt") or 0),
    )
    if spec.get("input") is not None:
        out.input.append(kernel.Payload(data=bytes(spec["input"])))
    if spec.get("attempt_timeout_ms") is not None:
        out.attempt_timeout.CopyFrom(_millis_duration(spec["attempt_timeout_ms"]))
    if spec.get("heartbeat_timeout_ms") is not None:
        out.heartbeat_timeout.CopyFrom(_millis_duration(spec["heartbeat_timeout_ms"]))
    return out
