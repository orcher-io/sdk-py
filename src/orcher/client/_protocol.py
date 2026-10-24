"""Client calls through sdk-core's client protocol (``orcher.client.v1``).

Every client call is one encoded ``ClientCall`` handed to the native
``ClientConnection``, answered with one encoded ``ClientAnswer``. This module
builds the calls, reads the answers, and raises a refused call as the error
the public API documents, keyed off the protocol's error code rather than any
message text.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from google.protobuf import duration_pb2, timestamp_pb2

from orcher._proto import client_pb2 as pb
from orcher.errors.client import ClientError
from orcher.errors.workflow import WorkflowError
from orcher.types import (
    ParentExecutionInfo,
    RetryPolicy,
    WorkflowExecution,
    WorkflowExecutionConfig,
    WorkflowExecutionDescription,
    WorkflowExecutionInfo,
    WorkflowListPage,
    WorkflowStatus,
)

__all__ = [
    "call",
    "decode_json",
    "duration",
    "encode_json",
]


def _json_default(value: Any) -> Any:
    # JSON has no bytes type, so bytes become a standard base64 string.
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def encode_json(value: Any) -> bytes:
    """``value`` as the JSON a workflow, query, update or event reads."""
    return json.dumps(value, default=_json_default, separators=(",", ":")).encode()


def decode_json(data: bytes) -> Any:
    """A JSON value as it came back; ``None`` for nothing at all."""
    return json.loads(data) if data else None


def duration(td: timedelta) -> duration_pb2.Duration:
    """``td`` as a protocol duration."""
    d = duration_pb2.Duration()
    d.FromTimedelta(td)
    return d


async def call(
    connection: Any, message: pb.ClientCall, *, workflow_id: str = "unknown"
) -> pb.ClientAnswer:
    """Make ``message`` on ``connection`` and return its answer.

    Raises:
        WorkflowError: If the call failed about its workflow.
        ClientError: If it failed otherwise.
    """
    answer = pb.ClientAnswer.FromString(await connection.call(message.SerializeToString()))
    if answer.WhichOneof("answer") == "error":
        raise _error(answer.error, workflow_id)
    return answer


def _error(error: pb.ClientError, workflow_id: str) -> Exception:
    code = error.code
    message = error.message
    if code == pb.ERROR_CODE_WORKFLOW_NOT_FOUND:
        return WorkflowError.not_found(workflow_id)
    if code == pb.ERROR_CODE_WORKFLOW_ALREADY_EXISTS:
        return WorkflowError.already_exists(workflow_id, run_id=error.run_id or None)
    if code == pb.ERROR_CODE_WORKFLOW_CANCELED:
        return WorkflowError.cancelled(workflow_id)
    if code == pb.ERROR_CODE_WORKFLOW_TERMINATED:
        return WorkflowError.terminated(workflow_id, message)
    if code in (pb.ERROR_CODE_WORKFLOW_EXECUTION_FAILED, pb.ERROR_CODE_WORKFLOW_TIMEOUT):
        return WorkflowError.execution_failed(workflow_id, message)
    if code == pb.ERROR_CODE_CONNECTION_FAILED:
        return ClientError.connection_failed("unknown", message)
    if code == pb.ERROR_CODE_TIMEOUT:
        return ClientError.timeout(message)
    return ClientError.invalid_request(message)


# ---- Reading answers ----

_STATUSES = {
    pb.WORKFLOW_STATUS_RUNNING: WorkflowStatus.RUNNING,
    pb.WORKFLOW_STATUS_COMPLETED: WorkflowStatus.COMPLETED,
    pb.WORKFLOW_STATUS_FAILED: WorkflowStatus.FAILED,
    pb.WORKFLOW_STATUS_CANCELLED: WorkflowStatus.CANCELLED,
    pb.WORKFLOW_STATUS_TERMINATED: WorkflowStatus.TERMINATED,
    pb.WORKFLOW_STATUS_TIMED_OUT: WorkflowStatus.TIMED_OUT,
    pb.WORKFLOW_STATUS_RESTARTED_FRESH: WorkflowStatus.RESTARTED_FRESH,
}

_STATUS_VALUES = {status: value for value, status in _STATUSES.items()}


def status(value: int) -> WorkflowStatus:
    """A protocol status; an unknown one reads as running."""
    return _STATUSES.get(value, WorkflowStatus.RUNNING)


def status_value(status: WorkflowStatus) -> int:
    """A status as the protocol numbers it."""
    return _STATUS_VALUES[status]


def _time(message: Any, name: str) -> datetime | None:
    if not message.HasField(name):
        return None
    t: timestamp_pb2.Timestamp = getattr(message, name)
    return t.ToDatetime(tzinfo=UTC)


def _seconds(message: Any, name: str) -> int | None:
    if not message.HasField(name):
        return None
    return int(getattr(message, name).ToTimedelta().total_seconds())


def _parent(summary: pb.WorkflowSummary) -> ParentExecutionInfo | None:
    if not summary.HasField("parent"):
        return None
    return ParentExecutionInfo(
        namespace=summary.parent.namespace,
        workflow_id=summary.parent.workflow.workflow_id,
        execution_id=summary.parent.workflow.run_id,
    )


def execution_info(summary: pb.WorkflowSummary) -> WorkflowExecutionInfo:
    """A run as a list or search reports it."""
    return WorkflowExecutionInfo(
        workflow_id=summary.workflow.workflow_id,
        execution_id=summary.workflow.run_id,
        workflow_type=summary.workflow_type,
        task_queue=summary.task_queue,
        namespace=summary.namespace,
        status=status(summary.status),
        start_time=_time(summary, "start_time"),
        close_time=_time(summary, "close_time"),
        execution_duration_seconds=_seconds(summary, "execution_duration"),
        journal_length=summary.journal_length,
        parent_execution=_parent(summary),
        labels=dict(summary.labels),
        memo=dict(summary.annotations),
        tags=list(summary.tags),
        started_by=summary.started_by,
        attempt=summary.attempt,
        cron_schedule=summary.cron_schedule,
        state_transition_count=summary.state_transition_count,
    )


def list_page(page: pb.WorkflowPage) -> WorkflowListPage:
    """One page of runs."""
    return WorkflowListPage(
        executions=[execution_info(summary) for summary in page.workflows],
        next_page_token=bytes(page.next_page_token),
    )


def _retry_policy(policy: pb.RetryPolicy) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=policy.max_attempts,
        initial_interval=policy.initial_interval.ToTimedelta(),
        max_interval=policy.max_interval.ToTimedelta(),
        backoff_coefficient=policy.backoff_coefficient,
        non_retryable_error_types=list(policy.non_retryable_errors),
    )


def description(d: pb.WorkflowDescription) -> WorkflowExecutionDescription:
    """A run in full."""
    summary = d.summary
    config = None
    if d.HasField("config"):
        c = d.config
        config = WorkflowExecutionConfig(
            task_queue=c.task_queue,
            execution_timeout_seconds=_seconds(c, "execution_timeout"),
            run_timeout_seconds=_seconds(c, "run_timeout"),
            default_task_timeout_seconds=_seconds(c, "task_timeout"),
            retry_policy=_retry_policy(c.retry_policy) if c.HasField("retry_policy") else None,
            cron_schedule=c.cron_schedule,
        )
    return WorkflowExecutionDescription(
        execution=WorkflowExecution(
            workflow_id=summary.workflow.workflow_id,
            run_id=summary.workflow.run_id,
        ),
        workflow_type=summary.workflow_type,
        task_queue=summary.task_queue,
        status=status(summary.status),
        start_time=_time(summary, "start_time"),
        close_time=_time(summary, "close_time"),
        journal_length=summary.journal_length,
        attempt=summary.attempt,
        labels=dict(summary.labels),
        memo=dict(summary.annotations),
        tags=list(summary.tags),
        started_by=summary.started_by,
        cron_schedule=summary.cron_schedule,
        state_transition_count=summary.state_transition_count,
        parent_execution=_parent(summary),
        pending_tasks=d.pending_tasks,
        pending_timers=d.pending_timers,
        pending_events=d.pending_events,
        execution_config=config,
    )
