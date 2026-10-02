"""Workflow command types for the Orcher Python SDK.

This module defines the commands that workflows can issue during execution.
Commands are recorded in the execution journal and replayed during recovery.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Any

__all__ = [
    "CommandType",
    "WorkflowCommand",
    "ScheduleTaskCommand",
    "StartTimerCommand",
    "StartChildWorkflowCommand",
    "SendEventCommand",
    "CancelChildWorkflowCommand",
    "CompleteWorkflowCommand",
    "FailWorkflowCommand",
    "RestartFreshCommand",
]


class CommandType(Enum):
    """Types of workflow commands."""

    SCHEDULE_TASK = "schedule_task"
    START_TIMER = "start_timer"
    START_CHILD_WORKFLOW = "start_child_workflow"
    SEND_EVENT = "send_event"
    CANCEL_CHILD_WORKFLOW = "cancel_child_workflow"
    COMPLETE_WORKFLOW = "complete_workflow"
    FAIL_WORKFLOW = "fail_workflow"
    RESTART_FRESH = "restart_fresh"
    WAIT_FOR_EVENT = "wait_for_event"


@dataclass
class WorkflowCommand:
    """Base class for workflow commands.

    Commands represent actions that a workflow wants to take. They are
    recorded during execution and can be replayed during recovery.
    """

    command_type: CommandType
    command_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert command to dictionary for serialization."""
        return {
            "type": self.command_type.value,
            "command_id": self.command_id,
        }


@dataclass
class ScheduleTaskCommand(WorkflowCommand):
    """Command to schedule a task for execution.

    Attributes:
        task_name: Name of the task to execute.
        task_id: Unique identifier for this task execution.
        input: Input data for the task.
        retry_policy: Optional retry policy override.
        timeout_seconds: Optional execution timeout.
        heartbeat_timeout_seconds: Optional heartbeat timeout.
    """

    task_name: str = ""
    task_id: str = ""
    input: Any = None
    retry_policy: dict[str, Any] | None = None
    timeout_seconds: float | None = None
    heartbeat_timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        self.command_type = CommandType.SCHEDULE_TASK

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "task_name": self.task_name,
                "task_id": self.task_id,
                "input": self.input,
                "retry_policy": self.retry_policy,
                "timeout_seconds": self.timeout_seconds,
                "heartbeat_timeout_seconds": self.heartbeat_timeout_seconds,
            }
        )
        return result


@dataclass
class StartTimerCommand(WorkflowCommand):
    """Command to start a durable timer.

    Attributes:
        timer_id: Unique identifier for the timer.
        duration: How long the timer should run.
    """

    timer_id: str = ""
    duration: timedelta = field(default_factory=lambda: timedelta(seconds=0))

    def __post_init__(self) -> None:
        self.command_type = CommandType.START_TIMER

    @property
    def duration_ms(self) -> int:
        """Get duration in milliseconds."""
        return int(self.duration.total_seconds() * 1000)

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "timer_id": self.timer_id,
                "duration_ms": self.duration_ms,
            }
        )
        return result


@dataclass
class StartChildWorkflowCommand(WorkflowCommand):
    """Command to start a child workflow.

    Attributes:
        workflow_type: Type/name of the child workflow.
        workflow_id: Unique identifier for the child workflow.
        args: Arguments to pass to the child workflow.
        task_queue: Optional task queue (defaults to parent's queue).
        namespace: Optional namespace (defaults to parent's namespace).
    """

    workflow_type: str = ""
    workflow_id: str = ""
    args: tuple = ()
    task_queue: str | None = None
    namespace: str | None = None

    def __post_init__(self) -> None:
        self.command_type = CommandType.START_CHILD_WORKFLOW

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "workflow_type": self.workflow_type,
                "workflow_id": self.workflow_id,
                "args": list(self.args),
                "task_queue": self.task_queue,
                "namespace": self.namespace,
            }
        )
        return result


@dataclass
class SendEventCommand(WorkflowCommand):
    """Command to send an event to another workflow.

    Attributes:
        target_workflow_id: ID of the workflow to send the event to.
        target_run_id: Optional run ID of the target workflow.
        event_name: Name of the event.
        payload: Event payload data.
    """

    target_workflow_id: str = ""
    target_run_id: str | None = None
    event_name: str = ""
    payload: Any = None

    def __post_init__(self) -> None:
        self.command_type = CommandType.SEND_EVENT

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "target_workflow_id": self.target_workflow_id,
                "target_run_id": self.target_run_id,
                "event_name": self.event_name,
                "payload": self.payload,
            }
        )
        return result


@dataclass
class CancelChildWorkflowCommand(WorkflowCommand):
    """Command to cancel a child workflow.

    Attributes:
        child_workflow_id: ID of the child workflow to cancel.
    """

    child_workflow_id: str = ""

    def __post_init__(self) -> None:
        self.command_type = CommandType.CANCEL_CHILD_WORKFLOW

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "child_workflow_id": self.child_workflow_id,
            }
        )
        return result


@dataclass
class CompleteWorkflowCommand(WorkflowCommand):
    """Command to complete the workflow with a result.

    Attributes:
        result: The workflow result.
    """

    result: Any = None

    def __post_init__(self) -> None:
        self.command_type = CommandType.COMPLETE_WORKFLOW

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "result": self.result,
            }
        )
        return result


@dataclass
class FailWorkflowCommand(WorkflowCommand):
    """Command to fail the workflow with an error.

    Attributes:
        error_message: Error message.
        error_type: Type of the error.
        details: Additional error details.
    """

    error_message: str = ""
    error_type: str = ""
    details: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        self.command_type = CommandType.FAIL_WORKFLOW

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "error_message": self.error_message,
                "error_type": self.error_type,
                "details": self.details,
            }
        )
        return result


@dataclass
class RestartFreshCommand(WorkflowCommand):
    """Command to restart the workflow as a fresh run, with a fresh history.

    This is used for long-running workflows that need to reset their
    history to avoid unbounded growth. ``ctx.restart_fresh()`` issues it.

    Attributes:
        input: Input for the fresh run.
        workflow_type: Workflow type for the fresh run; None keeps the current one.
        task_queue: Task queue for the fresh run; None keeps the current one.
    """

    input: Any = None
    workflow_type: str | None = None
    task_queue: str | None = None

    def __post_init__(self) -> None:
        self.command_type = CommandType.RESTART_FRESH

    def to_dict(self) -> dict[str, Any]:
        result = super().to_dict()
        result.update(
            {
                "input": self.input,
                "workflow_type": self.workflow_type,
                "task_queue": self.task_queue,
            }
        )
        return result
