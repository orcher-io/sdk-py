"""Task execution types for ORCHER Python SDK.

This module provides the TaskExecution dataclass and related types
for identifying and tracking task executions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

__all__ = [
    "TaskExecution",
    "TaskExecutionStatus",
    "TaskExecutionInfo",
    "TaskExecutionResult",
]


class TaskExecutionStatus(Enum):
    """Status of a task execution."""

    UNKNOWN = "unknown"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


@dataclass(frozen=True)
class TaskExecution:
    """Identifier for a task execution.

    A task execution is uniquely identified by the combination of
    workflow execution and task ID.

    Attributes:
        workflow_id: ID of the workflow that scheduled this task.
        run_id: Run ID of the workflow execution.
        task_id: Unique identifier for this task execution.
    """

    workflow_id: str
    run_id: str
    task_id: str

    def __str__(self) -> str:
        return f"{self.workflow_id}:{self.run_id}:{self.task_id}"

    def __repr__(self) -> str:
        return (
            f"TaskExecution(workflow_id={self.workflow_id!r}, "
            f"run_id={self.run_id!r}, task_id={self.task_id!r})"
        )


@dataclass
class TaskExecutionInfo:
    """Detailed information about a task execution.

    This provides comprehensive information about a task execution,
    including its current status, timing information, and metadata.

    Attributes:
        execution: The task execution identifier.
        task_type: The type/name of the task.
        task_queue: The task queue the task runs on.
        namespace: The namespace of the task.
        status: Current execution status.
        attempt: Current attempt number (1-based).
        max_attempts: Maximum number of attempts.
        scheduled_time: When the task was scheduled.
        start_time: When the current attempt started.
        close_time: When the task completed (if finished).
        last_heartbeat_time: When the last heartbeat was received.
        retry_policy: The retry policy for this task.
    """

    execution: TaskExecution
    task_type: str
    task_queue: str
    namespace: str
    status: TaskExecutionStatus = TaskExecutionStatus.UNKNOWN
    attempt: int = 1
    max_attempts: int = 1
    scheduled_time: datetime | None = None
    start_time: datetime | None = None
    close_time: datetime | None = None
    last_heartbeat_time: datetime | None = None
    retry_policy: dict[str, Any] | None = None

    @property
    def task_id(self) -> str:
        """Get the task ID."""
        return self.execution.task_id

    @property
    def workflow_id(self) -> str:
        """Get the workflow ID."""
        return self.execution.workflow_id

    @property
    def is_running(self) -> bool:
        """Check if the task is still running."""
        return self.status == TaskExecutionStatus.RUNNING

    @property
    def is_completed(self) -> bool:
        """Check if the task completed successfully."""
        return self.status == TaskExecutionStatus.COMPLETED

    @property
    def is_failed(self) -> bool:
        """Check if the task failed."""
        return self.status == TaskExecutionStatus.FAILED

    @property
    def is_finished(self) -> bool:
        """Check if the task has finished (any terminal state)."""
        return self.status in (
            TaskExecutionStatus.COMPLETED,
            TaskExecutionStatus.FAILED,
            TaskExecutionStatus.CANCELLED,
            TaskExecutionStatus.TIMED_OUT,
        )

    @property
    def can_retry(self) -> bool:
        """Check if the task can be retried."""
        return self.attempt < self.max_attempts and self.status == TaskExecutionStatus.FAILED


@dataclass
class TaskExecutionResult:
    """Result of a task execution.

    Attributes:
        execution: The task execution identifier.
        status: Final execution status.
        result: The task result (if completed successfully).
        error: Error information (if failed).
        attempt: The attempt number that produced this result.
        start_time: When the attempt started.
        end_time: When the attempt ended.
        duration_ms: Duration of the attempt in milliseconds.
    """

    execution: TaskExecution
    status: TaskExecutionStatus
    result: Any = None
    error: dict[str, Any] | None = None
    attempt: int = 1
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_ms: int | None = None

    @property
    def is_success(self) -> bool:
        """Check if the execution was successful."""
        return self.status == TaskExecutionStatus.COMPLETED

    @property
    def is_failure(self) -> bool:
        """Check if the execution failed."""
        return self.status in (
            TaskExecutionStatus.FAILED,
            TaskExecutionStatus.CANCELLED,
            TaskExecutionStatus.TIMED_OUT,
        )

    def get_error_message(self) -> str | None:
        """Get the error message if the task failed."""
        if self.error:
            return self.error.get("message")
        return None

    def get_error_type(self) -> str | None:
        """Get the error type if the task failed."""
        if self.error:
            return self.error.get("type")
        return None
