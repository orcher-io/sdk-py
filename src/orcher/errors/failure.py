"""Serializable failure types for workflow and task errors.

A Failure crosses workflow, task, and process boundaries intact, so the
receiving side can inspect its type, cause chain, and retryability.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Self, TypeVar

__all__ = [
    "FailureType",
    "ApplicationFailure",
    "TaskFailure",
    "WorkflowFailure",
    "TimeoutFailure",
    "CancelledFailure",
    "TerminatedFailure",
    "ChildWorkflowFailure",
    "Failure",
    "failure_from_exception",
]

T = TypeVar("T", bound="Failure")


class FailureType(Enum):
    """Kinds of failure a workflow or task can report."""

    APPLICATION = "APPLICATION"
    TASK = "TASK"
    WORKFLOW = "WORKFLOW"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    TERMINATED = "TERMINATED"
    CHILD_WORKFLOW = "CHILD_WORKFLOW"
    SERVER = "SERVER"
    UNKNOWN = "UNKNOWN"


@dataclass
class Failure:
    """Base failure type that can be serialized across boundaries.

    Unlike exceptions, failures are designed to be serialized and passed
    between workflows, tasks, and the server.

    Attributes:
        message: Human-readable error message.
        failure_type: The type of failure.
        source: Where the failure originated (e.g., "task:process_payment").
        stack_trace: Optional stack trace for debugging.
        cause: Optional nested failure that caused this one.
        details: Additional structured details about the failure.
        timestamp: When the failure occurred.
        retryable: Whether the operation that caused this failure can be retried.
    """

    message: str
    failure_type: FailureType = FailureType.UNKNOWN
    source: str = ""
    stack_trace: str = ""
    cause: Failure | None = None
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    retryable: bool = True

    def with_cause(self, cause: Failure) -> Failure:
        """Set the cause of this failure."""
        self.cause = cause
        return self

    def with_details(self, **kwargs: Any) -> Self:
        """Add details to the failure."""
        self.details.update(kwargs)
        return self

    def with_source(self, source: str) -> Self:
        """Set the source of this failure."""
        self.source = source
        return self

    def non_retryable(self) -> Self:
        """Mark this failure as non-retryable."""
        self.retryable = False
        return self

    @property
    def root_cause(self) -> Failure:
        """Get the root cause failure in the chain."""
        current = self
        while current.cause is not None:
            current = current.cause
        return current

    @property
    def failure_chain(self) -> list[Failure]:
        """Get the complete chain of failures."""
        chain: list[Failure] = [self]
        current: Failure = self
        while current.cause is not None:
            chain.append(current.cause)
            current = current.cause
        return chain

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dictionary for serialization."""
        result = {
            "message": self.message,
            "failure_type": self.failure_type.value,
            "source": self.source,
            "stack_trace": self.stack_trace,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "retryable": self.retryable,
        }
        if self.cause:
            result["cause"] = self.cause.to_dict()
        return result

    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any]) -> T:
        """Create a Failure from a dictionary."""
        cause_data = data.get("cause")
        cause = cls.from_dict(cause_data) if cause_data else None

        return cls(
            message=data.get("message", ""),
            failure_type=FailureType(data.get("failure_type", "UNKNOWN")),
            source=data.get("source", ""),
            stack_trace=data.get("stack_trace", ""),
            cause=cause,
            details=data.get("details", {}),
            timestamp=datetime.fromisoformat(data["timestamp"])
            if "timestamp" in data
            else datetime.now(),
            retryable=data.get("retryable", True),
        )

    def format(self, include_stack: bool = True) -> str:
        """Format the failure as a human-readable string."""
        lines = [f"[{self.failure_type.value}] {self.message}"]

        if self.source:
            lines.append(f"  Source: {self.source}")

        if self.details:
            lines.append(f"  Details: {self.details}")

        if include_stack and self.stack_trace:
            lines.append(f"  Stack trace:\n{self.stack_trace}")

        if self.cause:
            lines.append(f"  Caused by: {self.cause.format(include_stack)}")

        return "\n".join(lines)

    def __str__(self) -> str:
        return self.format(include_stack=False)

    def __repr__(self) -> str:
        return (
            f"Failure(message={self.message!r}, "
            f"type={self.failure_type.name}, "
            f"retryable={self.retryable})"
        )


@dataclass
class ApplicationFailure(Failure):
    """Failure from application code.

    Use this for business logic errors that should be reported to the caller.

    Example:
        >>> raise ApplicationFailure(
        ...     "Insufficient funds",
        ...     error_type="InsufficientFundsError",
        ... ).non_retryable()
    """

    error_type: str = ""  # Application-specific error type

    def __post_init__(self) -> None:
        self.failure_type = FailureType.APPLICATION

    @classmethod
    def from_error(
        cls,
        message: str,
        error_type: str = "",
        *,
        retryable: bool = True,
        **details: Any,
    ) -> ApplicationFailure:
        """Create an ApplicationFailure from an error message."""
        failure = cls(
            message=message,
            error_type=error_type,
            retryable=retryable,
        )
        if details:
            failure.with_details(**details)
        return failure


@dataclass
class TaskFailure(Failure):
    """Failure from a task execution.

    Contains information about the failed task including retry attempts.

    Attributes:
        task_id: ID of the failed task.
        task_type: Type/name of the failed task.
        attempt: Which attempt failed (1-indexed).
        max_attempts: Maximum configured attempts.
    """

    task_id: str = ""
    task_type: str = ""
    attempt: int = 1
    max_attempts: int = 1

    def __post_init__(self) -> None:
        self.failure_type = FailureType.TASK
        self.source = f"task:{self.task_type}" if self.task_type else "task"

    @property
    def is_final_attempt(self) -> bool:
        """Check if this was the final retry attempt."""
        return self.attempt >= self.max_attempts

    @classmethod
    def from_exception(
        cls,
        exception: Exception,
        task_id: str,
        task_type: str = "",
        attempt: int = 1,
        max_attempts: int = 1,
    ) -> TaskFailure:
        """Create a TaskFailure from an exception."""
        stack = "".join(
            traceback.format_exception(type(exception), exception, exception.__traceback__)
        )

        return cls(
            message=str(exception),
            task_id=task_id,
            task_type=task_type,
            attempt=attempt,
            max_attempts=max_attempts,
            stack_trace=stack,
            retryable=attempt < max_attempts,
        )


@dataclass
class WorkflowFailure(Failure):
    """Failure from a workflow execution.

    Attributes:
        workflow_id: ID of the failed workflow.
        workflow_type: Type/name of the failed workflow.
        run_id: Run ID of the failed execution.
    """

    workflow_id: str = ""
    workflow_type: str = ""
    run_id: str = ""

    def __post_init__(self) -> None:
        self.failure_type = FailureType.WORKFLOW
        self.source = f"workflow:{self.workflow_type}" if self.workflow_type else "workflow"
        self.retryable = False  # A failed workflow is not retried automatically.

    @classmethod
    def from_exception(
        cls,
        exception: Exception,
        workflow_id: str,
        workflow_type: str = "",
        run_id: str = "",
    ) -> WorkflowFailure:
        """Create a WorkflowFailure from an exception."""
        stack = "".join(
            traceback.format_exception(type(exception), exception, exception.__traceback__)
        )

        return cls(
            message=str(exception),
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            run_id=run_id,
            stack_trace=stack,
        )


@dataclass
class TimeoutFailure(Failure):
    """Failure due to a timeout.

    Attributes:
        timeout_type: Type of timeout (e.g., "start_to_close", "schedule_to_start").
        timeout_duration_ms: The timeout duration that was exceeded.
    """

    timeout_type: str = ""
    timeout_duration_ms: int = 0

    def __post_init__(self) -> None:
        self.failure_type = FailureType.TIMEOUT
        self.retryable = True  # A timeout may succeed on the next attempt.

    @classmethod
    def task_timeout(
        cls,
        task_id: str,
        task_type: str,
        timeout_type: str,
        timeout_duration_ms: int,
    ) -> TimeoutFailure:
        """Create a timeout failure for a task."""
        return cls(
            message=f"Task {task_type} timed out ({timeout_type})",
            source=f"task:{task_type}",
            timeout_type=timeout_type,
            timeout_duration_ms=timeout_duration_ms,
        ).with_details(task_id=task_id, task_type=task_type)

    @classmethod
    def workflow_timeout(
        cls,
        workflow_id: str,
        workflow_type: str,
        timeout_type: str,
        timeout_duration_ms: int,
    ) -> TimeoutFailure:
        """Create a timeout failure for a workflow."""
        failure = cls(
            message=f"Workflow {workflow_type} timed out ({timeout_type})",
            source=f"workflow:{workflow_type}",
            timeout_type=timeout_type,
            timeout_duration_ms=timeout_duration_ms,
        )
        failure.retryable = False
        return failure.with_details(workflow_id=workflow_id, workflow_type=workflow_type)


@dataclass
class CancelledFailure(Failure):
    """Failure due to cancellation.

    Attributes:
        cancelled_by: Who/what initiated the cancellation.
    """

    cancelled_by: str = ""

    def __post_init__(self) -> None:
        self.failure_type = FailureType.CANCELLED
        self.retryable = False

    @classmethod
    def task_cancelled(cls, task_id: str, reason: str = "") -> CancelledFailure:
        """Create a cancellation failure for a task."""
        return cls(
            message=reason or "Task was cancelled",
            source=f"task:{task_id}",
        ).with_details(task_id=task_id)

    @classmethod
    def workflow_cancelled(cls, workflow_id: str, reason: str = "") -> CancelledFailure:
        """Create a cancellation failure for a workflow."""
        return cls(
            message=reason or "Workflow was cancelled",
            source=f"workflow:{workflow_id}",
        ).with_details(workflow_id=workflow_id)


@dataclass
class TerminatedFailure(Failure):
    """Failure due to termination.

    Unlike cancellation, termination is immediate and gives the workflow or
    task no chance to clean up.

    Attributes:
        terminated_by: Who/what initiated the termination.
        reason: Reason for termination.
    """

    terminated_by: str = ""
    reason: str = ""

    def __post_init__(self) -> None:
        self.failure_type = FailureType.TERMINATED
        self.retryable = False

    @classmethod
    def workflow_terminated(
        cls,
        workflow_id: str,
        reason: str = "",
    ) -> TerminatedFailure:
        """Create a termination failure for a workflow."""
        return cls(
            message=reason or "Workflow was terminated",
            source=f"workflow:{workflow_id}",
            reason=reason,
        ).with_details(workflow_id=workflow_id)


@dataclass
class ChildWorkflowFailure(Failure):
    """Failure from a child workflow.

    Wraps the failure from a child workflow execution.

    Attributes:
        child_workflow_id: ID of the failed child workflow.
        child_workflow_type: Type of the failed child workflow.
        child_run_id: Run ID of the failed child workflow.
    """

    child_workflow_id: str = ""
    child_workflow_type: str = ""
    child_run_id: str = ""

    def __post_init__(self) -> None:
        self.failure_type = FailureType.CHILD_WORKFLOW
        self.source = f"child_workflow:{self.child_workflow_type}"
        self.retryable = False

    @classmethod
    def from_child_failure(
        cls,
        child_workflow_id: str,
        child_workflow_type: str,
        child_run_id: str,
        cause: Failure,
    ) -> ChildWorkflowFailure:
        """Create from a child workflow's failure."""
        return cls(
            message=f"Child workflow {child_workflow_type} failed: {cause.message}",
            child_workflow_id=child_workflow_id,
            child_workflow_type=child_workflow_type,
            child_run_id=child_run_id,
            cause=cause,
        )


def failure_from_exception(
    exception: BaseException | Failure,
    *,
    source: str = "",
    retryable: bool = True,
) -> Failure:
    """Convert a Python exception to a Failure.

    Args:
        exception: The exception to convert, or an existing Failure.
        source: Optional source identifier.
        retryable: Whether the failure should be marked as retryable.
            Ignored for programming errors (``ValueError``, ``TypeError``,
            ``AttributeError``, ``KeyError``, ``IndexError``,
            ``AssertionError``), which are never retryable.

    Returns:
        A Failure instance representing the exception.

    Example:
        >>> try:
        ...     risky_operation()
        ... except Exception as e:
        ...     failure = failure_from_exception(e, source="risky_operation")
    """
    if isinstance(exception, Failure):
        return exception

    stack = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))

    # Programming errors fail the same way on every attempt.
    non_retryable_types = (
        ValueError,
        TypeError,
        AttributeError,
        KeyError,
        IndexError,
        AssertionError,
    )
    if isinstance(exception, non_retryable_types):
        retryable = False

    return Failure(
        message=str(exception),
        failure_type=FailureType.APPLICATION,
        source=source,
        stack_trace=stack,
        retryable=retryable,
        details={"exception_type": type(exception).__name__},
    )
