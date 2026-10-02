"""Task-related error types."""

from typing import Any

from orcher.errors.base import OrcherError
from orcher.errors.codes import ErrorCode

__all__ = [
    "TaskError",
]


class TaskError(OrcherError):
    """Errors related to task execution.

    Attributes:
        task_id: ID of the task that encountered the error.
        task_type: Type/name of the task.
        attempts: How many times the task ran before this error, when known.
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        task_id: str | None = None,
        task_type: str | None = None,
        attempts: int | None = None,
        **kwargs: Any,
    ) -> None:
        self.task_id = task_id
        self.task_type = task_type
        self.attempts = attempts
        super().__init__(code, message, **kwargs)

    @classmethod
    def execution_failed(
        cls,
        task_id: str,
        message: str,
        *,
        task_type: str | None = None,
        attempts: int | None = None,
    ) -> "TaskError":
        """Create a task execution failed error.

        When the attempt count is known, the message is composed to match the
        Rust and TypeScript SDKs: ``Task 'name' failed after N attempts: reason``.
        """
        text = (
            f"Task '{task_type}' failed after {attempts} attempts: {message}"
            if task_type is not None and attempts is not None
            else message
        )
        return cls(
            ErrorCode.TASK_EXECUTION_FAILED,
            text,
            task_id=task_id,
            task_type=task_type,
            attempts=attempts,
        )

    @classmethod
    def timeout(cls, task_id: str, *, task_type: str | None = None) -> "TaskError":
        """Create a task timeout error."""
        return cls(
            ErrorCode.TASK_TIMEOUT,
            f"Task timed out: {task_id}",
            task_id=task_id,
            task_type=task_type,
        )

    @classmethod
    def cancelled(cls, task_id: str) -> "TaskError":
        """Create a task cancelled error."""
        return cls(
            ErrorCode.TASK_CANCELLED,
            f"Task was cancelled: {task_id}",
            task_id=task_id,
        )
