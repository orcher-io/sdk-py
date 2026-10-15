"""Task-related error types."""

from typing import Any

from orcher.errors.base import OrcherError
from orcher.errors.codes import ErrorCode, FailureCode

__all__ = [
    "TaskError",
]


class TaskError(OrcherError):
    """Errors related to task execution.

    Attributes:
        task_id: ID of the task that encountered the error.
        task_type: Type/name of the task.
        attempts: How many times the task ran before this error, when known.
        error_type: The failure's type as the engine recorded it: for a task
            that raised, the exception's class name. Empty when there is none.
        failure_code: Why the task failed, as the engine recorded it: one of
            the ``FailureCode`` values, such as ``"application"`` for a task
            that raised or ``"cancelled"`` for a cancelled one. It is a plain
            string, so a code newer than this SDK comes through as it is.
            ``"unspecified"`` when the engine gave none. (``code`` is the
            SDK's own ``ErrorCode``, as on every ``OrcherError``.)
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        task_id: str | None = None,
        task_type: str | None = None,
        attempts: int | None = None,
        error_type: str = "",
        failure_code: str = FailureCode.UNSPECIFIED,
        **kwargs: Any,
    ) -> None:
        self.task_id = task_id
        self.task_type = task_type
        self.attempts = attempts
        self.error_type = error_type
        self.failure_code = failure_code
        super().__init__(code, message, **kwargs)

    @property
    def is_cancelled(self) -> bool:
        """Whether the task failed because it was cancelled.

        True when the failure code is ``"cancelled"``, or when the error type
        is ``"Cancelled"``, which is how an engine that does not categorize
        failures records a cancellation.
        """
        return self.failure_code == FailureCode.CANCELLED or self.error_type == "Cancelled"

    @classmethod
    def execution_failed(
        cls,
        task_id: str,
        message: str,
        *,
        task_type: str | None = None,
        attempts: int | None = None,
        error_type: str = "",
        failure_code: str = FailureCode.UNSPECIFIED,
    ) -> "TaskError":
        """Create a task execution failed error.

        When the attempt count is known, the message is composed to match the
        Rust and TypeScript SDKs: ``Task 'name' failed after N attempts: reason``.
        ``error_type`` and ``failure_code`` are the failure's type and code as
        the engine recorded them.
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
            error_type=error_type,
            failure_code=failure_code,
        )

    @classmethod
    def timeout(cls, task_id: str, *, task_type: str | None = None) -> "TaskError":
        """Create a task timeout error."""
        return cls(
            ErrorCode.TASK_TIMEOUT,
            f"Task timed out: {task_id}",
            task_id=task_id,
            task_type=task_type,
            failure_code=FailureCode.TIMED_OUT,
        )

    @classmethod
    def cancelled(cls, task_id: str) -> "TaskError":
        """Create a task cancelled error."""
        return cls(
            ErrorCode.TASK_CANCELLED,
            f"Task was cancelled: {task_id}",
            task_id=task_id,
            failure_code=FailureCode.CANCELLED,
        )
