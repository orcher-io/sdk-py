"""Base error types for the SDK."""

from typing import Any

from orcher.errors.codes import ErrorCode, ErrorSeverity

__all__ = [
    "OrcherError",
    "ConfigurationError",
]


class OrcherError(Exception):
    """Base exception for all ORCHER SDK errors.

    Attributes:
        code: Error code identifying the error type.
        message: Human-readable error message.
        severity: Error severity level.
        cause: Original exception that caused this error.
        details: Additional error details.
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        cause: Exception | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.severity = severity
        self.cause = cause
        self.details = details or {}
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        return f"[{self.code.name}] {self.message}"

    @property
    def is_retryable(self) -> bool:
        """Whether this error may succeed if retried.

        Cancellation, termination, non-determinism, authorization, and
        configuration errors are never retryable; all other codes are.
        """
        non_retryable = {
            ErrorCode.WORKFLOW_NON_DETERMINISTIC,
            ErrorCode.WORKFLOW_CANCELLED,
            ErrorCode.WORKFLOW_TERMINATED,
            ErrorCode.TASK_CANCELLED,
            ErrorCode.CLIENT_UNAUTHORIZED,
            ErrorCode.CONFIGURATION_INVALID,
        }
        return self.code not in non_retryable

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"code={self.code.name}, "
            f"message={self.message!r}, "
            f"severity={self.severity.name}"
            f")"
        )


class ConfigurationError(OrcherError):
    """Errors related to SDK configuration."""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(ErrorCode.CONFIGURATION_INVALID, message, **kwargs)

    @classmethod
    def missing_required(cls, field: str) -> "ConfigurationError":
        """Create a missing required field error."""
        return cls(f"Missing required configuration field: {field}")

    @classmethod
    def invalid_value(cls, field: str, value: Any, reason: str) -> "ConfigurationError":
        """Create an invalid value error."""
        return cls(f"Invalid value for {field}={value!r}: {reason}")
