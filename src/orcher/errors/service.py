"""Service-related error types."""

from orcher.errors.base import OrcherError
from orcher.errors.codes import ErrorCode

__all__ = [
    "WorkerError",
]


class WorkerError(OrcherError):
    """Errors related to Service (worker) operations."""

    @classmethod
    def not_running(cls) -> "WorkerError":
        """Create a service not running error."""
        return cls(ErrorCode.SERVICE_NOT_RUNNING, "Service is not running")

    @classmethod
    def startup_failed(cls, reason: str) -> "WorkerError":
        """Create a service startup error."""
        return cls(ErrorCode.SERVICE_STARTUP_ERROR, f"Service startup failed: {reason}")

    @classmethod
    def shutdown_failed(cls, reason: str) -> "WorkerError":
        """Create a service shutdown error."""
        return cls(ErrorCode.SERVICE_SHUTDOWN_ERROR, f"Service shutdown failed: {reason}")
