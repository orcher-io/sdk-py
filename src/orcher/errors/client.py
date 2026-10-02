"""Client-related error types."""

from orcher.errors.base import OrcherError
from orcher.errors.codes import ErrorCode

__all__ = [
    "ClientError",
]


class ClientError(OrcherError):
    """Errors related to client operations."""

    @classmethod
    def connection_failed(cls, address: str, reason: str) -> "ClientError":
        """Create a connection failed error."""
        return cls(
            ErrorCode.CLIENT_CONNECTION_FAILED,
            f"Failed to connect to {address}: {reason}",
        )

    @classmethod
    def unauthorized(cls, message: str = "Unauthorized") -> "ClientError":
        """Create an unauthorized error."""
        return cls(ErrorCode.CLIENT_UNAUTHORIZED, message)

    @classmethod
    def timeout(cls, operation: str) -> "ClientError":
        """Create a request timeout error."""
        return cls(
            ErrorCode.CLIENT_REQUEST_TIMEOUT,
            f"Request timed out: {operation}",
        )

    @classmethod
    def invalid_request(cls, message: str) -> "ClientError":
        """Create an invalid request error."""
        return cls(ErrorCode.CLIENT_INVALID_REQUEST, message)
