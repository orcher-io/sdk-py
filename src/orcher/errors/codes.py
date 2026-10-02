"""Error codes and severity levels for SDK errors."""

from enum import Enum

__all__ = [
    "ErrorCode",
    "ErrorSeverity",
]


class ErrorCode(Enum):
    """Numeric error codes, grouped by category in blocks of 1000."""

    # Workflow errors (1xxx)
    WORKFLOW_EXECUTION_FAILED = 1001
    WORKFLOW_TIMEOUT = 1002
    WORKFLOW_NON_DETERMINISTIC = 1003
    WORKFLOW_REPLAY_ERROR = 1004
    WORKFLOW_NOT_FOUND = 1005
    WORKFLOW_ALREADY_EXISTS = 1006
    WORKFLOW_CANCELLED = 1007
    WORKFLOW_TERMINATED = 1008
    WORKFLOW_QUERY_FAILED = 1009
    WORKFLOW_EVENT_FAILED = 1010
    WORKFLOW_INVALID_STATE = 1011

    # Task errors (2xxx)
    TASK_EXECUTION_FAILED = 2001
    TASK_TIMEOUT = 2002
    TASK_RETRY_LIMIT_EXCEEDED = 2003
    TASK_CANCELLED = 2004
    TASK_NOT_FOUND = 2005
    TASK_HEARTBEAT_TIMEOUT = 2006
    TASK_INVALID_INPUT = 2007
    TASK_SCHEDULE_FAILED = 2008

    # Client errors (3xxx)
    CLIENT_CONNECTION_FAILED = 3001
    CLIENT_UNAUTHORIZED = 3002
    CLIENT_REQUEST_TIMEOUT = 3003
    CLIENT_INVALID_REQUEST = 3004

    # Service errors (4xxx)
    SERVICE_NOT_RUNNING = 4001
    SERVICE_STARTUP_ERROR = 4002
    SERVICE_SHUTDOWN_ERROR = 4003

    # Configuration errors (5xxx)
    CONFIGURATION_INVALID = 5001

    # Internal errors (9xxx)
    INTERNAL_ERROR = 9001
    SERIALIZATION_ERROR = 9002
    DESERIALIZATION_ERROR = 9003


class ErrorSeverity(Enum):
    """Severity levels for errors."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
