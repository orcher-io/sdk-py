"""Error types raised by the SDK.

The hierarchy mirrors the one in the Rust core. It includes:

- base error types (``OrcherError`` and its subclasses),
- failure types, which serialize errors across process boundaries, and
- error context and chaining helpers.
"""

from orcher.errors.base import ConfigurationError, OrcherError
from orcher.errors.client import ClientError
from orcher.errors.codes import ErrorCode, ErrorSeverity
from orcher.errors.context import (
    ContextualError,
    ErrorChain,
    ErrorContext,
    capture_exception,
    format_error_chain,
    with_context,
)
from orcher.errors.failure import (
    ApplicationFailure,
    CancelledFailure,
    ChildWorkflowFailure,
    Failure,
    FailureType,
    TaskFailure,
    TerminatedFailure,
    TimeoutFailure,
    WorkflowFailure,
    failure_from_exception,
)
from orcher.errors.service import WorkerError
from orcher.errors.task import TaskError
from orcher.errors.workflow import WorkflowError, WorkflowSuspendedError

__all__ = [
    # Error codes and severity
    "ErrorCode",
    "ErrorSeverity",
    # Base error types
    "OrcherError",
    "WorkflowError",
    "TaskError",
    "ClientError",
    "WorkerError",
    "ConfigurationError",
    # Error context
    "ErrorContext",
    "ErrorChain",
    "ContextualError",
    "with_context",
    "capture_exception",
    "format_error_chain",
    # Failure types
    "FailureType",
    "Failure",
    "ApplicationFailure",
    "TaskFailure",
    "WorkflowFailure",
    "TimeoutFailure",
    "CancelledFailure",
    "TerminatedFailure",
    "ChildWorkflowFailure",
    "failure_from_exception",
    # Control flow
    "WorkflowSuspendedError",
]
