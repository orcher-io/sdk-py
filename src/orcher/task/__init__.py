"""Task execution module for ORCHER Python SDK.

This module provides components for task execution:

- TaskContext: The execution context passed to task functions
- TaskInfo: Metadata about the current task execution
- CancellationToken: Token for checking and handling task cancellation
- TaskExecution: Identifier for a task execution
- TaskExecutionInfo: Detailed information about a task execution
- TaskExecutionResult: Result of a task execution
"""

from orcher.task.cancellation import CancellationToken
from orcher.task.context import TaskContext
from orcher.task.execution import (
    TaskExecution,
    TaskExecutionInfo,
    TaskExecutionResult,
    TaskExecutionStatus,
)
from orcher.task.info import TaskInfo
from orcher.task.reference import TaskReference

__all__ = [
    # Context
    "TaskContext",
    "TaskInfo",
    # Cancellation
    "CancellationToken",
    # Execution types
    "TaskExecution",
    "TaskExecutionStatus",
    "TaskExecutionInfo",
    "TaskExecutionResult",
    # Reference
    "TaskReference",
]
