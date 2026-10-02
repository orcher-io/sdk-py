"""
Core types for ORCHER testing utilities.

This module defines foundational types used throughout the testing system,
including execution traces, test options, and mock configurations.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ExecutionStatus(Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


@dataclass
class TaskExecution:
    """Record of a task execution."""

    task_name: str
    input: Any
    output: Any | None = None
    error: Exception | None = None
    status: str = "pending"  # pending, running, completed, failed
    start_time: datetime | None = None
    end_time: datetime | None = None
    attempt: int = 1


@dataclass
class TaskCall:
    """Record of a task call."""

    task_name: str
    input: Any
    output: Any | None = None
    error: Exception | None = None
    timestamp: datetime = field(default_factory=datetime.now)
    duration_ms: float | None = None


@dataclass
class WorkflowEvent:
    """Record of a workflow event."""

    name: str
    data: Any
    timestamp: datetime
    event_id: str


@dataclass
class WorkflowQuery:
    """Record of a workflow query."""

    name: str
    args: Any | None = None
    result: Any = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ChildWorkflowExecution:
    """Record of a child workflow execution."""

    workflow_id: str
    workflow_type: str
    input: Any
    result: Any | None = None
    status: ExecutionStatus = ExecutionStatus.PENDING
    parent_workflow_id: str = ""


@dataclass
class TimerExecution:
    """Record of a timer execution."""

    timer_id: str
    duration_ms: int
    created_at: datetime
    fire_at: datetime
    fired: bool = False
    cancelled: bool = False


@dataclass
class ExecutionTrace:
    """
    Execution trace for a workflow run.

    Contains complete history of workflow execution including tasks,
    events, queries, and state changes.
    """

    workflow_id: str
    workflow_type: str
    status: ExecutionStatus = ExecutionStatus.PENDING
    state: dict[str, Any] = field(default_factory=dict)
    tasks_executed: list[TaskExecution] = field(default_factory=list)
    events_received: list[WorkflowEvent] = field(default_factory=list)
    queries_handled: list[WorkflowQuery] = field(default_factory=list)
    child_workflows: list[ChildWorkflowExecution] = field(default_factory=list)
    timers: list[TimerExecution] = field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None
    error: Exception | None = None
    result: Any | None = None

    def get_state(self, key: str) -> Any:
        """Get state value by key."""
        return self.state.get(key)

    def set_state(self, key: str, value: Any) -> None:
        """Set state value."""
        self.state[key] = value


@dataclass
class WorkflowTestEnvOptions:
    """Options for creating a test workflow environment."""

    __test__ = False  # Prevent pytest from collecting this class

    namespace: str = "test"
    task_queue: str = "test-queue"
    capture_snapshots: bool = False
    enable_tracing: bool = True
    initial_time: datetime | None = None
    timeout_ms: int = 30000
    strict_mode: bool = False


# Alias of WorkflowTestEnvOptions.
TestEnvOptions = WorkflowTestEnvOptions


@dataclass
class WorkflowExecutionOptions:
    """Options for workflow execution in tests."""

    __test__ = False  # Prevent pytest from collecting this class

    workflow_id: str | None = None
    task_queue: str | None = None
    timeout_ms: int | None = None
    initial_state: dict[str, Any] | None = None
    trace: bool = True


# Alias of WorkflowExecutionOptions.
TestWorkflowOptions = WorkflowExecutionOptions


@dataclass
class TestEnvStats:
    """Test environment statistics."""

    workflows_executed: int = 0
    tasks_executed: int = 0
    events_sent: int = 0
    queries_handled: int = 0
    child_workflows_spawned: int = 0
    timers_created: int = 0
    total_execution_time_ms: float = 0.0


# Mock strategy types


@dataclass
class FixedMockStrategy:
    """Fixed value mock - returns same value every time."""

    value: Any

    @property
    def type(self) -> str:
        return "fixed"


@dataclass
class SequenceMockStrategy:
    """Sequence mock - returns different values for each call."""

    values: list[Any]
    current_index: int = 0

    @property
    def type(self) -> str:
        return "sequence"

    def next_value(self) -> Any:
        """Get next value in sequence."""
        if self.current_index >= len(self.values):
            raise IndexError(f"Mock sequence exhausted after {len(self.values)} calls")
        value = self.values[self.current_index]
        self.current_index += 1
        return value


@dataclass
class FunctionMockStrategy:
    """Function mock - executes custom function."""

    fn: Callable[[Any], Any]

    @property
    def type(self) -> str:
        return "function"


@dataclass
class ErrorMockStrategy:
    """Error mock - always throws error."""

    error: Exception

    @property
    def type(self) -> str:
        return "error"


MockStrategy = FixedMockStrategy | SequenceMockStrategy | FunctionMockStrategy | ErrorMockStrategy


@dataclass
class TaskMock:
    """Task mock configuration."""

    task_name: str
    strategy: MockStrategy
    call_count: int = 0
    calls: list[TaskCall] = field(default_factory=list)
