"""
Test data builder utilities for ORCHER testing.

This module provides fluent builder utilities for creating test fixtures with
sensible defaults and easy customization. These builders make it ergonomic to
set up test data without boilerplate.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

from orcher.testing.types import (
    ChildWorkflowExecution,
    ErrorMockStrategy,
    ExecutionStatus,
    ExecutionTrace,
    FixedMockStrategy,
    FunctionMockStrategy,
    SequenceMockStrategy,
    TaskExecution,
    TimerExecution,
    WorkflowEvent,
    WorkflowExecutionOptions,
    WorkflowQuery,
)

__all__ = [
    "WorkflowExecutionBuilder",
    "TaskMockBuilder",
    "ExecutionTraceBuilder",
]


class WorkflowExecutionBuilder:
    """
    Builder for WorkflowExecutionOptions.

    Creates workflow execution options with sensible defaults for testing.
    Default configuration:
    - Random workflow_id (UUID)
    - task_queue: "test-queue"
    - timeout_ms: 30000 (30 seconds)
    - initial_state: {} (empty dict)
    - trace: True

    Example:
        >>> options = (
        ...     WorkflowExecutionBuilder()
        ...     .with_workflow_id("wf-123")
        ...     .with_timeout_ms(60000)
        ...     .with_initial_state({"count": 0})
        ...     .build()
        ... )
    """

    def __init__(self) -> None:
        """Initialize builder with default values."""
        self._workflow_id: str | None = None
        self._task_queue: str = "test-queue"
        self._timeout_ms: int = 30000
        self._initial_state: dict[str, Any] = {}
        self._trace: bool = True

    def with_workflow_id(self, workflow_id: str) -> WorkflowExecutionBuilder:
        """
        Set the workflow ID.

        Args:
            workflow_id: Workflow identifier

        Returns:
            Self for chaining
        """
        self._workflow_id = workflow_id
        return self

    def with_task_queue(self, task_queue: str) -> WorkflowExecutionBuilder:
        """
        Set the task queue.

        Args:
            task_queue: Task queue name

        Returns:
            Self for chaining
        """
        self._task_queue = task_queue
        return self

    def with_timeout_ms(self, timeout_ms: int) -> WorkflowExecutionBuilder:
        """
        Set the execution timeout in milliseconds.

        Args:
            timeout_ms: Timeout in milliseconds

        Returns:
            Self for chaining
        """
        self._timeout_ms = timeout_ms
        return self

    def with_initial_state(self, state: dict[str, Any]) -> WorkflowExecutionBuilder:
        """
        Set the initial workflow state.

        Args:
            state: Initial state dictionary

        Returns:
            Self for chaining
        """
        self._initial_state = state
        return self

    def with_trace(self, trace: bool) -> WorkflowExecutionBuilder:
        """
        Set whether to enable execution tracing.

        Args:
            trace: Enable tracing if True

        Returns:
            Self for chaining
        """
        self._trace = trace
        return self

    def build(self) -> WorkflowExecutionOptions:
        """
        Build the WorkflowExecutionOptions.

        Returns:
            Configured WorkflowExecutionOptions instance
        """
        workflow_id = self._workflow_id or f"wf-{uuid.uuid4()}"

        return WorkflowExecutionOptions(
            workflow_id=workflow_id,
            task_queue=self._task_queue,
            timeout_ms=self._timeout_ms,
            initial_state=self._initial_state.copy(),
            trace=self._trace,
        )


class TaskMockBuilder:
    """
    Builder for task mock configurations.

    Creates mock configurations that can be registered with MockTaskRegistry.
    Provides a fluent API for configuring different mock strategies.

    Example:
        >>> # Create a fixed value mock
        >>> task_name, strategy = (
        ...     TaskMockBuilder()
        ...     .for_task("charge_card")
        ...     .returns({"charge_id": "ch_123"})
        ...     .build()
        ... )
        >>> registry.register_mock(task_name, strategy)

        >>> # Create a sequence mock
        >>> task_name, strategy = (
        ...     TaskMockBuilder()
        ...     .for_task("api_call")
        ...     .returns_sequence([
        ...         {"status": "processing"},
        ...         {"status": "completed"}
        ...     ])
        ...     .build()
        ... )
    """

    def __init__(self) -> None:
        """Initialize builder."""
        self._task_name: str | None = None
        self._strategy: (
            FixedMockStrategy
            | SequenceMockStrategy
            | FunctionMockStrategy
            | ErrorMockStrategy
            | None
        ) = None

    def for_task(self, task_name: str) -> TaskMockBuilder:
        """
        Set the task name to mock.

        Args:
            task_name: Name of the task

        Returns:
            Self for chaining
        """
        self._task_name = task_name
        return self

    def returns(self, value: Any) -> TaskMockBuilder:
        """
        Configure mock to return a fixed value.

        Args:
            value: Value to return

        Returns:
            Self for chaining
        """
        self._strategy = FixedMockStrategy(value=value)
        return self

    def returns_sequence(self, values: list[Any]) -> TaskMockBuilder:
        """
        Configure mock to return a sequence of values.

        Args:
            values: List of values to return

        Returns:
            Self for chaining
        """
        self._strategy = SequenceMockStrategy(values=values, current_index=0)
        return self

    def throws(self, error: Exception) -> TaskMockBuilder:
        """
        Configure mock to throw an error.

        Args:
            error: Exception to throw

        Returns:
            Self for chaining
        """
        self._strategy = ErrorMockStrategy(error=error)
        return self

    def with_fn(self, fn: Callable[[Any], Any]) -> TaskMockBuilder:
        """
        Configure mock to use a custom function.

        Args:
            fn: Function to execute

        Returns:
            Self for chaining
        """
        self._strategy = FunctionMockStrategy(fn=fn)
        return self

    def build(
        self,
    ) -> tuple[
        str, FixedMockStrategy | SequenceMockStrategy | FunctionMockStrategy | ErrorMockStrategy
    ]:
        """
        Build the mock configuration.

        Returns:
            Tuple of (task_name, mock_strategy)

        Raises:
            ValueError: If task name or strategy not configured
        """
        if self._task_name is None:
            raise ValueError("Task name not set. Call for_task() first.")
        if self._strategy is None:
            raise ValueError("Mock strategy not set. Call returns(), throws(), or with_fn().")

        return (self._task_name, self._strategy)


class ExecutionTraceBuilder:
    """
    Builder for ExecutionTrace test fixtures.

    Creates execution traces with customizable components for assertion testing.
    Useful for verifying workflow execution results in tests.

    Default configuration:
    - workflow_id: random UUID
    - workflow_type: "test-workflow"
    - status: PENDING
    - Empty state, tasks, events, queries, child workflows, timers

    Example:
        >>> trace = (
        ...     ExecutionTraceBuilder()
        ...     .with_workflow_id("wf-123")
        ...     .with_workflow_type("OrderWorkflow")
        ...     .with_status(ExecutionStatus.COMPLETED)
        ...     .add_task(TaskExecution(
        ...         task_name="charge_card",
        ...         input={"amount": 99.99},
        ...         output={"charge_id": "ch_123"},
        ...         status="completed"
        ...     ))
        ...     .with_result({"order_id": "ord-456"})
        ...     .build()
        ... )
    """

    def __init__(self) -> None:
        """Initialize builder with default values."""
        self._workflow_id: str | None = None
        self._workflow_type: str = "test-workflow"
        self._status: ExecutionStatus = ExecutionStatus.PENDING
        self._state: dict[str, Any] = {}
        self._tasks_executed: list[TaskExecution] = []
        self._events_received: list[WorkflowEvent] = []
        self._queries_handled: list[WorkflowQuery] = []
        self._child_workflows: list[ChildWorkflowExecution] = []
        self._timers: list[TimerExecution] = []
        self._start_time: datetime | None = None
        self._end_time: datetime | None = None
        self._error: Exception | None = None
        self._result: Any | None = None

    def with_workflow_id(self, workflow_id: str) -> ExecutionTraceBuilder:
        """
        Set workflow ID.

        Args:
            workflow_id: Workflow identifier

        Returns:
            Self for chaining
        """
        self._workflow_id = workflow_id
        return self

    def with_workflow_type(self, workflow_type: str) -> ExecutionTraceBuilder:
        """
        Set workflow type.

        Args:
            workflow_type: Workflow type name

        Returns:
            Self for chaining
        """
        self._workflow_type = workflow_type
        return self

    def with_status(self, status: ExecutionStatus) -> ExecutionTraceBuilder:
        """
        Set execution status.

        Args:
            status: Execution status

        Returns:
            Self for chaining
        """
        self._status = status
        return self

    def with_state(self, state: dict[str, Any]) -> ExecutionTraceBuilder:
        """
        Set workflow state.

        Args:
            state: State dictionary

        Returns:
            Self for chaining
        """
        self._state = state
        return self

    def add_task(self, task: TaskExecution) -> ExecutionTraceBuilder:
        """
        Add a task execution record.

        Args:
            task: Task execution to add

        Returns:
            Self for chaining
        """
        self._tasks_executed.append(task)
        return self

    def add_event(self, event: WorkflowEvent) -> ExecutionTraceBuilder:
        """
        Add a workflow event record.

        Args:
            event: Event to add

        Returns:
            Self for chaining
        """
        self._events_received.append(event)
        return self

    def add_query(self, query: WorkflowQuery) -> ExecutionTraceBuilder:
        """
        Add a workflow query record.

        Args:
            query: Query to add

        Returns:
            Self for chaining
        """
        self._queries_handled.append(query)
        return self

    def add_child_workflow(self, child: ChildWorkflowExecution) -> ExecutionTraceBuilder:
        """
        Add a child workflow execution record.

        Args:
            child: Child workflow to add

        Returns:
            Self for chaining
        """
        self._child_workflows.append(child)
        return self

    def add_timer(self, timer: TimerExecution) -> ExecutionTraceBuilder:
        """
        Add a timer execution record.

        Args:
            timer: Timer to add

        Returns:
            Self for chaining
        """
        self._timers.append(timer)
        return self

    def with_start_time(self, start_time: datetime) -> ExecutionTraceBuilder:
        """
        Set execution start time.

        Args:
            start_time: Start timestamp

        Returns:
            Self for chaining
        """
        self._start_time = start_time
        return self

    def with_end_time(self, end_time: datetime) -> ExecutionTraceBuilder:
        """
        Set execution end time.

        Args:
            end_time: End timestamp

        Returns:
            Self for chaining
        """
        self._end_time = end_time
        return self

    def with_result(self, result: Any) -> ExecutionTraceBuilder:
        """
        Set workflow result.

        Args:
            result: Workflow result value

        Returns:
            Self for chaining
        """
        self._result = result
        return self

    def with_error(self, error: Exception) -> ExecutionTraceBuilder:
        """
        Set workflow error.

        Args:
            error: Exception that occurred

        Returns:
            Self for chaining
        """
        self._error = error
        return self

    def build(self) -> ExecutionTrace:
        """
        Build the ExecutionTrace.

        Returns:
            Configured ExecutionTrace instance
        """
        workflow_id = self._workflow_id or f"wf-{uuid.uuid4()}"

        return ExecutionTrace(
            workflow_id=workflow_id,
            workflow_type=self._workflow_type,
            status=self._status,
            state=self._state.copy(),
            tasks_executed=list(self._tasks_executed),
            events_received=list(self._events_received),
            queries_handled=list(self._queries_handled),
            child_workflows=list(self._child_workflows),
            timers=list(self._timers),
            start_time=self._start_time,
            end_time=self._end_time,
            error=self._error,
            result=self._result,
        )
