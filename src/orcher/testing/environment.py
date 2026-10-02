"""
Test Workflow Environment for ORCHER Python SDK.

This module provides the TestWorkflowEnvironment class, which is the primary
entry point for testing workflows in-memory without a running server.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from datetime import datetime, timedelta
from typing import Any, TypeVar

from orcher.testing.mocks import MockTaskBuilder, MockTaskRegistry
from orcher.testing.time_controller import TimeController
from orcher.testing.types import (
    ExecutionStatus,
    ExecutionTrace,
    TaskExecution,
    TestEnvOptions,
    TestEnvStats,
    TestWorkflowOptions,
)

TInput = TypeVar("TInput")
TResult = TypeVar("TResult")


class TestWorkflowEnvironment:
    """
    Test environment for in-memory workflow execution.

    Provides a complete testing harness for workflows including:
    - In-memory workflow execution without a server
    - Task mocking with fluent API
    - Time control for deterministic testing
    - Execution history tracking
    - State inspection and assertions

    Example:
        >>> from orcher.testing import TestWorkflowEnvironment
        >>> from myapp.workflows import OrderWorkflow
        >>>
        >>> async def test_order_workflow():
        ...     env = TestWorkflowEnvironment()
        ...
        ...     # Mock task responses
        ...     env.mock_task("charge_card").returns({"charge_id": "ch_123"})
        ...     env.mock_task("reserve_inventory").returns({"reserved": True})
        ...
        ...     # Execute workflow
        ...     result = await env.execute_workflow(
        ...         OrderWorkflow,
        ...         {"order_id": "order-123", "amount": 99.99}
        ...     )
        ...
        ...     # Assertions
        ...     assert result["status"] == "completed"
        ...     env.assert_task_called("charge_card", times=1)
    """

    __test__ = False  # Prevent pytest from collecting this class

    def __init__(self, options: TestEnvOptions | None = None) -> None:
        """
        Create a new test workflow environment.

        Args:
            options: Environment configuration options

        Example:
            >>> env = TestWorkflowEnvironment(TestEnvOptions(
            ...     namespace="test",
            ...     task_queue="test-queue",
            ...     timeout_ms=5000,
            ... ))
        """
        self._options = options or TestEnvOptions()
        self._mock_registry = MockTaskRegistry()
        self._time_controller = TimeController(self._options.initial_time)
        self._traces: dict[str, ExecutionTrace] = {}
        self._stats = TestEnvStats()

    @classmethod
    def create(cls, options: TestEnvOptions | None = None) -> TestWorkflowEnvironment:
        """
        Factory method to create a test environment.

        Args:
            options: Environment configuration options

        Returns:
            New TestWorkflowEnvironment instance

        Example:
            >>> env = TestWorkflowEnvironment.create(TestEnvOptions(
            ...     namespace="test",
            ...     initial_time=datetime(2025, 1, 1)
            ... ))
        """
        return cls(options)

    # =========================================================================
    # Workflow Execution
    # =========================================================================

    async def execute_workflow(
        self,
        workflow_class: Any,
        input_data: Any = None,
        options: TestWorkflowOptions | None = None,
    ) -> Any:
        """
        Execute a workflow with given input.

        The input reaches the workflow the way a worker passes it: a dict is
        spread as keyword arguments, ``None`` passes nothing, and any other
        value is passed as one positional argument.

        Args:
            workflow_class: A ``@workflow`` function, or a ``@workflow`` class
                with a ``run`` method
            input_data: Input data for the workflow
            options: Execution options

        Returns:
            Workflow result

        Raises:
            Exception: If workflow execution fails

        Example:
            >>> result = await env.execute_workflow(
            ...     OrderWorkflow,
            ...     {"order_id": "123", "amount": 99.99},
            ...     TestWorkflowOptions(workflow_id="test-wf-1")
            ... )
        """
        options = options or TestWorkflowOptions()
        workflow_id = options.workflow_id or f"test-{uuid.uuid4().hex[:8]}"

        workflow_name = getattr(workflow_class, "__orcher_workflow__", None)
        workflow_type = workflow_name.name if workflow_name else workflow_class.__name__

        trace = ExecutionTrace(
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            status=ExecutionStatus.RUNNING,
            start_time=self._time_controller.now(),
            state=dict(options.initial_state or {}),
        )
        self._traces[workflow_id] = trace

        start_time = datetime.now()

        try:
            # The test context routes every task call through the mock registry.
            context = TestWorkflowContext(
                workflow_id=workflow_id,
                workflow_type=workflow_type,
                mock_registry=self._mock_registry,
                time_controller=self._time_controller,
                trace=trace,
                stats=self._stats,
            )

            args, kwargs = _workflow_arguments(input_data)
            if isinstance(workflow_class, type):
                workflow_instance = workflow_class()
                if not hasattr(workflow_instance, "run"):
                    raise ValueError(
                        f"Workflow class {workflow_class.__name__} has no 'run' method"
                    )
                result = workflow_instance.run(context, *args, **kwargs)
            else:
                result = workflow_class(context, *args, **kwargs)
            if inspect.isawaitable(result):
                result = await result

            trace.status = ExecutionStatus.COMPLETED
            trace.result = result
            trace.end_time = self._time_controller.now()

            self._stats.workflows_executed += 1
            self._stats.total_execution_time_ms += (
                datetime.now() - start_time
            ).total_seconds() * 1000

            return result

        except Exception as e:
            trace.status = ExecutionStatus.FAILED
            trace.error = e
            trace.end_time = self._time_controller.now()

            self._stats.workflows_executed += 1
            self._stats.total_execution_time_ms += (
                datetime.now() - start_time
            ).total_seconds() * 1000

            raise

    async def start_workflow(
        self,
        workflow_class: type[Any],
        input_data: Any,
        options: TestWorkflowOptions | None = None,
    ) -> asyncio.Task[Any]:
        """
        Start a workflow without waiting for result.

        Returns a task that can be awaited later.

        Args:
            workflow_class: Workflow class
            input_data: Workflow input
            options: Execution options

        Returns:
            Task that resolves with workflow result

        Example:
            >>> task = await env.start_workflow(LongWorkflow, input_data)
            >>> # Do other things...
            >>> await env.advance_time(5000)
            >>> result = await task
        """
        return asyncio.create_task(self.execute_workflow(workflow_class, input_data, options))

    # =========================================================================
    # Task Mocking
    # =========================================================================

    def mock_task(self, task_name: str) -> MockTaskBuilder[Any, Any]:
        """
        Create a mock for a task.

        Returns a fluent builder for configuring mock behavior.

        Args:
            task_name: Task name to mock

        Returns:
            MockTaskBuilder for configuring the mock

        Example:
            >>> # Fixed value
            >>> env.mock_task("charge_card").returns({"charge_id": "ch_123"})

            >>> # Sequence
            >>> env.mock_task("fetch_page").returns_sequence([
            ...     {"page": 1, "data": [...]},
            ...     {"page": 2, "data": [...]},
            ... ])

            >>> # Function
            >>> env.mock_task("calc_tax").with_fn(lambda amt: amt * 0.08)

            >>> # Error
            >>> env.mock_task("failing").throws(RuntimeError("Down"))
        """
        return MockTaskBuilder(task_name, self._mock_registry)

    def is_task_mocked(self, task_name: str) -> bool:
        """
        Check if a task is mocked.

        Args:
            task_name: Task name

        Returns:
            True if task has a mock
        """
        return self._mock_registry.has_mock(task_name)

    def get_task_call_count(self, task_name: str) -> int:
        """
        Get task call count.

        Args:
            task_name: Task name

        Returns:
            Number of times task was called
        """
        return self._mock_registry.get_call_count(task_name)

    # =========================================================================
    # Time Control
    # =========================================================================

    async def advance_time(self, ms: int) -> None:
        """
        Advance time by specified milliseconds.

        Fires all timers that expire during advancement.

        Args:
            ms: Milliseconds to advance

        Example:
            >>> await env.advance_time(5000)  # Advance 5 seconds
        """
        await self._time_controller.advance(ms)

    async def advance_time_to(self, target_time: datetime) -> None:
        """
        Advance time to specific datetime.

        Args:
            target_time: Target time

        Example:
            >>> await env.advance_time_to(datetime(2025, 1, 1, 12, 0, 0))
        """
        await self._time_controller.advance_to(target_time)

    def get_current_time(self) -> datetime:
        """
        Get current time in test environment.

        Returns:
            Current simulated time
        """
        return self._time_controller.now()

    def set_current_time(self, time: datetime) -> None:
        """
        Set current time to specific value.

        Args:
            time: New current time
        """
        self._time_controller.set_time(time)

    # =========================================================================
    # Execution Inspection
    # =========================================================================

    def get_execution_trace(self, workflow_id: str) -> ExecutionTrace | None:
        """
        Get execution trace for a workflow.

        Args:
            workflow_id: Workflow ID

        Returns:
            ExecutionTrace or None if not found
        """
        return self._traces.get(workflow_id)

    def get_all_execution_traces(self) -> dict[str, ExecutionTrace]:
        """
        Get all execution traces.

        Returns:
            Dictionary of workflow_id -> ExecutionTrace
        """
        return dict(self._traces)

    def get_workflow_state(
        self,
        workflow_id: str,
        key: str | None = None,
    ) -> Any:
        """
        Get workflow state.

        Args:
            workflow_id: Workflow ID
            key: State key (optional, returns all state if omitted)

        Returns:
            State value, state dict, or None
        """
        trace = self._traces.get(workflow_id)
        if trace is None:
            return None

        if key is not None:
            return trace.get_state(key)
        return dict(trace.state)

    # =========================================================================
    # Assertions
    # =========================================================================

    def assert_task_called(
        self,
        task_name: str,
        times: int | None = None,
    ) -> None:
        """
        Assert that a task was called.

        Args:
            task_name: Task name
            times: Expected call count (optional)

        Raises:
            AssertionError: If assertion fails

        Example:
            >>> env.assert_task_called("charge_card")
            >>> env.assert_task_called("charge_card", times=3)
        """
        if times is None:
            self._mock_registry.verify_called(task_name)
        else:
            self._mock_registry.verify_called_times(task_name, times)

    def assert_task_called_with(self, task_name: str, expected_input: Any) -> None:
        """
        Assert that a task was called with specific input.

        Args:
            task_name: Task name
            expected_input: Expected input

        Raises:
            AssertionError: If assertion fails

        Example:
            >>> env.assert_task_called_with("charge_card", {"amount": 99.99})
        """
        self._mock_registry.verify_called_with(task_name, expected_input)

    def assert_task_not_called(self, task_name: str) -> None:
        """
        Assert that a task was never called.

        Args:
            task_name: Task name

        Raises:
            AssertionError: If task was called

        Example:
            >>> env.assert_task_not_called("refund")
        """
        self._mock_registry.verify_not_called(task_name)

    def assert_workflow_completed(self, workflow_id: str) -> None:
        """
        Assert that a workflow completed successfully.

        Args:
            workflow_id: Workflow ID

        Raises:
            AssertionError: If workflow did not complete

        Example:
            >>> env.assert_workflow_completed("test-wf-1")
        """
        trace = self._traces.get(workflow_id)
        if trace is None:
            raise AssertionError(f"Workflow '{workflow_id}' not found")

        if trace.status != ExecutionStatus.COMPLETED:
            raise AssertionError(
                f"Expected workflow '{workflow_id}' to be completed, "
                f"but status is {trace.status.value}"
            )

    def assert_workflow_failed(
        self,
        workflow_id: str,
        error_message: str | None = None,
    ) -> None:
        """
        Assert that a workflow failed.

        Args:
            workflow_id: Workflow ID
            error_message: Optional expected error message substring

        Raises:
            AssertionError: If workflow did not fail

        Example:
            >>> env.assert_workflow_failed("test-wf-1")
            >>> env.assert_workflow_failed("test-wf-1", "Payment failed")
        """
        trace = self._traces.get(workflow_id)
        if trace is None:
            raise AssertionError(f"Workflow '{workflow_id}' not found")

        if trace.status != ExecutionStatus.FAILED:
            raise AssertionError(
                f"Expected workflow '{workflow_id}' to be failed, "
                f"but status is {trace.status.value}"
            )

        if error_message is not None and trace.error is not None:
            actual_message = str(trace.error)
            if error_message not in actual_message:
                raise AssertionError(
                    f"Expected workflow '{workflow_id}' to fail with message "
                    f"containing '{error_message}', but got: {actual_message}"
                )

    def assert_state_equals(
        self,
        workflow_id: str,
        key: str,
        expected_value: Any,
    ) -> None:
        """
        Assert that workflow state has specific value.

        Args:
            workflow_id: Workflow ID
            key: State key
            expected_value: Expected value

        Raises:
            AssertionError: If state doesn't match

        Example:
            >>> env.assert_state_equals("test-wf-1", "counter", 5)
        """
        trace = self._traces.get(workflow_id)
        if trace is None:
            raise AssertionError(f"Workflow '{workflow_id}' not found")

        actual_value = trace.get_state(key)
        if actual_value != expected_value:
            raise AssertionError(
                f"Expected state['{key}'] to be {expected_value!r}, but got {actual_value!r}"
            )

    # =========================================================================
    # Statistics
    # =========================================================================

    def get_stats(self) -> TestEnvStats:
        """
        Get test environment statistics.

        Returns:
            Copy of current statistics
        """
        return TestEnvStats(
            workflows_executed=self._stats.workflows_executed,
            tasks_executed=self._stats.tasks_executed,
            events_sent=self._stats.events_sent,
            queries_handled=self._stats.queries_handled,
            child_workflows_spawned=self._stats.child_workflows_spawned,
            timers_created=self._stats.timers_created,
            total_execution_time_ms=self._stats.total_execution_time_ms,
        )

    def get_summary(self) -> str:
        """
        Get summary of test environment.

        Returns:
            Human-readable summary
        """
        traces = self._traces
        completed = sum(1 for t in traces.values() if t.status == ExecutionStatus.COMPLETED)
        failed = sum(1 for t in traces.values() if t.status == ExecutionStatus.FAILED)

        lines = [
            "Test Environment Summary",
            "=" * 40,
            f"Namespace: {self._options.namespace}",
            f"Task Queue: {self._options.task_queue}",
            "",
            "Statistics:",
            f"- Workflows Executed: {self._stats.workflows_executed} "
            f"({completed} completed, {failed} failed)",
            f"- Tasks Executed: {self._stats.tasks_executed}",
            f"- Events Sent: {self._stats.events_sent}",
            f"- Timers Created: {self._stats.timers_created}",
            f"- Total Execution Time: {self._stats.total_execution_time_ms:.1f}ms",
            "",
            f"Current Time: {self._time_controller.now().isoformat()}",
            f"Pending Timers: {self._time_controller.get_pending_timer_count()}",
            "",
            self._mock_registry.get_summary(),
        ]

        return "\n".join(lines)

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def cleanup(self) -> None:
        """
        Clean up test environment.

        Clears all mocks, traces, and timers.

        Example:
            >>> # In test teardown
            >>> await env.cleanup()
        """
        self._mock_registry.clear()
        self._traces.clear()
        self._time_controller.clear_timers()

    def reset(self) -> None:
        """
        Reset test environment to initial state.

        Keeps configuration but clears all execution data.

        Example:
            >>> env.reset()
        """
        self._mock_registry.clear()
        self._traces.clear()
        self._time_controller.reset(self._options.initial_time)
        self._stats = TestEnvStats()


class TestWorkflowContext:
    """
    Workflow context for test execution.

    Provides the same interface as WorkflowContext but routes
    task execution through the mock registry.
    """

    def __init__(
        self,
        workflow_id: str,
        workflow_type: str,
        mock_registry: MockTaskRegistry,
        time_controller: TimeController,
        trace: ExecutionTrace,
        stats: TestEnvStats,
    ) -> None:
        """Initialize the test context."""
        self._workflow_id = workflow_id
        self._workflow_type = workflow_type
        self._mock_registry = mock_registry
        self._time_controller = time_controller
        self._trace = trace
        self._stats = stats
        self._state: dict[str, Any] = trace.state

    @property
    def workflow_id(self) -> str:
        """Get workflow ID."""
        return self._workflow_id

    @property
    def workflow_type(self) -> str:
        """Get workflow type."""
        return self._workflow_type

    async def execute_task(
        self,
        task_ref: Any,
        input_data: Any = None,
        /,
        *,
        retry_policy: Any = None,
        timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        queue_timeout: timedelta | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a task (via mock registry).

        Called the way ``WorkflowContext.execute_task`` is: the task's
        arguments as keyword arguments, which the mock receives as one dict.
        A single positional ``input_data`` is also accepted. The retry policy
        and timeouts are accepted and ignored, since a mock neither retries
        nor times out.

        Args:
            task_ref: A ``@task`` function, a TaskReference or a task name
            input_data: Task input, when no keyword arguments are given
            **kwargs: The task's arguments

        Returns:
            Task result from mock
        """
        task_name = _task_name(task_ref)
        if kwargs:
            input_data = kwargs

        task_exec = TaskExecution(
            task_name=task_name,
            input=input_data,
            status="running",
            start_time=self._time_controller.now(),
            attempt=1,
        )
        self._trace.tasks_executed.append(task_exec)

        try:
            result = await self._mock_registry.execute(task_name, input_data)
            task_exec.status = "completed"
            task_exec.output = result
            task_exec.end_time = self._time_controller.now()
            self._stats.tasks_executed += 1
            return result
        except Exception as e:
            task_exec.status = "failed"
            task_exec.error = e
            task_exec.end_time = self._time_controller.now()
            self._stats.tasks_executed += 1
            raise

    async def sleep(self, duration: timedelta | float) -> None:
        """
        Sleep for the specified duration (advances test time).

        Matches ``WorkflowContext.sleep``: pass a ``timedelta``, or a bare number
        interpreted as seconds, so a timer behaves the same in tests as in
        production.

        Args:
            duration: How long to sleep — a ``timedelta`` or seconds as a number.

        Example:
            >>> await ctx.sleep(timedelta(hours=1))
            >>> await ctx.sleep(60)  # 60 seconds
        """
        if isinstance(duration, (int, float)):
            duration = timedelta(seconds=duration)
        await self._time_controller.advance(int(duration.total_seconds() * 1000))
        self._stats.timers_created += 1

    def get_state(self, key: str) -> Any:
        """Get workflow state value."""
        return self._state.get(key)

    def set_state(self, key: str, value: Any) -> None:
        """Set workflow state value."""
        self._state[key] = value

    def now(self) -> datetime:
        """Get current time."""
        return self._time_controller.now()

    @property
    def is_replaying(self) -> bool:
        """Check if workflow is replaying (always False in tests)."""
        return False


def _task_name(task_ref: Any) -> str:
    """The registered name of a ``@task`` function, a TaskReference or a name."""
    for attr in ("__orcher_task_name__", "task_name", "_task_name"):
        name = getattr(task_ref, attr, None)
        if isinstance(name, str):
            return name
    return str(task_ref)


def _workflow_arguments(input_data: Any) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Spread a workflow input into call arguments, as the worker does."""
    if isinstance(input_data, dict):
        return (), input_data
    if input_data is None:
        return (), {}
    return (input_data,), {}
