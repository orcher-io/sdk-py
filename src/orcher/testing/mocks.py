"""
Task Mocking utilities for ORCHER testing.

This module provides MockTaskRegistry and MockTaskBuilder for setting up
mock task responses in tests.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Any, Generic, TypeVar

from orcher.testing.types import (
    ErrorMockStrategy,
    FixedMockStrategy,
    FunctionMockStrategy,
    MockStrategy,
    SequenceMockStrategy,
    TaskCall,
    TaskMock,
)

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class MockTaskRegistry:
    """
    Registry for task mocks.

    Stores mock configurations and tracks task calls during test execution.

    Example:
        >>> registry = MockTaskRegistry()
        >>> registry.register_mock("charge_card", FixedMockStrategy({"id": "ch_123"}))
        >>> result = await registry.execute("charge_card", {"amount": 99.99})
    """

    def __init__(self) -> None:
        """Initialize an empty mock registry."""
        self._mocks: dict[str, TaskMock] = {}

    def register_mock(self, task_name: str, strategy: MockStrategy) -> None:
        """
        Register a mock for a task.

        Args:
            task_name: Name of the task to mock
            strategy: Mock strategy to use
        """
        self._mocks[task_name] = TaskMock(
            task_name=task_name,
            strategy=strategy,
            call_count=0,
            calls=[],
        )

    def has_mock(self, task_name: str) -> bool:
        """
        Check if a task has a mock registered.

        Args:
            task_name: Task name to check

        Returns:
            True if task is mocked
        """
        return task_name in self._mocks

    def get_mock(self, task_name: str) -> TaskMock | None:
        """
        Get mock configuration for a task.

        Args:
            task_name: Task name

        Returns:
            TaskMock or None if not mocked
        """
        return self._mocks.get(task_name)

    async def execute(self, task_name: str, input_data: Any) -> Any:
        """
        Execute a mocked task.

        Args:
            task_name: Task name to execute
            input_data: Input data for the task

        Returns:
            Mock result

        Raises:
            ValueError: If task is not mocked
            Exception: If mock strategy throws
        """
        mock = self._mocks.get(task_name)
        if mock is None:
            raise ValueError(
                f"Task '{task_name}' is not mocked. "
                f"Use env.mock_task('{task_name}').returns(...) to mock it."
            )

        start_time = datetime.now()
        mock.call_count += 1

        call = TaskCall(
            task_name=task_name,
            input=input_data,
            timestamp=start_time,
        )

        try:
            result = await self._execute_strategy(mock.strategy, input_data)
            call.output = result
            call.duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            mock.calls.append(call)
            return result
        except Exception as e:
            call.error = e
            call.duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            mock.calls.append(call)
            raise

    async def _execute_strategy(self, strategy: MockStrategy, input_data: Any) -> Any:
        """Execute a mock strategy and return the result."""
        if isinstance(strategy, FixedMockStrategy):
            return strategy.value

        elif isinstance(strategy, SequenceMockStrategy):
            value = strategy.next_value()
            if isinstance(value, Exception):
                raise value
            return value

        elif isinstance(strategy, FunctionMockStrategy):
            result = strategy.fn(input_data)
            if asyncio.iscoroutine(result):
                return await result
            return result

        elif isinstance(strategy, ErrorMockStrategy):
            raise strategy.error

        else:
            raise ValueError(f"Unknown mock strategy type: {type(strategy)}")

    def get_call_count(self, task_name: str) -> int:
        """
        Get number of times a task was called.

        Args:
            task_name: Task name

        Returns:
            Call count (0 if not mocked or not called)
        """
        mock = self._mocks.get(task_name)
        return mock.call_count if mock else 0

    def get_calls(self, task_name: str) -> list[TaskCall]:
        """
        Get all calls made to a task.

        Args:
            task_name: Task name

        Returns:
            List of TaskCall records
        """
        mock = self._mocks.get(task_name)
        return list(mock.calls) if mock else []

    def get_last_call(self, task_name: str) -> TaskCall | None:
        """
        Get the last call made to a task.

        Args:
            task_name: Task name

        Returns:
            Last TaskCall or None
        """
        mock = self._mocks.get(task_name)
        if mock and mock.calls:
            return mock.calls[-1]
        return None

    def verify_called(self, task_name: str) -> None:
        """
        Verify that a task was called at least once.

        Args:
            task_name: Task name

        Raises:
            AssertionError: If task was not called
        """
        count = self.get_call_count(task_name)
        if count == 0:
            raise AssertionError(f"Expected task '{task_name}' to be called, but it was not")

    def verify_called_times(self, task_name: str, expected_count: int) -> None:
        """
        Verify that a task was called exactly N times.

        Args:
            task_name: Task name
            expected_count: Expected call count

        Raises:
            AssertionError: If call count doesn't match
        """
        actual_count = self.get_call_count(task_name)
        if actual_count != expected_count:
            raise AssertionError(
                f"Expected task '{task_name}' to be called {expected_count} time(s), "
                f"but it was called {actual_count} time(s)"
            )

    def verify_called_with(self, task_name: str, expected_input: Any) -> None:
        """
        Verify that a task was called with specific input.

        Args:
            task_name: Task name
            expected_input: Expected input data

        Raises:
            AssertionError: If task was not called with expected input
        """
        calls = self.get_calls(task_name)
        for call in calls:
            if call.input == expected_input:
                return

        if not calls:
            raise AssertionError(
                f"Expected task '{task_name}' to be called with {expected_input}, "
                f"but it was never called"
            )
        else:
            actual_inputs = [c.input for c in calls]
            raise AssertionError(
                f"Expected task '{task_name}' to be called with {expected_input}, "
                f"but it was called with: {actual_inputs}"
            )

    def verify_not_called(self, task_name: str) -> None:
        """
        Verify that a task was never called.

        Args:
            task_name: Task name

        Raises:
            AssertionError: If task was called
        """
        count = self.get_call_count(task_name)
        if count > 0:
            raise AssertionError(
                f"Expected task '{task_name}' not to be called, but it was called {count} time(s)"
            )

    def clear(self) -> None:
        """Clear all mocks and call history."""
        self._mocks.clear()

    def reset_calls(self) -> None:
        """Reset call history but keep mock configurations."""
        for mock in self._mocks.values():
            mock.call_count = 0
            mock.calls.clear()
            if isinstance(mock.strategy, SequenceMockStrategy):
                mock.strategy.current_index = 0

    def get_summary(self) -> str:
        """
        Get a summary of all mocks and calls.

        Returns:
            Human-readable summary string
        """
        lines = ["Task Mocks:", "-" * 40]

        if not self._mocks:
            lines.append("  (no mocks registered)")
        else:
            for name, mock in self._mocks.items():
                strategy_type = mock.strategy.type
                lines.append(f"  {name}: {strategy_type} (called {mock.call_count}x)")

        return "\n".join(lines)


class MockTaskBuilder(Generic[TInput, TOutput]):
    """
    Fluent builder for configuring task mocks.

    Provides a fluent API for setting up mock behavior for tasks.

    Example:
        >>> # Fixed value mock
        >>> env.mock_task("charge_card").returns({"charge_id": "ch_123"})

        >>> # Sequence mock
        >>> env.mock_task("fetch_page").returns_sequence([
        ...     {"page": 1, "data": [...]},
        ...     {"page": 2, "data": [...]},
        ...     ValueError("No more pages")
        ... ])

        >>> # Function mock
        >>> env.mock_task("calculate_tax").with_fn(lambda amount: amount * 0.08)

        >>> # Error mock
        >>> env.mock_task("failing_service").throws(RuntimeError("Service down"))
    """

    def __init__(self, task_name: str, registry: MockTaskRegistry) -> None:
        """
        Initialize the builder.

        Args:
            task_name: Task name to mock
            registry: Registry to register mock with
        """
        self._task_name = task_name
        self._registry = registry

    def returns(self, value: TOutput) -> None:
        """
        Mock the task to return a fixed value.

        The task will always return the same value every time it's called.

        Args:
            value: Value to return

        Example:
            >>> env.mock_task("validate_order").returns({"valid": True})
        """
        strategy = FixedMockStrategy(value=value)
        self._registry.register_mock(self._task_name, strategy)

    def returns_sequence(self, values: list[TOutput | Exception]) -> None:
        """
        Mock the task to return a sequence of values.

        Each call will return the next value in the sequence.
        Include Exception instances to simulate failures.

        Args:
            values: List of values or exceptions to return

        Raises:
            IndexError: If sequence is exhausted

        Example:
            >>> # Simulate retry: fail twice, then succeed
            >>> env.mock_task("charge_card").returns_sequence([
            ...     TimeoutError("Gateway timeout"),
            ...     TimeoutError("Gateway timeout"),
            ...     {"charge_id": "ch_123", "success": True}
            ... ])
        """
        strategy = SequenceMockStrategy(values=list(values), current_index=0)
        self._registry.register_mock(self._task_name, strategy)

    def with_fn(self, fn: Callable[[TInput], TOutput]) -> None:
        """
        Mock the task with a custom function.

        The function receives the task input and returns the output.
        Can be async and can raise exceptions.

        Args:
            fn: Function to execute for mock

        Example:
            >>> env.mock_task("calculate_total").with_fn(
            ...     lambda items: sum(item["price"] for item in items)
            ... )
        """
        strategy = FunctionMockStrategy(fn=fn)
        self._registry.register_mock(self._task_name, strategy)

    def throws(self, error: Exception | str) -> None:
        """
        Mock the task to always throw an error.

        Args:
            error: Exception to throw (or string to wrap in RuntimeError)

        Example:
            >>> env.mock_task("failing_service").throws(
            ...     RuntimeError("Service unavailable")
            ... )
        """
        if isinstance(error, str):
            error = RuntimeError(error)
        strategy = ErrorMockStrategy(error=error)
        self._registry.register_mock(self._task_name, strategy)

    def resolves(self, value: TOutput) -> None:
        """
        Alias for returns() for better readability with async code.

        Args:
            value: Value to resolve with
        """
        self.returns(value)

    def rejects(self, error: Exception | str) -> None:
        """
        Alias for throws() for better readability with async code.

        Args:
            error: Error to reject with
        """
        self.throws(error)

    def returns_once_then(
        self,
        first_value: TOutput,
        subsequent: TOutput | Exception,
    ) -> None:
        """
        Return a value once, then return/throw something else.

        Args:
            first_value: Value for first call
            subsequent: Value or exception for subsequent calls

        Example:
            >>> env.mock_task("api").returns_once_then(
            ...     {"data": "success"},
            ...     RuntimeError("Rate limited")
            ... )
        """
        # A sequence of 100 values: calls after the 100th raise IndexError.
        values: list[TOutput | Exception] = [first_value]
        for _ in range(99):
            values.append(subsequent)
        self.returns_sequence(values)

    def throws_once_then(
        self,
        error: Exception | str,
        subsequent_value: TOutput,
    ) -> None:
        """
        Throw once, then return a value.

        Useful for testing retry logic.

        Args:
            error: Error for first call
            subsequent_value: Value for subsequent calls

        Example:
            >>> env.mock_task("flaky_api").throws_once_then(
            ...     TimeoutError("Timeout"),
            ...     {"data": "success"}
            ... )
        """
        if isinstance(error, str):
            error = RuntimeError(error)

        # A sequence of 100 values: calls after the 100th raise IndexError.
        values: list[TOutput | Exception] = [error]
        for _ in range(99):
            values.append(subsequent_value)
        self.returns_sequence(values)

    async def resolves_after(self, value: TOutput, delay_ms: int) -> None:
        """
        Return a value after a delay.

        Useful for testing timeout scenarios.

        Args:
            value: Value to return
            delay_ms: Delay in milliseconds

        Example:
            >>> await env.mock_task("slow_api").resolves_after({"data": "result"}, 5000)
        """

        async def delayed_fn(_: Any) -> TOutput:
            await asyncio.sleep(delay_ms / 1000.0)
            return value

        self.with_fn(delayed_fn)  # type: ignore

    async def rejects_after(
        self,
        error: Exception | str,
        delay_ms: int,
    ) -> None:
        """
        Throw an error after a delay.

        Args:
            error: Error to throw
            delay_ms: Delay in milliseconds
        """
        if isinstance(error, str):
            error = RuntimeError(error)

        async def delayed_fn(_: Any) -> TOutput:
            await asyncio.sleep(delay_ms / 1000.0)
            raise error

        self.with_fn(delayed_fn)  # type: ignore
