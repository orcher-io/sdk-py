"""Base interceptor classes and the context passed to them."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Protocol, runtime_checkable

__all__ = [
    "InterceptorContext",
    "ExecutionInfo",
    "NextFn",
    "Interceptor",
    "WorkflowInterceptor",
    "TaskInterceptor",
    "InterceptorFactory",
]

# Calls the next interceptor in the chain, or the workflow or task itself.
NextFn = Callable[[Any], Awaitable[Any]]


@dataclass
class InterceptorContext:
    """Context passed to interceptors during execution.

    Carries metadata about the current execution for logging, metrics,
    tracing, and similar uses.
    """

    # Execution identifiers
    workflow_id: str
    run_id: str

    # Type information
    workflow_type: str

    # Set only for task interceptors.
    task_name: str | None = None
    task_id: str | None = None

    # Timing
    start_time: datetime = field(default_factory=datetime.now)

    # Free-form values that interceptors share with each other.
    attributes: dict[str, Any] = field(default_factory=dict)

    # The workflow context a task context was derived from.
    parent: Optional["InterceptorContext"] = None

    def with_task(self, task_name: str, task_id: str) -> "InterceptorContext":
        """Create a child context for a task, copying this context's attributes."""
        return InterceptorContext(
            workflow_id=self.workflow_id,
            run_id=self.run_id,
            workflow_type=self.workflow_type,
            task_name=task_name,
            task_id=task_id,
            start_time=datetime.now(),
            attributes=dict(self.attributes),
            parent=self,
        )

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a custom attribute."""
        self.attributes[key] = value

    def get_attribute(self, key: str, default: Any = None) -> Any:
        """Get a custom attribute."""
        return self.attributes.get(key, default)


@dataclass
class ExecutionInfo:
    """Information about an execution for interceptor callbacks."""

    input_data: Any

    # Set only for exit and success callbacks.
    output_data: Any | None = None

    # Set only for error callbacks.
    error: Exception | None = None

    # Set only for exit callbacks.
    duration_ms: float | None = None

    success: bool = True


class Interceptor:
    """Base class for all interceptors.

    Interceptors form a chain of responsibility. Each one can act before and
    after the execution and decides whether to continue the chain.
    """

    @property
    def name(self) -> str:
        """Return the name of this interceptor."""
        return self.__class__.__name__

    @property
    def order(self) -> int:
        """Return the position of this interceptor; lower values run first.

        The default is 100. The conventional ranges are:

        - below 50: system interceptors (the built-in tracing and metrics use 10)
        - 50 to 99: framework interceptors (the built-in logging uses 50)
        - 100 and above: user interceptors
        """
        return 100


class WorkflowInterceptor(Interceptor):
    """Interceptor for workflow executions.

    Provides ``intercept_execute``, which wraps the whole execution, and
    the ``on_enter``, ``on_success``, ``on_error``, and ``on_exit`` hooks.
    Exit hooks run in reverse order, so the first interceptor to enter is
    the last to exit.
    """

    async def intercept_execute(
        self,
        context: InterceptorContext,
        input_data: Any,
        next_fn: NextFn,
    ) -> Any:
        """Intercept workflow execution.

        Override this method to wrap the entire workflow execution. It runs
        when the chain's ``execute`` method is called. Call
        ``next_fn(input_data)`` to continue the chain.

        Args:
            context: The interceptor context with execution metadata.
            input_data: The input to the workflow.
            next_fn: Function to call the next interceptor or actual workflow.

        Returns:
            The result of the workflow execution.
        """
        return await next_fn(input_data)

    async def on_enter(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Called when workflow execution starts.

        Override this for logging, metrics, or setup.
        """
        pass

    async def on_exit(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Called when workflow execution completes (success or failure).

        Override this for cleanup, logging, or metrics.
        """
        pass

    async def on_success(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Called when workflow execution succeeds.

        Override this for success-specific handling.
        """
        pass

    async def on_error(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Called when workflow execution fails.

        Override this for error handling, alerting, etc.
        """
        pass


class TaskInterceptor(Interceptor):
    """Interceptor for task executions.

    Provides ``intercept_execute``, which wraps the task execution, the
    ``on_enter``, ``on_success``, ``on_error``, and ``on_exit`` hooks, and
    ``on_retry``. Exit hooks run in reverse order.
    """

    async def intercept_execute(
        self,
        context: InterceptorContext,
        input_data: Any,
        next_fn: NextFn,
    ) -> Any:
        """Intercept task execution.

        Override this method to wrap task execution. It runs when the chain's
        ``execute`` method is called. Call ``next_fn(input_data)`` to continue
        the chain.

        Args:
            context: The interceptor context with execution metadata.
            input_data: The input to the task.
            next_fn: Function to call the next interceptor or actual task.

        Returns:
            The result of the task execution.
        """
        return await next_fn(input_data)

    async def on_enter(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Called when task execution starts."""
        pass

    async def on_exit(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Called when task execution completes (success or failure)."""
        pass

    async def on_success(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Called when task execution succeeds."""
        pass

    async def on_error(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Called when task execution fails."""
        pass

    async def on_retry(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
        attempt: int,
        max_attempts: int,
    ) -> None:
        """Called before a task is retried, via ``notify_retry`` on the chain.

        Args:
            context: The interceptor context.
            info: Execution info with the error that triggered retry.
            attempt: Current attempt number (1-indexed).
            max_attempts: Maximum number of attempts configured.
        """
        pass


@runtime_checkable
class InterceptorFactory(Protocol):
    """Anything that makes a matched workflow and task interceptor pair.

    The built-in ``LoggingInterceptor``, ``MetricsInterceptor`` and
    ``TracingInterceptor`` are factories of this kind: they are not
    interceptors themselves, but ``workflow()`` and ``task()`` return
    interceptors that share the factory's configuration. ``WorkerBuilder``
    accepts one in ``interceptor()`` and adds both.
    """

    def workflow(self) -> WorkflowInterceptor:
        """Create the workflow interceptor."""
        ...

    def task(self) -> TaskInterceptor:
        """Create the task interceptor."""
        ...
