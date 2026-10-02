"""Executors that run workflow and task code.

Provides WorkflowExecutor and TaskExecutor, which execute the workflows and
tasks received from pollers.
"""

from __future__ import annotations

import asyncio
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Generic, TypeVar

from orcher._internal.logging import get_logger
from orcher.decorators.registry import GlobalRegistry
from orcher.task.context import TaskContext
from orcher.task.info import TaskInfo
from orcher.worker.poller import TaskTask, WorkflowTask
from orcher.workflow.context import WorkflowContext
from orcher.workflow.info import WorkflowInfo

__all__ = [
    "Executor",
    "ExecutorConfig",
    "WorkflowExecutor",
    "TaskExecutor",
    "ExecutionResult",
    "ExecutionStatus",
]

logger = get_logger("executor")

T = TypeVar("T")
R = TypeVar("R")


class ExecutionStatus(Enum):
    """Status of an execution."""

    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass
class ExecutorConfig:
    """Configuration for an executor.

    Attributes:
        max_concurrent: Maximum concurrent executions.
        default_timeout_seconds: Default execution timeout.
    """

    max_concurrent: int = 100
    default_timeout_seconds: float = 300.0


@dataclass
class ExecutionResult(Generic[R]):
    """Result of an execution.

    Attributes:
        status: The execution status.
        result: The result value (if completed).
        error: Error information (if failed).
        error_type: Type of the error.
        error_message: Error message.
        error_traceback: Stack trace.
        started_at: When execution started.
        completed_at: When execution completed.
    """

    status: ExecutionStatus
    result: R | None = None
    error: Exception | None = None
    error_type: str | None = None
    error_message: str | None = None
    error_traceback: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    @property
    def is_success(self) -> bool:
        """Whether the execution completed successfully."""
        return self.status == ExecutionStatus.COMPLETED

    @property
    def is_failure(self) -> bool:
        """Whether the execution failed."""
        return self.status == ExecutionStatus.FAILED

    @property
    def duration_ms(self) -> int | None:
        """Execution duration in milliseconds, or None if not both timestamps are set."""
        if self.started_at and self.completed_at:
            delta = self.completed_at - self.started_at
            return int(delta.total_seconds() * 1000)
        return None

    @classmethod
    def success(cls, result: R) -> ExecutionResult[R]:
        """Create a successful result."""
        return cls(
            status=ExecutionStatus.COMPLETED,
            result=result,
            completed_at=datetime.now(),
        )

    @classmethod
    def failure(cls, error: Exception) -> ExecutionResult[R]:
        """Create a failed result."""
        return cls(
            status=ExecutionStatus.FAILED,
            error=error,
            error_type=type(error).__name__,
            error_message=str(error),
            error_traceback=traceback.format_exc(),
            completed_at=datetime.now(),
        )


class Executor(ABC, Generic[T, R]):
    """Base class for executors.

    Executors receive tasks from pollers and execute the corresponding
    workflow or task code.
    """

    def __init__(self, config: ExecutorConfig) -> None:
        """Initialize the executor.

        Args:
            config: Executor configuration.
        """
        self._config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent)
        self._active_executions: dict[str, asyncio.Task[ExecutionResult[R]]] = {}

    @property
    def config(self) -> ExecutorConfig:
        """The executor configuration."""
        return self._config

    @property
    def active_count(self) -> int:
        """The number of active executions."""
        return len(self._active_executions)

    async def execute(self, task: T) -> ExecutionResult[R]:
        """Execute a unit of work within the concurrency limit.

        Exceptions and cancellation are captured in the returned result rather
        than raised.

        Args:
            task: The work to execute.

        Returns:
            The execution result.
        """
        async with self._semaphore:
            task_id = self._get_task_id(task)
            started_at = datetime.now()

            try:
                logger.debug(f"Starting execution: {task_id}")
                result = await self._execute_internal(task)
                result.started_at = started_at
                logger.debug(f"Completed execution: {task_id}")
                return result

            except asyncio.CancelledError:
                logger.debug(f"Execution cancelled: {task_id}")
                return ExecutionResult(
                    status=ExecutionStatus.CANCELLED,
                    started_at=started_at,
                    completed_at=datetime.now(),
                )

            except Exception as e:
                logger.error(f"Execution failed: {task_id}: {e}")
                result = ExecutionResult.failure(e)
                result.started_at = started_at
                return result

    @abstractmethod
    async def _execute_internal(self, task: T) -> ExecutionResult[R]:
        """Internal execution implementation.

        Args:
            task: The task to execute.

        Returns:
            The execution result.
        """
        pass

    @abstractmethod
    def _get_task_id(self, task: T) -> str:
        """Get a unique identifier for the task.

        Args:
            task: The task.

        Returns:
            A unique identifier string.
        """
        pass

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the executor, waiting for active executions.

        Args:
            timeout: Maximum time to wait for executions to complete.
        """
        if not self._active_executions:
            return

        logger.info(f"Waiting for {len(self._active_executions)} active executions...")

        tasks = list(self._active_executions.values())
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=timeout,
            )
        except TimeoutError:
            logger.warning(f"Shutdown timeout, cancelling {len(tasks)} executions")
            for task in tasks:
                task.cancel()


class WorkflowExecutor(Executor[WorkflowTask, Any]):
    """Executor for workflow tasks.

    Executes workflow code in response to workflow tasks received from
    the WorkflowPoller.
    """

    def __init__(self, config: ExecutorConfig) -> None:
        """Initialize the workflow executor."""
        super().__init__(config)
        self._registry = GlobalRegistry.get_instance()

    def _get_task_id(self, task: WorkflowTask) -> str:
        """Get the workflow task identifier."""
        return f"{task.workflow_id}:{task.run_id}"

    async def _execute_internal(self, task: WorkflowTask) -> ExecutionResult[Any]:
        """Execute a workflow task.

        Args:
            task: The workflow task.

        Returns:
            The execution result.
        """
        metadata = self._registry.get_workflow(task.workflow_type)
        if metadata is None:
            return ExecutionResult.failure(
                ValueError(f"Unknown workflow type: {task.workflow_type}")
            )

        info = WorkflowInfo(
            workflow_id=task.workflow_id,
            run_id=task.run_id,
            workflow_type=task.workflow_type,
            task_queue=task.task_queue,
            namespace="default",  # Always "default": WorkflowTask carries no namespace.
            attempt=1,
            started_at=datetime.now(),
        )
        context = WorkflowContext(info)

        try:
            if metadata.is_function:
                # Function-based workflow
                result = await metadata.handler(context)
            else:
                # Class-based workflow
                workflow_class = metadata.handler
                workflow_instance = workflow_class()
                if metadata.run_method is not None:
                    result = await metadata.run_method(workflow_instance, context)
                else:
                    result = await workflow_instance.run(context)
            return ExecutionResult.success(result)

        except NotImplementedError as e:
            # Raised by workflow APIs that need the native bridge when it is absent.
            logger.debug(f"Workflow execution requires native bridge: {e}")
            return ExecutionResult.failure(e)

        except Exception as e:
            return ExecutionResult.failure(e)


class TaskExecutor(Executor[TaskTask, Any]):
    """Executor for tasks.

    Executes task code in response to tasks received from the TaskPoller.
    """

    def __init__(self, config: ExecutorConfig) -> None:
        """Initialize the task executor."""
        super().__init__(config)
        self._registry = GlobalRegistry.get_instance()
        self._task_instances: dict[type[Any], Any] = {}

    def _get_task_id(self, task: TaskTask) -> str:
        """Get the task identifier."""
        return f"{task.workflow_id}:{task.task_id}"

    def _get_task_handler_instance(self, handler_class: type[Any]) -> Any:
        """Get or create an instance of a task handler class.

        Args:
            handler_class: The task handler class.

        Returns:
            An instance of the class.
        """
        if handler_class not in self._task_instances:
            self._task_instances[handler_class] = handler_class()
        return self._task_instances[handler_class]

    async def _execute_internal(self, task: TaskTask) -> ExecutionResult[Any]:
        """Execute a task.

        Args:
            task: The task to execute.

        Returns:
            The execution result.
        """
        metadata = self._registry.get_task(task.task_type)
        if metadata is None:
            return ExecutionResult.failure(ValueError(f"Unknown task type: {task.task_type}"))

        info = TaskInfo(
            task_id=task.task_id,
            task_type=task.task_type,
            workflow_id=task.workflow_id,
            run_id=task.run_id,
            task_queue=task.task_queue,
            namespace="default",  # Always "default": TaskTask carries no namespace.
            attempt=task.attempt,
            scheduled_at=task.scheduled_time or datetime.now(),
            started_at=task.started_time or datetime.now(),
            # Expose the timeouts to the handler so a task can see the limits
            # it runs under.
            heartbeat_timeout=(
                timedelta(seconds=task.heartbeat_timeout_seconds)
                if task.heartbeat_timeout_seconds is not None
                else None
            ),
            start_to_close_timeout=(
                timedelta(seconds=task.start_to_close_timeout_seconds)
                if task.start_to_close_timeout_seconds is not None
                else None
            ),
        )
        context = TaskContext(
            info,
            heartbeat_enabled=task.heartbeat_timeout_seconds is not None,
        )

        try:
            if metadata.is_function:
                # Function-based task
                handler = metadata.handler
                if task.input is not None:
                    if isinstance(task.input, (list, tuple)):
                        result = await handler(context, *task.input)
                    elif isinstance(task.input, dict):
                        result = await handler(context, **task.input)
                    else:
                        result = await handler(context, task.input)
                else:
                    result = await handler(context)
            else:
                # Class-based task
                if metadata.handler_class is None or metadata.method_name is None:
                    return ExecutionResult.failure(
                        ValueError(f"Task {task.task_type} missing handler_class or method_name")
                    )
                handler_instance = self._get_task_handler_instance(metadata.handler_class)
                method = getattr(handler_instance, metadata.method_name, None)

                # `@tasks` replaces the method on the class with a TaskReference,
                # so a lookup by name returns that reference, which is not
                # callable. The real function is on the metadata; bind it to the
                # instance so it still receives `self`.
                if not callable(method):
                    raw = metadata.handler
                    if raw is None:
                        return ExecutionResult.failure(
                            ValueError(
                                f"Task {task.task_type} has no callable handler: "
                                f"'{metadata.method_name}' resolved to "
                                f"{type(method).__name__} and no handler was registered"
                            )
                        )
                    method = raw.__get__(handler_instance, type(handler_instance))

                if task.input is not None:
                    if isinstance(task.input, (list, tuple)):
                        result = await method(context, *task.input)
                    elif isinstance(task.input, dict):
                        result = await method(context, **task.input)
                    else:
                        result = await method(context, task.input)
                else:
                    result = await method(context)

            return ExecutionResult.success(result)

        except Exception as e:
            return ExecutionResult.failure(e)
