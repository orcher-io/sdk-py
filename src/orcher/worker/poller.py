"""Poller types and pure-Python poller classes.

Defines the data types for polled work (WorkflowTask, TaskTask, PollResult)
and the Poller base class with WorkflowPoller and TaskPoller. The Worker does
not use these pollers: it polls the server through the native module. The
pure-Python pollers never contact the server; they wait out the poll interval
and return no work.
"""

from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Generic, TypeVar

from orcher._internal.logging import get_logger

__all__ = [
    "Poller",
    "PollerState",
    "PollerConfig",
    "WorkflowPoller",
    "TaskPoller",
    "PollResult",
    "WorkflowTask",
    "TaskTask",
]

logger = get_logger("poller")

T = TypeVar("T")


class PollerState(Enum):
    """State of a poller."""

    STOPPED = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()


@dataclass
class PollerConfig:
    """Configuration for a poller.

    Attributes:
        task_queue: The task queue to poll.
        namespace: The namespace to poll in.
        identity: Worker identity string.
        poll_interval_ms: Interval between polls in milliseconds.
        max_concurrent: Maximum concurrent executions.
        sticky_queue_name: Optional sticky queue for workflow task routing.
    """

    task_queue: str
    namespace: str
    identity: str
    poll_interval_ms: int = 1000
    max_concurrent: int = 100
    sticky_queue_name: str | None = None


@dataclass
class WorkflowTask:
    """A workflow task received from polling.

    Attributes:
        task_token: Unique token for this task.
        workflow_id: ID of the workflow.
        run_id: Run ID of the workflow execution.
        workflow_type: Type/name of the workflow.
        task_queue: The task queue.
        history_events: Events from the workflow history.
        query: Optional query to execute.
        started_event_id: ID of the workflow started event.
        previous_started_event_id: ID of the previous started event.
    """

    task_token: bytes
    workflow_id: str
    run_id: str
    workflow_type: str
    task_queue: str
    history_events: list[dict[str, Any]] = field(default_factory=list)
    query: dict[str, Any] | None = None
    started_event_id: int = 0
    previous_started_event_id: int = 0


@dataclass
class TaskTask:
    """A task received from polling.

    Attributes:
        task_token: Unique token for this task.
        task_id: Unique ID for this task execution.
        workflow_id: ID of the workflow that scheduled this task.
        run_id: Run ID of the workflow execution.
        task_type: Type/name of the task.
        task_queue: The task queue.
        input: Input data for the task.
        attempt: Current attempt number (1-based).
        scheduled_time: When the task was scheduled.
        started_time: When this attempt started.
        heartbeat_timeout_seconds: Heartbeat timeout.
        schedule_to_close_timeout_seconds: Total timeout.
        start_to_close_timeout_seconds: Execution timeout.
    """

    task_token: bytes
    task_id: str
    workflow_id: str
    run_id: str
    task_type: str
    task_queue: str
    input: Any = None
    attempt: int = 1
    scheduled_time: datetime | None = None
    started_time: datetime | None = None
    heartbeat_timeout_seconds: float | None = None
    schedule_to_close_timeout_seconds: float | None = None
    start_to_close_timeout_seconds: float | None = None


@dataclass
class PollResult(Generic[T]):
    """Result of a poll operation.

    Attributes:
        task: The task received, if any.
        is_shutdown: Whether shutdown was requested.
        error: Any error that occurred during polling.
    """

    task: T | None = None
    is_shutdown: bool = False
    error: Exception | None = None

    @property
    def has_task(self) -> bool:
        """Whether a task was received."""
        return self.task is not None

    @property
    def has_error(self) -> bool:
        """Whether an error occurred."""
        return self.error is not None


class Poller(ABC, Generic[T]):
    """Base class for pollers.

    A poller runs a background loop that calls _poll_once() and dispatches
    each received task to the handler set with set_task_handler().
    """

    def __init__(self, config: PollerConfig) -> None:
        """Initialize the poller.

        Args:
            config: Poller configuration.
        """
        self._config = config
        self._state = PollerState.STOPPED
        self._shutdown_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._on_task: Callable[[T], Any] | None = None

    @property
    def config(self) -> PollerConfig:
        """The poller configuration."""
        return self._config

    @property
    def state(self) -> PollerState:
        """The current poller state."""
        return self._state

    @property
    def is_running(self) -> bool:
        """Whether the poller is running."""
        return self._state == PollerState.RUNNING

    def set_task_handler(self, handler: Callable[[T], Any]) -> None:
        """Set the handler for received tasks.

        Args:
            handler: Callback to invoke when a task is received.
        """
        self._on_task = handler

    async def start(self) -> None:
        """Start the poller.

        This begins polling in the background.
        """
        if self._state != PollerState.STOPPED:
            raise RuntimeError(f"Cannot start poller in state {self._state}")

        self._state = PollerState.STARTING
        self._shutdown_event.clear()

        self._task = asyncio.create_task(self._poll_loop())
        self._state = PollerState.RUNNING

        logger.info(f"{self.__class__.__name__} started for queue '{self._config.task_queue}'")

    async def stop(self, timeout: float = 10.0) -> None:
        """Stop the poller.

        Args:
            timeout: Maximum time to wait for graceful shutdown.
        """
        if self._state == PollerState.STOPPED:
            return

        self._state = PollerState.STOPPING
        self._shutdown_event.set()

        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=timeout)
            except TimeoutError:
                logger.warning(f"{self.__class__.__name__} did not stop within timeout")
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._task

        self._state = PollerState.STOPPED
        logger.info(f"{self.__class__.__name__} stopped")

    async def _poll_loop(self) -> None:
        """Main polling loop."""
        while not self._shutdown_event.is_set():
            try:
                result = await self._poll_once()

                if result.is_shutdown:
                    break

                if result.has_error:
                    logger.error(f"Poll error: {result.error}")
                    # Wait one poll interval before retrying after an error.
                    await asyncio.sleep(self._config.poll_interval_ms / 1000.0)
                    continue

                if result.has_task and self._on_task:
                    try:
                        await self._dispatch_task(result.task)  # type: ignore
                    except Exception as e:
                        logger.error(f"Error dispatching task: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in poll loop: {e}")
                await asyncio.sleep(self._config.poll_interval_ms / 1000.0)

    @abstractmethod
    async def _poll_once(self) -> PollResult[T]:
        """Poll for a single task.

        Returns:
            The poll result.
        """
        pass

    async def _dispatch_task(self, task: T) -> None:
        """Dispatch a task to the handler.

        Args:
            task: The task to dispatch.
        """
        if self._on_task:
            result = self._on_task(task)
            if asyncio.iscoroutine(result):
                await result


class WorkflowPoller(Poller[WorkflowTask]):
    """Poller for workflow tasks.

    Intended to poll for workflow tasks and dispatch them to the workflow
    executor. It does not contact the server; see the module docstring.
    """

    async def _poll_once(self) -> PollResult[WorkflowTask]:
        """Poll for a workflow task.

        Returns:
            The poll result.
        """
        if self._shutdown_event.is_set():
            return PollResult(is_shutdown=True)

        # Does not reach the server: waits one poll interval, or until
        # shutdown, and reports no work.
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(),
                timeout=self._config.poll_interval_ms / 1000.0,
            )
            return PollResult(is_shutdown=True)
        except TimeoutError:
            return PollResult()


class TaskPoller(Poller[TaskTask]):
    """Poller for tasks.

    Intended to poll for task executions and dispatch them to the task
    executor. It does not contact the server; see the module docstring.
    """

    async def _poll_once(self) -> PollResult[TaskTask]:
        """Poll for a task.

        Returns:
            The poll result.
        """
        if self._shutdown_event.is_set():
            return PollResult(is_shutdown=True)

        # Does not reach the server: waits one poll interval, or until
        # shutdown, and reports no work.
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(),
                timeout=self._config.poll_interval_ms / 1000.0,
            )
            return PollResult(is_shutdown=True)
        except TimeoutError:
            return PollResult()
