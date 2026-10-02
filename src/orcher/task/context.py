"""Task execution context for ORCHER Python SDK.

This module provides the TaskContext class that is passed to task methods,
providing access to heartbeat, cancellation, and task metadata.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from orcher.task.cancellation import CancellationToken
from orcher.task.info import TaskInfo

__all__ = ["TaskContext"]


class TaskContext:
    """Execution context for task code.

    TaskContext is passed as the first argument to task methods. It provides:

    - Task metadata via `info`
    - Heartbeat mechanism for long-running tasks
    - Cancellation handling via `cancellation_token`

    Unlike WorkflowContext, tasks CAN:
    - Make external API calls
    - Access databases
    - Use random/time freely
    - Have side effects

    However, tasks SHOULD:
    - Check for cancellation periodically
    - Be idempotent when possible (for retries)

    The worker heartbeats every task it runs on its own, so a long task needs
    no heartbeat code to stay alive, and one whose worker dies is timed out
    and retried. ``heartbeat`` is for reporting progress.

    Example:
        >>> @task(name="process-order")
        ... async def process_order(self, ctx: TaskContext, order_id: str) -> dict:
        ...     # Access task info
        ...     print(f"Processing order {order_id}, attempt {ctx.info.attempt}")
        ...
        ...     # Long-running operation with heartbeat
        ...     for step in ["validate", "charge", "fulfill"]:
        ...         ctx.cancellation_token.raise_if_cancelled()
        ...         result = await do_step(step, order_id)
        ...         await ctx.heartbeat({"step": step, "status": "done"})
        ...
        ...     return {"order_id": order_id, "status": "completed"}
    """

    def __init__(
        self,
        info: TaskInfo,
        *,
        heartbeat_enabled: bool = True,
        heartbeat_sender: Callable[[bytes | None], bool] | None = None,
    ) -> None:
        """Initialize the task context.

        Args:
            info: Task execution information.
            heartbeat_enabled: Whether heartbeat is enabled.
            heartbeat_sender: Hands a heartbeat's serialized details (or
                ``None``) to the worker, which sends them with the next
                heartbeat due; returns whether the task has been asked to stop.
                Absent, ``heartbeat`` sends nothing.
        """
        self._info = info
        self._heartbeat_enabled = heartbeat_enabled
        self._heartbeat_sender = heartbeat_sender
        self._cancellation_token = CancellationToken()
        self._logger: logging.LoggerAdapter[logging.Logger] | None = None

    @property
    def info(self) -> TaskInfo:
        """Get task execution information."""
        return self._info

    # Convenience properties for common info fields
    @property
    def task_id(self) -> str:
        """Get the task ID."""
        return self._info.task_id

    @property
    def task_type(self) -> str:
        """Get the task type/name."""
        return self._info.task_type

    @property
    def attempt(self) -> int:
        """Get the current attempt number (1-based)."""
        return self._info.attempt

    @property
    def heartbeat_timeout(self) -> timedelta | None:
        """Maximum time allowed between heartbeats, or ``None`` if unset.

        A task that reports progress needs this to pace itself: a task that
        goes longer than this without a heartbeat is timed out, even if it is
        healthy.
        """
        return self._info.heartbeat_timeout

    @property
    def start_to_close_timeout(self) -> timedelta | None:
        """Maximum time this attempt may run, or ``None`` if unset."""
        return self._info.start_to_close_timeout

    @property
    def logger(self) -> logging.LoggerAdapter[logging.Logger]:
        """Get a structured logger with task context fields.

        The logger automatically includes ``task_id``, ``task_type``,
        ``workflow_id``, and ``attempt`` in every message.

        Example:
            >>> ctx.logger.info("Processing item %s", item_id)
        """
        if self._logger is None:
            base = logging.getLogger(f"orcher.task.{self._info.task_type}")
            self._logger = logging.LoggerAdapter(
                base,
                {
                    "task_id": self._info.task_id,
                    "task_type": self._info.task_type,
                    "workflow_id": self._info.workflow_id,
                    "attempt": self._info.attempt,
                },
            )
        return self._logger

    @property
    def is_retry(self) -> bool:
        """Check if this task execution is a retry (attempt > 1)."""
        return self._info.attempt > 1

    @property
    def cancellation_token(self) -> CancellationToken:
        """Get the cancellation token.

        Use this to check for and respond to cancellation requests.
        """
        return self._cancellation_token

    def can_heartbeat(self) -> bool:
        """Check if heartbeat is enabled for this task.

        Heartbeat may be disabled if no heartbeat timeout is configured.
        """
        return self._heartbeat_enabled

    async def heartbeat(self, details: Any = None) -> None:
        """Send a heartbeat to indicate the task is still alive.

        The worker already heartbeats the task on its own while it runs; this
        adds one from the task's code, folded into the same stream. It never
        waits, and however often it is called no more heartbeats are sent than
        the worker's timer sends. The latest details go with the next one.

        Args:
            details: Optional details to include with the heartbeat.
                Can be used to report progress.

        Example:
            >>> for i, item in enumerate(items):
            ...     await process(item)
            ...     await ctx.heartbeat({"processed": i + 1, "total": len(items)})
        """
        if not self._heartbeat_enabled or self._heartbeat_sender is None:
            return

        payload = None if details is None else json.dumps(details, default=str).encode()
        if self._heartbeat_sender(payload):
            self._cancellation_token.cancel()

    def __repr__(self) -> str:
        return (
            f"TaskContext("
            f"task_id={self._info.task_id!r}, "
            f"task_type={self._info.task_type!r}, "
            f"attempt={self._info.attempt}"
            f")"
        )
