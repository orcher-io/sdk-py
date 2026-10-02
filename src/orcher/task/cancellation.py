"""Cancellation token for task execution.

This module provides the CancellationToken class for checking and
handling task cancellation.
"""

from __future__ import annotations

import asyncio

__all__ = ["CancellationToken"]


class CancellationToken:
    """Token for checking and handling task cancellation.

    Tasks can check this token to see if cancellation has been requested
    and respond appropriately.

    Example:
        >>> @task(name="long-running")
        ... async def long_task(self, ctx: TaskContext, items: list) -> int:
        ...     processed = 0
        ...     for item in items:
        ...         # Check for cancellation periodically
        ...         ctx.cancellation_token.raise_if_cancelled()
        ...
        ...         await process_item(item)
        ...         processed += 1
        ...
        ...         # Send heartbeat to indicate progress
        ...         await ctx.heartbeat({"processed": processed})
        ...     return processed
    """

    def __init__(self) -> None:
        """Initialize the cancellation token."""
        self._cancelled = False
        self._event = asyncio.Event()

    @property
    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested."""
        return self._cancelled

    def cancel(self) -> None:
        """Request cancellation (internal use)."""
        self._cancelled = True
        self._event.set()

    async def wait_for_cancellation(self) -> None:
        """Wait until cancellation is requested.

        This can be used in conjunction with asyncio.wait() to implement
        cancellation-aware waiting.
        """
        await self._event.wait()

    def raise_if_cancelled(self) -> None:
        """Raise a cancellation ``TaskError`` if cancellation has been requested.

        Raises:
            TaskError: If cancelled.
        """
        if self._cancelled:
            from orcher.errors import TaskError

            raise TaskError.cancelled("unknown")

    def __repr__(self) -> str:
        status = "cancelled" if self._cancelled else "active"
        return f"CancellationToken(status={status})"
