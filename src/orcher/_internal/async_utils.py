"""Async utilities for ORCHER Python SDK.

This module provides async helper functions used throughout the SDK.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar, cast

from orcher._internal.logging import get_logger

__all__ = [
    "run_sync",
    "create_task_with_error_handling",
    "timeout_async",
    "cancel_task_safely",
]

T = TypeVar("T")

logger = get_logger("async")


def run_sync(coro: Awaitable[T]) -> T:
    """Run an async coroutine synchronously.

    This is useful for running async code from sync contexts, such as
    in tests or when integrating with sync frameworks.

    Args:
        coro: The coroutine to run.

    Returns:
        The result of the coroutine.

    Raises:
        RuntimeError: If called from within an existing event loop.

    Example:
        >>> async def fetch_data():
        ...     return "data"
        >>> result = run_sync(fetch_data())
        >>> print(result)
        'data'
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No running loop, safe to use asyncio.run
        pass
    else:
        raise RuntimeError(
            "run_sync cannot be called from within an async context. Use 'await' instead."
        )

    # Cast Awaitable to Coroutine for asyncio.run which requires Coroutine
    return asyncio.run(cast(Coroutine[Any, Any, T], coro))


def create_task_with_error_handling(
    coro: Awaitable[T],
    name: str | None = None,
    on_error: Callable[[Exception], None] | None = None,
) -> asyncio.Task[T]:
    """Create an asyncio task with automatic error handling.

    Errors in the task are logged automatically. An optional error callback
    can be provided for custom error handling.

    Args:
        coro: The coroutine to run as a task.
        name: Optional name for the task.
        on_error: Optional callback for error handling.

    Returns:
        The created task.

    Example:
        >>> async def risky_operation():
        ...     raise ValueError("Something went wrong")
        >>> task = create_task_with_error_handling(
        ...     risky_operation(),
        ...     name="risky-task",
        ...     on_error=lambda e: print(f"Error: {e}")
        ... )
    """

    async def wrapped() -> T:
        try:
            return await coro
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Task {name or 'unnamed'} failed: {e}", exc_info=True)
            if on_error:
                on_error(e)
            raise

    task = asyncio.create_task(wrapped(), name=name)
    return task


async def timeout_async(
    coro: Awaitable[T],
    timeout_seconds: float,
    timeout_message: str | None = None,
) -> T:
    """Run a coroutine with a timeout.

    Args:
        coro: The coroutine to run.
        timeout_seconds: Maximum time to wait in seconds.
        timeout_message: Optional custom timeout error message.

    Returns:
        The result of the coroutine.

    Raises:
        asyncio.TimeoutError: If the coroutine doesn't complete in time.

    Example:
        >>> async def slow_operation():
        ...     await asyncio.sleep(10)
        ...     return "done"
        >>> result = await timeout_async(slow_operation(), 5.0)
        asyncio.TimeoutError: Operation timed out after 5.0 seconds
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except TimeoutError as err:
        if timeout_message:
            raise TimeoutError(timeout_message) from err
        raise TimeoutError(f"Operation timed out after {timeout_seconds} seconds") from err


async def cancel_task_safely(task: asyncio.Task[Any], timeout: float = 5.0) -> None:
    """Cancel a task and wait for it to complete.

    This handles the CancelledError properly and waits for the task
    to finish cleanup.

    Args:
        task: The task to cancel.
        timeout: Maximum time to wait for cancellation.
    """
    if task.done():
        return

    task.cancel()

    try:
        await asyncio.wait_for(task, timeout=timeout)
    except asyncio.CancelledError:
        pass
    except TimeoutError:
        logger.warning(f"Task {task.get_name()} did not cancel within {timeout}s")
    except Exception as e:
        logger.debug(f"Task {task.get_name()} raised during cancellation: {e}")
