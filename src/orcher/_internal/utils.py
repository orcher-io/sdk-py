"""Common utilities for ORCHER Python SDK.

This module provides common utility functions used throughout the SDK.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

__all__ = [
    "generate_id",
    "ensure_async",
    "validate_name",
    "is_async_callable",
]

T = TypeVar("T")

# Pattern for valid workflow/task names
NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")


def generate_id(prefix: str = "") -> str:
    """Generate a unique ID.

    Args:
        prefix: Optional prefix for the ID.

    Returns:
        A unique identifier string.

    Example:
        >>> generate_id("wf")
        'wf-a1b2c3d4e5f6'
    """
    unique_part = uuid.uuid4().hex[:12]
    if prefix:
        return f"{prefix}-{unique_part}"
    return unique_part


def is_async_callable(obj: Any) -> bool:
    """Check if an object is an async callable.

    Args:
        obj: The object to check.

    Returns:
        True if the object is an async function or has an async __call__.
    """
    if asyncio.iscoroutinefunction(obj):
        return True

    if callable(obj):
        return asyncio.iscoroutinefunction(obj.__call__)

    return False


def ensure_async(
    func: Callable[..., T] | Callable[..., Awaitable[T]],
) -> Callable[..., Awaitable[T]]:
    """Wrap a sync function to make it async.

    If the function is already async, returns it unchanged.

    Args:
        func: A sync or async function.

    Returns:
        An async version of the function.

    Example:
        >>> def sync_func(x):
        ...     return x * 2
        >>> async_func = ensure_async(sync_func)
        >>> await async_func(5)
        10
    """
    if is_async_callable(func):
        return func  # type: ignore

    async def async_wrapper(*args: Any, **kwargs: Any) -> T:
        return func(*args, **kwargs)  # type: ignore

    # Preserve function metadata
    async_wrapper.__name__ = getattr(func, "__name__", "wrapped")
    async_wrapper.__doc__ = getattr(func, "__doc__", None)

    return async_wrapper


def validate_name(name: str, entity_type: str = "name") -> None:
    """Validate a workflow or task name.

    Names must:
    - Start with a letter
    - Contain only letters, numbers, underscores, and hyphens

    Args:
        name: The name to validate.
        entity_type: Type of entity for error messages (e.g., "workflow", "task").

    Raises:
        ValueError: If the name is invalid.

    Example:
        >>> validate_name("my-workflow", "workflow")  # OK
        >>> validate_name("123-invalid", "workflow")  # Raises ValueError
    """
    if not name:
        raise ValueError(f"{entity_type} name cannot be empty")

    if not NAME_PATTERN.match(name):
        raise ValueError(
            f"Invalid {entity_type} name '{name}'. "
            f"Names must start with a letter and contain only "
            f"letters, numbers, underscores, and hyphens."
        )


def get_callable_name(func: Callable[..., Any]) -> str:
    """Get a readable name for a callable.

    Args:
        func: The callable to get the name for.

    Returns:
        A string representation of the callable's name.
    """
    if hasattr(func, "__name__"):
        return func.__name__
    if hasattr(func, "__class__"):
        return func.__class__.__name__
    return repr(func)
