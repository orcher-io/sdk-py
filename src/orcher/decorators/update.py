"""The ``@update`` decorator for defining update handlers.

Update handlers let external clients send a synchronous mutation to a running
workflow and receive the result.

Unlike queries, which are read-only and synchronous, updates can mutate
workflow state and can be async. Unlike events, which are fire-and-forget,
updates return a result to the caller.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, TypeVar, get_type_hints

from orcher.decorators.registry import GlobalRegistry, HandlerType, UpdateMetadata

__all__ = ["update"]

F = TypeVar("F", bound=Callable[..., Any])


def update(
    *,
    name: str,
    description: str = "",
    timeout: float | None = None,
) -> Callable[[F], F]:
    """Define an update handler function.

    Update handlers are invoked when an external client sends an update to a
    running workflow. They can read and mutate workflow state via WorkflowContext,
    and return a result back to the caller.

    The decorated function should:
    - Accept WorkflowContext as the first argument
    - Be async (recommended) or sync
    - Return a serializable result

    Args:
        name: Unique name for the update handler. Used for identification and routing.
        description: Optional description of what this update does.
        timeout: Optional execution timeout in seconds.

    Returns:
        A decorator that registers the update handler function.

    Raises:
        ValueError: If the function signature is invalid.
        ValueError: If the update name is already registered.

    Example:
        >>> from orcher import update, WorkflowContext
        >>>
        >>> @update(name="change_address")
        ... async def change_address(ctx: WorkflowContext, new_address: dict) -> dict:
        ...     current = ctx.get_state("status")
        ...     if current == "shipped":
        ...         return {"success": False, "error": "Already shipped"}
        ...     ctx.set_state("address", new_address)
        ...     return {"success": True, "address": new_address}
        >>>
        >>> @update(name="cancel_order", timeout=30.0)
        ... async def cancel_order(ctx: WorkflowContext, reason: str) -> dict:
        ...     status = ctx.get_state("status")
        ...     if status in ("shipped", "delivered"):
        ...         return {"success": False, "error": f"Cannot cancel: {status}"}
        ...     ctx.set_state("status", "cancelled")
        ...     return {"success": True, "reason": reason}
    """

    def decorator(fn: F) -> F:
        if not callable(fn):
            raise ValueError(f"@update decorator must be applied to a function, got {type(fn)}")

        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())

        if len(params) < 1:
            raise ValueError(
                f"Update function '{fn.__name__}' must accept at least (ctx: WorkflowContext). "
                f"Found parameters: {params}"
            )

        # Used to deserialize the result; unresolvable hints are ignored.
        return_type = None
        try:
            type_hints = get_type_hints(fn)
            return_type = type_hints.get("return")
        except Exception:
            pass

        metadata = UpdateMetadata(
            name=name,
            handler=fn,
            handler_type=HandlerType.FUNCTION,
            description=description,
            timeout=timeout,
            return_type=return_type,
        )

        registry = GlobalRegistry.get_instance()
        registry.register_update(metadata)

        # Attach metadata to the function for introspection
        fn.__orcher_update__ = metadata  # type: ignore
        fn.__orcher_update_name__ = name  # type: ignore
        fn.__orcher_is_update__ = True  # type: ignore
        fn.__orcher_update_timeout__ = timeout  # type: ignore

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)

        # The wrapper is what callers hold, so it carries the same metadata.
        wrapper.__orcher_update__ = metadata  # type: ignore
        wrapper.__orcher_update_name__ = name  # type: ignore
        wrapper.__orcher_is_update__ = True  # type: ignore
        wrapper.__orcher_update_timeout__ = timeout  # type: ignore

        return wrapper  # type: ignore

    return decorator
