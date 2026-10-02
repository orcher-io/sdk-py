"""The ``@query`` decorator for defining query handlers.

Query handlers let external clients read workflow state without affecting
execution. They must be read-only: no state mutation and no side effects.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, TypeVar, get_type_hints

from orcher.decorators.registry import GlobalRegistry, HandlerType, QueryMetadata

__all__ = ["query"]

F = TypeVar("F", bound=Callable[..., Any])


def query(
    *,
    name: str,
    description: str = "",
    timeout: float | None = None,
    cache_ttl: float | None = None,
) -> Callable[[F], F]:
    """Define a query handler function.

    Query handlers are invoked when an external client queries a running
    workflow. They should be read-only and not modify workflow state.

    The decorated function should:
    - Accept WorkflowContext as the first argument
    - Be synchronous (queries should be fast, read-only operations)
    - Return a serializable result

    Args:
        name: Unique name for the query handler. Used for identification and routing.
        description: Optional description of what this query returns.
        timeout: Optional execution timeout in seconds.
        cache_ttl: Optional cache TTL in seconds for query results.

    Returns:
        A decorator that registers the query handler function.

    Raises:
        ValueError: If the function signature is invalid.
        ValueError: If the query name is already registered.

    Example:
        >>> from orcher import query, WorkflowContext
        >>>
        >>> @query(name="get_status")
        ... def get_status(ctx: WorkflowContext) -> str:
        ...     return ctx.get_state("status", "unknown")
        >>>
        >>> @query(name="get_progress", description="Calculate order progress")
        ... def get_progress(ctx: WorkflowContext) -> dict:
        ...     done = ctx.get_state("items_processed", 0)
        ...     total = ctx.get_state("total_items", 1)
        ...     return {"done": done, "total": total, "pct": done / total * 100}
    """

    def decorator(fn: F) -> F:
        if not callable(fn):
            raise ValueError(f"@query decorator must be applied to a function, got {type(fn)}")

        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())

        if len(params) < 1:
            raise ValueError(
                f"Query function '{fn.__name__}' must accept at least (ctx: WorkflowContext). "
                f"Found parameters: {params}"
            )

        # Used to deserialize the result; unresolvable hints are ignored.
        return_type = None
        try:
            type_hints = get_type_hints(fn)
            return_type = type_hints.get("return")
        except Exception:
            pass

        metadata = QueryMetadata(
            name=name,
            handler=fn,
            handler_type=HandlerType.FUNCTION,
            description=description,
            timeout=timeout,
            cache_ttl=cache_ttl,
            return_type=return_type,
        )

        registry = GlobalRegistry.get_instance()
        registry.register_query(metadata)

        # Attach metadata to the function for introspection
        fn.__orcher_query__ = metadata  # type: ignore
        fn.__orcher_query_name__ = name  # type: ignore
        fn.__orcher_is_query__ = True  # type: ignore
        fn.__orcher_query_timeout__ = timeout  # type: ignore

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)

        # The wrapper is what callers hold, so it carries the same metadata.
        wrapper.__orcher_query__ = metadata  # type: ignore
        wrapper.__orcher_query_name__ = name  # type: ignore
        wrapper.__orcher_is_query__ = True  # type: ignore
        wrapper.__orcher_query_timeout__ = timeout  # type: ignore

        return wrapper  # type: ignore

    return decorator
