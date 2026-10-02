"""Query handling for workflows in the Orcher Python SDK.

This module provides utilities for handling queries in workflows.
Queries let external systems read workflow state without
affecting the execution.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

__all__ = [
    "QueryHandler",
    "QueryRegistry",
    "query_handler",
]

T = TypeVar("T")


@dataclass
class QueryHandler:
    """Metadata for a query handler method.

    Attributes:
        name: The query name this handler responds to.
        method_name: Name of the handler method.
        method: The actual handler callable.
    """

    name: str
    method_name: str
    method: Callable[..., Any]


def query_handler(name: str) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to mark a method as a query handler.

    Query handlers are called when an external client queries the workflow.
    They should be read-only and not modify workflow state or cause
    side effects.

    IMPORTANT: Query handlers must be synchronous and should not call
    any workflow context methods that would generate commands.

    Args:
        name: The name of the query to handle.

    Returns:
        A decorator that marks the method as a query handler.

    Example:
        >>> @workflow(name="counter-workflow")
        ... class CounterWorkflow:
        ...     def __init__(self):
        ...         self.count = 0
        ...         self.status = "running"
        ...
        ...     @query_handler("get_count")
        ...     def get_count(self) -> int:
        ...         return self.count
        ...
        ...     @query_handler("get_status")
        ...     def get_status(self) -> str:
        ...         return self.status
        ...
        ...     async def run(self, ctx: WorkflowContext) -> int:
        ...         for i in range(100):
        ...             self.count = i
        ...             await ctx.sleep(timedelta(seconds=1))
        ...         self.status = "completed"
        ...         return self.count
        >>>
        >>> # From client:
        >>> count = await handle.query("get_count")
        >>> status = await handle.query("get_status")
    """

    def decorator(method: Callable[..., T]) -> Callable[..., T]:
        method.__orcher_query_handler__ = True  # type: ignore
        method.__orcher_query_name__ = name  # type: ignore
        return method

    return decorator


class QueryRegistry:
    """Registry for query handlers in a workflow class.

    This is used internally to track which methods handle which queries.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, QueryHandler] = {}

    def register(self, handler: QueryHandler) -> None:
        """Register a query handler.

        Args:
            handler: The query handler to register.

        Raises:
            ValueError: If a handler for this query is already registered.
        """
        if handler.name in self._handlers:
            raise ValueError(
                f"Query handler for '{handler.name}' is already registered "
                f"by method '{self._handlers[handler.name].method_name}'"
            )
        self._handlers[handler.name] = handler

    def get_handler(self, query_name: str) -> QueryHandler | None:
        """Get the handler for a query.

        Args:
            query_name: The query name.

        Returns:
            The handler, or None if not found.
        """
        return self._handlers.get(query_name)

    def list_handlers(self) -> list[QueryHandler]:
        """List all registered handlers."""
        return list(self._handlers.values())

    def has_handler(self, query_name: str) -> bool:
        """Check if a handler exists for the query."""
        return query_name in self._handlers

    def list_query_names(self) -> list[str]:
        """List all registered query names."""
        return list(self._handlers.keys())

    @classmethod
    def from_class(cls, workflow_class: type) -> QueryRegistry:
        """Create a registry from a workflow class.

        Scans the class for methods decorated with @query_handler.

        Args:
            workflow_class: The workflow class to scan.

        Returns:
            A registry with all query handlers from the class.
        """
        registry = cls()

        for attr_name in dir(workflow_class):
            if attr_name.startswith("_"):
                continue

            try:
                attr = getattr(workflow_class, attr_name)
            except AttributeError:
                continue

            if not callable(attr):
                continue

            if getattr(attr, "__orcher_query_handler__", False):
                query_name = getattr(attr, "__orcher_query_name__", attr_name)
                handler = QueryHandler(
                    name=query_name,
                    method_name=attr_name,
                    method=attr,
                )
                registry.register(handler)

        return registry


def execute_query(
    workflow_instance: Any,
    query_name: str,
    args: tuple = (),
    kwargs: dict[str, Any] | None = None,
) -> Any:
    """Execute a query on a workflow instance.

    This finds and calls the appropriate query handler.

    Args:
        workflow_instance: The workflow instance.
        query_name: Name of the query to execute.
        args: Positional arguments for the query.
        kwargs: Keyword arguments for the query.

    Returns:
        The query result.

    Raises:
        ValueError: If no handler exists for the query.
    """
    if kwargs is None:
        kwargs = {}

    workflow_class = type(workflow_instance)

    for attr_name in dir(workflow_class):
        if attr_name.startswith("_"):
            continue

        try:
            attr = getattr(workflow_class, attr_name)
        except AttributeError:
            continue

        if not callable(attr):
            continue

        if getattr(attr, "__orcher_query_handler__", False):
            handler_query_name = getattr(attr, "__orcher_query_name__", attr_name)
            if handler_query_name == query_name:
                method = getattr(workflow_instance, attr_name)
                return method(*args, **kwargs)

    raise ValueError(
        f"No query handler found for '{query_name}' in workflow '{workflow_class.__name__}'"
    )
