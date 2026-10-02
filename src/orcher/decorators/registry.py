"""Global registry of workflow, task, query, update, and actor metadata.

The decorators populate a process-wide singleton registry when the decorated
module is imported.

Handlers are either plain decorated functions or classes. A class-based
handler is instantiated with its dependencies before it runs.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcher.types import RetryPolicy

__all__ = [
    "GlobalRegistry",
    "WorkflowMetadata",
    "TaskMetadata",
    "QueryMetadata",
    "UpdateMetadata",
    "HandlerType",
]


class HandlerType(Enum):
    """Type of handler registration."""

    FUNCTION = auto()  # Standalone decorated function
    CLASS = auto()  # Method on a class that is constructed with its dependencies


@dataclass
class WorkflowMetadata:
    """Metadata for a registered workflow.

    Covers both function-based and class-based workflows.

    Attributes:
        name: Unique name of the workflow.
        version: Version string for the workflow.
        handler: The workflow function or class.
        handler_type: Whether this is a function or class-based handler.
        description: Optional description.
        cron_schedule: Optional cron expression for periodic execution.
        run_method: The ``run`` method, for class-based workflows.
    """

    name: str
    version: str
    handler: Callable[..., Any] | type[Any]
    handler_type: HandlerType = HandlerType.FUNCTION
    description: str = ""
    cron_schedule: str | None = None

    # Set only for class-based workflows.
    run_method: Callable[..., Any] | None = None

    @property
    def is_function(self) -> bool:
        """Check if this is a function-based workflow."""
        return self.handler_type == HandlerType.FUNCTION

    @property
    def is_class(self) -> bool:
        """Check if this is a class-based workflow."""
        return self.handler_type == HandlerType.CLASS

    @property
    def cls(self) -> type[Any] | None:
        """Get the handler class (for class-based workflows)."""
        if self.is_class:
            return self.handler  # type: ignore
        return None

    def __repr__(self) -> str:
        if self.is_function:
            name = getattr(self.handler, "__name__", str(self.handler))
            return f"WorkflowMetadata(name={self.name!r}, version={self.version!r}, fn={name})"
        else:
            cls_name = getattr(self.handler, "__name__", str(self.handler))
            return f"WorkflowMetadata(name={self.name!r}, version={self.version!r}, cls={cls_name})"


@dataclass
class TaskMetadata:
    """Metadata for a registered task.

    Covers both function-based tasks and task methods on a ``@tasks`` class.

    Attributes:
        name: Unique name of the task.
        handler: The task function or method.
        handler_type: Whether this is a function or class method.
        retry_policy: Optional retry policy configuration.
        timeout: Optional timeout in seconds.
        heartbeat_timeout: Optional heartbeat timeout in seconds.
        return_type: Optional return type hint for result deserialization.
        handler_class: The owning class, for class-based tasks.
        method_name: The method name on ``handler_class``, for class-based tasks.
    """

    name: str
    handler: Callable[..., Any]
    handler_type: HandlerType = HandlerType.FUNCTION
    retry_policy: RetryPolicy | dict[str, Any] | None = None
    timeout: float | None = None
    heartbeat_timeout: float | None = None

    # Lets a recorded result (a plain dict on replay) be restored to its dataclass.
    return_type: type[Any] | None = None

    # Set only for class-based tasks.
    handler_class: type[Any] | None = None
    method_name: str | None = None

    @property
    def is_function(self) -> bool:
        """Check if this is a function-based task."""
        return self.handler_type == HandlerType.FUNCTION

    @property
    def is_class(self) -> bool:
        """Check if this is a class-based task."""
        return self.handler_type == HandlerType.CLASS

    def __repr__(self) -> str:
        if self.is_function:
            name = getattr(self.handler, "__name__", str(self.handler))
            return f"TaskMetadata(name={self.name!r}, fn={name})"
        else:
            cls_name = getattr(self.handler_class, "__name__", "Unknown")
            return f"TaskMetadata(name={self.name!r}, method={cls_name}.{self.method_name})"


@dataclass
class UpdateMetadata:
    """Metadata for a registered update handler.

    Update handlers allow external clients to send synchronous mutations
    to running workflows and receive processed results back.

    Attributes:
        name: Unique name of the update handler.
        handler: The update handler function.
        handler_type: Whether this is a function or class method.
        description: Optional description.
        timeout: Optional timeout in seconds.
        return_type: Optional return type hint for result deserialization.
    """

    name: str
    handler: Callable[..., Any]
    handler_type: HandlerType = HandlerType.FUNCTION
    description: str = ""
    timeout: float | None = None
    return_type: type[Any] | None = None

    @property
    def is_function(self) -> bool:
        """Check if this is a function-based update handler."""
        return self.handler_type == HandlerType.FUNCTION

    def __repr__(self) -> str:
        name = getattr(self.handler, "__name__", str(self.handler))
        return f"UpdateMetadata(name={self.name!r}, fn={name})"


@dataclass
class QueryMetadata:
    """Metadata for a registered query handler.

    Query handlers allow external clients to read workflow state
    without affecting execution.

    Attributes:
        name: Unique name of the query handler.
        handler: The query handler function.
        handler_type: Whether this is a function or class method.
        description: Optional description.
        timeout: Optional timeout in seconds.
        cache_ttl: Optional cache TTL in seconds.
        return_type: Optional return type hint for result deserialization.
    """

    name: str
    handler: Callable[..., Any]
    handler_type: HandlerType = HandlerType.FUNCTION
    description: str = ""
    timeout: float | None = None
    cache_ttl: float | None = None
    return_type: type[Any] | None = None

    @property
    def is_function(self) -> bool:
        """Check if this is a function-based query handler."""
        return self.handler_type == HandlerType.FUNCTION

    def __repr__(self) -> str:
        name = getattr(self.handler, "__name__", str(self.handler))
        return f"QueryMetadata(name={self.name!r}, fn={name})"


class GlobalRegistry:
    """Singleton registry for workflow and task metadata.

    This registry stores metadata about all workflows and tasks registered
    via decorators. It is populated at module import time.

    The registry is thread-safe for concurrent access.

    Example:
        >>> registry = GlobalRegistry.get_instance()
        >>> workflow = registry.get_workflow("my-workflow")
        >>> task = registry.get_task("my-task")
    """

    _instance: GlobalRegistry | None = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        """Initialize the registry. Use get_instance() instead."""
        self._workflows: dict[str, WorkflowMetadata] = {}
        self._tasks: dict[str, TaskMetadata] = {}
        self._queries: dict[str, QueryMetadata] = {}
        self._updates: dict[str, UpdateMetadata] = {}
        self._actors: dict[str, Any] = {}  # ActorMetadata (from orcher.actor.types)
        self._registry_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> GlobalRegistry:
        """Get the singleton registry instance.

        Returns:
            The global registry instance.
        """
        if cls._instance is None:
            with cls._lock:
                # Re-check under the lock so concurrent callers create one instance.
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Clear every registration; intended for tests.

        Removes all registered workflows, tasks, queries, updates, and actors.
        """
        with cls._lock:
            if cls._instance is not None:
                cls._instance._workflows.clear()
                cls._instance._tasks.clear()
                cls._instance._queries.clear()
                cls._instance._updates.clear()
                cls._instance._actors.clear()

    def register_workflow(self, metadata: WorkflowMetadata) -> None:
        """Register a workflow.

        Args:
            metadata: The workflow metadata to register.

        Raises:
            ValueError: If a workflow with the same name is already registered.
        """
        with self._registry_lock:
            if metadata.name in self._workflows:
                existing = self._workflows[metadata.name]
                existing_name = getattr(existing.handler, "__name__", str(existing.handler))
                new_name = getattr(metadata.handler, "__name__", str(metadata.handler))
                raise ValueError(
                    f"Workflow '{metadata.name}' is already registered by {existing_name}. "
                    f"Cannot register again from {new_name}."
                )
            self._workflows[metadata.name] = metadata

    def register_task(self, metadata: TaskMetadata) -> None:
        """Register a task.

        Args:
            metadata: The task metadata to register.

        Raises:
            ValueError: If a task with the same name is already registered.
        """
        with self._registry_lock:
            if metadata.name in self._tasks:
                existing = self._tasks[metadata.name]
                existing_name = getattr(existing.handler, "__name__", str(existing.handler))
                new_name = getattr(metadata.handler, "__name__", str(metadata.handler))
                raise ValueError(
                    f"Task '{metadata.name}' is already registered by {existing_name}. "
                    f"Cannot register again from {new_name}."
                )
            self._tasks[metadata.name] = metadata

    def get_workflow(self, name: str) -> WorkflowMetadata | None:
        """Get workflow metadata by name.

        Args:
            name: The workflow name.

        Returns:
            The workflow metadata, or None if not found.
        """
        return self._workflows.get(name)

    def get_task(self, name: str) -> TaskMetadata | None:
        """Get task metadata by name.

        Args:
            name: The task name.

        Returns:
            The task metadata, or None if not found.
        """
        return self._tasks.get(name)

    def register_query(self, metadata: QueryMetadata) -> None:
        """Register a query handler.

        Args:
            metadata: The query handler metadata to register.

        Raises:
            ValueError: If a query handler with the same name is already registered.
        """
        with self._registry_lock:
            if metadata.name in self._queries:
                existing = self._queries[metadata.name]
                existing_name = getattr(existing.handler, "__name__", str(existing.handler))
                new_name = getattr(metadata.handler, "__name__", str(metadata.handler))
                raise ValueError(
                    f"Query handler '{metadata.name}' is already registered by {existing_name}. "
                    f"Cannot register again from {new_name}."
                )
            self._queries[metadata.name] = metadata

    def get_query(self, name: str) -> QueryMetadata | None:
        """Get query handler metadata by name.

        Args:
            name: The query handler name.

        Returns:
            The query handler metadata, or None if not found.
        """
        return self._queries.get(name)

    def list_queries(self) -> list[QueryMetadata]:
        """List all registered query handlers.

        Returns:
            List of query handler metadata.
        """
        return list(self._queries.values())

    def has_query(self, name: str) -> bool:
        """Check if a query handler is registered.

        Args:
            name: The query handler name.

        Returns:
            True if the query handler is registered.
        """
        return name in self._queries

    @property
    def query_count(self) -> int:
        """Get the number of registered query handlers."""
        return len(self._queries)

    def register_update(self, metadata: UpdateMetadata) -> None:
        """Register an update handler.

        Args:
            metadata: The update handler metadata to register.

        Raises:
            ValueError: If an update handler with the same name is already registered.
        """
        with self._registry_lock:
            if metadata.name in self._updates:
                existing = self._updates[metadata.name]
                existing_name = getattr(existing.handler, "__name__", str(existing.handler))
                new_name = getattr(metadata.handler, "__name__", str(metadata.handler))
                raise ValueError(
                    f"Update handler '{metadata.name}' is already registered by {existing_name}. "
                    f"Cannot register again from {new_name}."
                )
            self._updates[metadata.name] = metadata

    def get_update(self, name: str) -> UpdateMetadata | None:
        """Get update handler metadata by name.

        Args:
            name: The update handler name.

        Returns:
            The update handler metadata, or None if not found.
        """
        return self._updates.get(name)

    def list_updates(self) -> list[UpdateMetadata]:
        """List all registered update handlers.

        Returns:
            List of update handler metadata.
        """
        return list(self._updates.values())

    def has_update(self, name: str) -> bool:
        """Check if an update handler is registered.

        Args:
            name: The update handler name.

        Returns:
            True if the update handler is registered.
        """
        return name in self._updates

    @property
    def update_count(self) -> int:
        """Get the number of registered update handlers."""
        return len(self._updates)

    def list_workflows(self) -> list[WorkflowMetadata]:
        """List all registered workflows.

        Returns:
            List of workflow metadata.
        """
        return list(self._workflows.values())

    def list_tasks(self) -> list[TaskMetadata]:
        """List all registered tasks.

        Returns:
            List of task metadata.
        """
        return list(self._tasks.values())

    def has_workflow(self, name: str) -> bool:
        """Check if a workflow is registered.

        Args:
            name: The workflow name.

        Returns:
            True if the workflow is registered.
        """
        return name in self._workflows

    def has_task(self, name: str) -> bool:
        """Check if a task is registered.

        Args:
            name: The task name.

        Returns:
            True if the task is registered.
        """
        return name in self._tasks

    @property
    def workflow_count(self) -> int:
        """Get the number of registered workflows."""
        return len(self._workflows)

    @property
    def task_count(self) -> int:
        """Get the number of registered tasks."""
        return len(self._tasks)

    def register_actor(self, metadata: Any) -> None:
        """Register an actor.

        Args:
            metadata: The ActorMetadata to register.

        Raises:
            ValueError: If an actor with the same name is already registered.
        """
        with self._registry_lock:
            name = metadata.name
            if name in self._actors:
                raise ValueError(f"Actor '{name}' is already registered. Cannot register again.")
            self._actors[name] = metadata

    def get_actor(self, name: str) -> Any | None:
        """Get actor metadata by name."""
        return self._actors.get(name)

    def list_actors(self) -> list[Any]:
        """List all registered actors."""
        return list(self._actors.values())

    def has_actor(self, name: str) -> bool:
        """Check if an actor is registered."""
        return name in self._actors

    @property
    def actor_count(self) -> int:
        """Get the number of registered actors."""
        return len(self._actors)

    def __repr__(self) -> str:
        return (
            f"GlobalRegistry(workflows={self.workflow_count}, "
            f"tasks={self.task_count}, queries={self.query_count}, "
            f"updates={self.update_count}, actors={self.actor_count})"
        )
