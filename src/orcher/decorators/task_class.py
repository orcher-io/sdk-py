"""The ``@tasks`` class decorator for class-based task handlers.

A task class is constructed with its dependencies, so its task methods can use
injected clients and services.
"""

from __future__ import annotations

from typing import Any, TypeVar

from orcher.decorators.registry import GlobalRegistry, HandlerType, TaskMetadata
from orcher.task.reference import TaskReference

__all__ = ["tasks", "register_task_class"]

T = TypeVar("T")


def tasks(cls: type[T]) -> type[T]:
    """Register a class containing task methods.

    Scans the class for methods decorated with ``@task``, registers them in
    the GlobalRegistry, and replaces each one with a TaskReference that
    workflows pass to ``ctx.execute_task``.

    A Service executes the tasks on an instance of the class constructed with
    its dependencies.

    Args:
        cls: The class containing task methods decorated with @task.

    Returns:
        The same class with task methods replaced by TaskReferences.

    Raises:
        ValueError: If a task name is already registered.

    Example:
        >>> from orcher import task, TaskContext, tasks
        >>>
        >>> @tasks
        ... class PaymentTasks:
        ...     def __init__(self, gateway: PaymentGateway):
        ...         self.gateway = gateway
        ...
        ...     @task(name="charge-card")
        ...     async def charge_card(self, ctx: TaskContext, amount: int) -> str:
        ...         return await self.gateway.charge(amount)
        >>>
        >>> # PaymentTasks.charge_card is now a TaskReference
        >>> # In a workflow:
        >>> result = await ctx.execute_task(PaymentTasks.charge_card, amount=100)
    """
    registry = GlobalRegistry.get_instance()

    # Collect the @task methods, then register and replace each one.
    task_methods: list[tuple[str, str, Any]] = []

    for attr_name in dir(cls):
        if attr_name.startswith("_"):
            continue

        try:
            attr = getattr(cls, attr_name)
        except AttributeError:
            continue

        if not callable(attr):
            continue

        if not getattr(attr, "__orcher_is_task__", False):
            continue

        task_name = getattr(attr, "__orcher_task_name__", None)
        if task_name is None:
            continue

        original_metadata = getattr(attr, "__orcher_task__", None)

        task_methods.append((attr_name, task_name, original_metadata))

    for attr_name, task_name, original_metadata in task_methods:
        method = getattr(cls, attr_name)

        # Register the undecorated function, not the @task wrapper.
        handler = method
        if hasattr(method, "__wrapped__"):
            handler = method.__wrapped__

        metadata = TaskMetadata(
            name=task_name,
            handler=handler,
            handler_type=HandlerType.CLASS,
            retry_policy=original_metadata.retry_policy if original_metadata else None,
            timeout=original_metadata.timeout if original_metadata else None,
            heartbeat_timeout=original_metadata.heartbeat_timeout if original_metadata else None,
            handler_class=cls,
            method_name=attr_name,
            return_type=original_metadata.return_type if original_metadata else None,
        )

        registry.register_task(metadata)

        task_ref: TaskReference[Any, Any] = TaskReference(
            task_name=task_name,
            handler_class=cls,
            method_name=attr_name,
            metadata=metadata,
        )
        setattr(cls, attr_name, task_ref)

    cls.__orcher_task_class__ = True  # type: ignore

    return cls


# Alias of `tasks`.
register_task_class = tasks
