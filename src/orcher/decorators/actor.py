"""The ``@actor`` class decorator and ``@operation`` method decorator.

Together they define an actor type and the operations it accepts.

Example::

    from orcher import actor, operation
    from orcher.actor import ActorContext, OperationMode

    @actor(name="ShoppingCart")
    class ShoppingCart:

        @operation(mode=OperationMode.EXCLUSIVE)
        async def add_item(self, ctx: ActorContext, item: dict) -> list:
            items = await ctx.state.get("items") or []
            items.append(item)
            await ctx.state.set("items", items)
            return items

        @operation(mode=OperationMode.SHARED)
        async def get_items(self, ctx: ActorContext) -> list:
            return await ctx.state.get("items") or []
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from typing import Any

from orcher.actor.types import ActorMetadata, OperationMetadata, OperationMode
from orcher.decorators.registry import GlobalRegistry

logger = logging.getLogger(__name__)

__all__ = ["actor", "operation"]

# Attribute that @operation sets on a method and @actor reads back.
_OPERATION_ATTR = "__orcher_operation__"


def operation(
    mode: OperationMode = OperationMode.EXCLUSIVE,
    *,
    timeout: float | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a method as an actor operation.

    Args:
        mode: Execution mode (EXCLUSIVE for writes, SHARED for reads).
        timeout: Optional timeout in seconds for this operation.

    Returns:
        Decorator that marks the method with operation metadata.
    """

    def decorator(method: Callable[..., Any]) -> Callable[..., Any]:
        setattr(
            method,
            _OPERATION_ATTR,
            {
                "mode": mode,
                "timeout": timeout,
            },
        )
        return method

    return decorator


def actor(
    name: str,
    *,
    description: str = "",
) -> Callable[[type[Any]], type[Any]]:
    """Register a class as an actor.

    Scans the class for methods decorated with ``@operation`` and registers
    them all with the GlobalRegistry.

    Args:
        name: Unique name for the actor type.
        description: Optional description.

    Returns:
        Class decorator.

    Example::

        @actor(name="Counter")
        class Counter:
            @operation(mode=OperationMode.EXCLUSIVE)
            async def increment(self, ctx: ActorContext, delta: int) -> int:
                count = await ctx.state.get("count") or 0
                count += delta
                await ctx.state.set("count", count)
                return count
    """

    def decorator(cls: type[Any]) -> type[Any]:
        operations: dict[str, OperationMetadata] = {}

        for method_name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            op_meta = getattr(method, _OPERATION_ATTR, None)
            if op_meta is not None:
                operations[method_name] = OperationMetadata(
                    actor_name=name,
                    operation_name=method_name,
                    mode=op_meta["mode"],
                    method=method,
                    timeout=op_meta.get("timeout"),
                )

        if not operations:
            logger.warning(
                f"Actor '{name}' ({cls.__name__}) has no @operation methods. "
                "Did you forget to decorate them?"
            )

        metadata = ActorMetadata(
            name=name,
            handler_class=cls,
            operations=operations,
            description=description or cls.__doc__ or "",
        )

        registry = GlobalRegistry.get_instance()
        registry.register_actor(metadata)

        # Attach metadata to the class for introspection
        cls.__orcher_actor__ = metadata

        logger.debug(
            f"Registered actor '{name}' with {len(operations)} operation(s): "
            f"{list(operations.keys())}"
        )

        return cls

    return decorator
