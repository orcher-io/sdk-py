"""Actor support for the ORCHER Python SDK.

Actors are stateful, keyed entities with exclusive and shared operations.
Each actor instance (identified by name + key) has its own persistent state.

Example::

    from orcher import actor, operation
    from orcher.actor import ActorContext, SharedActorContext, OperationMode

    @actor(name="Counter")
    class Counter:

        @operation(mode=OperationMode.EXCLUSIVE)
        async def increment(self, ctx: ActorContext, delta: int = 1) -> int:
            count = await ctx.state.get("count") or 0
            count += delta
            await ctx.state.set("count", count)
            return count

        @operation(mode=OperationMode.SHARED)
        async def get_count(self, ctx: ActorContext) -> int:
            return await ctx.state.get("count") or 0
"""

from orcher.actor.client import ActorInvocationClient
from orcher.actor.context import ActorContext, SharedActorContext
from orcher.actor.state import (
    ActorStateClient,
    ActorStateClientConfig,
    ActorStateManager,
    MockStateBackend,
)
from orcher.actor.types import ActorKey, ActorMetadata, OperationMetadata, OperationMode

__all__ = [
    # Core types
    "ActorKey",
    "OperationMode",
    "OperationMetadata",
    "ActorMetadata",
    # Contexts
    "ActorContext",
    "SharedActorContext",
    # State
    "ActorStateClient",
    "ActorStateClientConfig",
    "ActorStateManager",
    "MockStateBackend",
    # Client
    "ActorInvocationClient",
]
