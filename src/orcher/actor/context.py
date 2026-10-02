"""Actor execution contexts.

Provides ActorContext (read-write) for exclusive operations and
SharedActorContext (semantically read-only) for shared operations.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from orcher.actor.types import ActorKey

if TYPE_CHECKING:
    from orcher.actor.state.manager import ActorStateManager

__all__ = [
    "ActorContext",
    "SharedActorContext",
]


class ActorContext:
    """Context for exclusive actor operations.

    Provides access to actor identity, state management, and
    deterministic utilities. Passed as the first argument to
    operations decorated with ``@operation(mode=OperationMode.EXCLUSIVE)``.

    Example::

        @operation(mode=OperationMode.EXCLUSIVE)
        async def add_item(self, ctx: ActorContext, item: dict) -> list:
            items = await ctx.state.get("items") or []
            items.append(item)
            await ctx.state.set("items", items)
            return items
    """

    def __init__(
        self,
        actor_key: ActorKey,
        state_manager: ActorStateManager,
        execution_id: str,
        workflow_execution_id: str | None = None,
    ) -> None:
        self._actor_key = actor_key
        self._state_manager = state_manager
        self._execution_id = execution_id
        self._workflow_execution_id = workflow_execution_id

    @property
    def actor_name(self) -> str:
        """Name of the actor type."""
        return self._actor_key.actor_name

    @property
    def key(self) -> str:
        """Instance key for this actor."""
        return self._actor_key.key

    @property
    def execution_id(self) -> str:
        """Unique ID for the current operation execution."""
        return self._execution_id

    @property
    def actor_key(self) -> ActorKey:
        """Full actor key (name + instance key)."""
        return self._actor_key

    @property
    def state(self) -> ActorStateManager:
        """State manager for get/set/delete operations."""
        return self._state_manager

    @property
    def workflow_execution_id(self) -> str | None:
        """Workflow execution ID, if this operation was triggered by a workflow."""
        return self._workflow_execution_id

    def deterministic_uuid(self, seed: str) -> uuid.UUID:
        """Generate a deterministic UUID from the actor key and a seed string."""
        namespace = self._actor_key.to_workflow_id()
        return uuid.uuid5(namespace, seed)


class SharedActorContext:
    """Context for shared (read-only) actor operations.

    Semantically identical to ActorContext but signals that the operation
    should only read state, not mutate it. The server may execute shared
    operations concurrently for the same key.

    Example::

        @operation(mode=OperationMode.SHARED)
        async def get_items(self, ctx: SharedActorContext) -> list:
            return await ctx.state.get("items") or []
    """

    def __init__(
        self,
        actor_key: ActorKey,
        state_manager: ActorStateManager,
        execution_id: str,
        workflow_execution_id: str | None = None,
    ) -> None:
        self._inner = ActorContext(
            actor_key=actor_key,
            state_manager=state_manager,
            execution_id=execution_id,
            workflow_execution_id=workflow_execution_id,
        )

    @property
    def actor_name(self) -> str:
        return self._inner.actor_name

    @property
    def key(self) -> str:
        return self._inner.key

    @property
    def execution_id(self) -> str:
        return self._inner.execution_id

    @property
    def actor_key(self) -> ActorKey:
        return self._inner.actor_key

    @property
    def state(self) -> ActorStateManager:
        """State accessor (prefer get/list only for shared operations)."""
        return self._inner.state

    @property
    def workflow_execution_id(self) -> str | None:
        return self._inner.workflow_execution_id

    def deterministic_uuid(self, seed: str) -> uuid.UUID:
        return self._inner.deterministic_uuid(seed)
