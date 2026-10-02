"""High-level actor state manager.

Wraps ActorStateClient with convenience methods scoped to a specific
actor instance. This is what ``ctx.state`` returns.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from orcher.actor.state.client import ActorStateClient
from orcher.actor.types import ActorKey

T = TypeVar("T")

__all__ = ["ActorStateManager"]


class ActorStateManager:
    """High-level state management for an actor instance.

    All operations are scoped to the actor's key (name + instance key).

    Example::

        items = await ctx.state.get("items")
        await ctx.state.set("items", [1, 2, 3])
        await ctx.state.delete("items")
        keys = await ctx.state.list()
    """

    def __init__(
        self,
        actor_key: ActorKey,
        client: ActorStateClient,
        execution_id: str,
    ) -> None:
        self._actor_key = actor_key
        self._client = client
        self._execution_id = execution_id

    @property
    def actor_name(self) -> str:
        return self._actor_key.actor_name

    @property
    def key(self) -> str:
        return self._actor_key.key

    @property
    def execution_id(self) -> str:
        return self._execution_id

    @property
    def actor_key(self) -> ActorKey:
        return self._actor_key

    async def get(self, key: str) -> Any | None:
        """Get a state value by key."""
        return await self._client.get_state(
            self._actor_key.actor_name,
            self._actor_key.key,
            key,
            self._execution_id,
        )

    async def set(self, key: str, value: Any) -> None:
        """Set a state value."""
        await self._client.set_state(
            self._actor_key.actor_name,
            self._actor_key.key,
            key,
            value,
            self._execution_id,
        )

    async def delete(self, key: str) -> bool:
        """Delete a state value. Returns True if it existed."""
        return await self._client.delete_state(
            self._actor_key.actor_name,
            self._actor_key.key,
            key,
            self._execution_id,
        )

    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        return (await self.get(key)) is not None

    async def list(self, prefix: str | None = None) -> list[str]:
        """List state keys, optionally filtered by prefix."""
        return await self._client.list_state_keys(
            self._actor_key.actor_name,
            self._actor_key.key,
            self._execution_id,
            prefix=prefix,
        )

    async def clear_all(self) -> None:
        """Delete all state for this actor instance."""
        keys = await self.list()
        for key in keys:
            await self.delete(key)

    async def update(self, key: str, fn: Callable[[Any | None], Any]) -> Any:
        """Read a state value, transform it with ``fn``, and write it back.

        The read and write are separate calls. The sequence is safe from lost
        updates inside an exclusive operation, which runs serialized per actor key.

        Args:
            key: The state key.
            fn: A function that receives the current value (or None) and returns
                the new value.

        Returns:
            The new value.
        """
        current = await self.get(key)
        new_value = fn(current)
        await self.set(key, new_value)
        return new_value
