"""Mock state backend for testing.

Provides an in-memory implementation of the state storage interface,
enabling actor tests without a running server.
"""

from __future__ import annotations

__all__ = ["MockStateBackend"]


class MockStateBackend:
    """In-memory state backend for testing actors.

    Example::

        mock = MockStateBackend()
        client = ActorStateClient.new_mock(mock)
        ctx = ActorContext(actor_key, ActorStateManager(...), "exec-1")
    """

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    async def get(self, full_key: str) -> bytes | None:
        return self._store.get(full_key)

    async def set(self, full_key: str, value: bytes) -> None:
        self._store[full_key] = value

    async def delete(self, full_key: str) -> bool:
        if full_key in self._store:
            del self._store[full_key]
            return True
        return False

    async def list_keys(self, prefix: str) -> list[str]:
        return [k for k in self._store if k.startswith(prefix)]

    def clear(self) -> None:
        self._store.clear()
