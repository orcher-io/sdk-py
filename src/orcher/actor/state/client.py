"""Actor state client.

Low-level client for actor state operations. Supports both a gRPC backend
(for production) and a mock backend (for testing).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, TypeVar

from orcher.actor.state.mock import MockStateBackend

logger = logging.getLogger(__name__)

T = TypeVar("T")

__all__ = [
    "ActorStateClient",
    "ActorStateClientConfig",
]


@dataclass
class ActorStateClientConfig:
    """Configuration for the actor state client."""

    server_url: str = "http://localhost:8080"
    timeout: float = 10.0
    max_retries: int = 3
    enable_cache: bool = True
    cache_ttl: float = 60.0
    auth_token: str | None = None


class ActorStateClient:
    """Client for actor state operations.

    Uses either the native bridge or an in-memory backend for tests. Through
    the bridge, state calls go to the server's ActorService gRPC endpoint.
    With neither backend, reads return empty results and writes are dropped,
    each with a logged warning.
    """

    def __init__(
        self,
        server_url: str | None = None,
        config: ActorStateClientConfig | None = None,
        *,
        bridge: Any | None = None,
        _mock_backend: MockStateBackend | None = None,
    ) -> None:
        self._config = config or ActorStateClientConfig(
            server_url=server_url or "http://localhost:50051",
        )
        self._bridge = bridge
        self._mock = _mock_backend

    @classmethod
    def new_mock(cls, backend: MockStateBackend | None = None) -> ActorStateClient:
        """Create a mock client backed by in-memory storage."""
        return cls(_mock_backend=backend or MockStateBackend())

    @property
    def is_mock(self) -> bool:
        return self._mock is not None

    def _full_key(self, actor_name: str, key: str, state_key: str) -> str:
        return f"actor:{actor_name}:{key}:{state_key}"

    async def get_state(
        self,
        actor_name: str,
        key: str,
        state_key: str,
        execution_id: str,
    ) -> Any | None:
        """Get a state value, returning None if not found."""
        full_key = self._full_key(actor_name, key, state_key)

        if self._mock is not None:
            raw = await self._mock.get(full_key)
            if raw is None:
                return None
            return json.loads(raw)

        if self._bridge is not None and hasattr(self._bridge, "actor_get_state"):
            result_bytes = await self._bridge.actor_get_state(
                actor_name=actor_name,
                key=key,
                state_key=state_key,
                execution_id=execution_id,
            )
            if result_bytes is None:
                return None
            return json.loads(result_bytes)

        logger.warning("No state backend available; returning None")
        return None

    async def set_state(
        self,
        actor_name: str,
        key: str,
        state_key: str,
        value: Any,
        execution_id: str,
    ) -> None:
        """Set a state value."""
        full_key = self._full_key(actor_name, key, state_key)
        raw = json.dumps(value).encode("utf-8")

        if self._mock is not None:
            await self._mock.set(full_key, raw)
            return

        if self._bridge is not None and hasattr(self._bridge, "actor_set_state"):
            await self._bridge.actor_set_state(
                actor_name=actor_name,
                key=key,
                state_key=state_key,
                value=raw,
                execution_id=execution_id,
            )
            return

        logger.warning("No state backend available; set ignored")

    async def delete_state(
        self,
        actor_name: str,
        key: str,
        state_key: str,
        execution_id: str,
    ) -> bool:
        """Delete a state value. Returns True if it existed."""
        full_key = self._full_key(actor_name, key, state_key)

        if self._mock is not None:
            return await self._mock.delete(full_key)

        if self._bridge is not None and hasattr(self._bridge, "actor_delete_state"):
            result = await self._bridge.actor_delete_state(
                actor_name=actor_name,
                key=key,
                state_key=state_key,
                execution_id=execution_id,
            )
            return bool(result)

        logger.warning("No state backend available; delete ignored")
        return False

    async def list_state_keys(
        self,
        actor_name: str,
        key: str,
        execution_id: str,
        prefix: str | None = None,
    ) -> list[str]:
        """List state keys, optionally filtered by prefix."""
        full_prefix = self._full_key(actor_name, key, prefix or "")

        if self._mock is not None:
            full_keys = await self._mock.list_keys(full_prefix)
            # Return keys relative to this actor instance.
            base = self._full_key(actor_name, key, "")
            return [k[len(base) :] for k in full_keys]

        if self._bridge is not None and hasattr(self._bridge, "actor_list_state_keys"):
            keys = await self._bridge.actor_list_state_keys(
                actor_name=actor_name,
                key=key,
                execution_id=execution_id,
                prefix=prefix or "",
            )
            return list(keys) if keys else []

        logger.warning("No state backend available; list_keys returning empty")
        return []
