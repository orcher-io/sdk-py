"""Actor invocation client.

Provides ActorInvocationClient for invoking actor operations from
workflows or external code.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Sends not finished yet, held so they are not garbage-collected mid-way.
_sends_in_flight: set[asyncio.Future[Any]] = set()

__all__ = ["ActorInvocationClient"]


class ActorInvocationClient:
    """Client for invoking actor operations.

    Wraps the native bridge to send operation requests to actors
    and receive results.

    Example::

        client = ActorInvocationClient(bridge, "ShoppingCart", "user-123")
        items = await client.invoke("get_items")
        await client.invoke("add_item", {"name": "Widget", "price": 9.99})
    """

    def __init__(
        self,
        bridge: Any,
        actor_name: str,
        key: str,
    ) -> None:
        self._bridge = bridge
        self._actor_name = actor_name
        self._key = key

    @property
    def actor_name(self) -> str:
        return self._actor_name

    @property
    def key(self) -> str:
        return self._key

    async def invoke(
        self,
        operation: str,
        input_data: Any = None,
        *,
        timeout_ms: int | None = None,
    ) -> Any:
        """Invoke an actor operation and wait for the result.

        Args:
            operation: Operation name to invoke.
            input_data: Input data (will be JSON-serialized).
            timeout_ms: Optional timeout in milliseconds.

        Returns:
            The operation result (JSON-deserialized), or None if no native
            bridge is attached; that case is logged as a warning.
        """
        payload = json.dumps(input_data).encode("utf-8") if input_data is not None else b"null"

        if self._bridge is not None and hasattr(self._bridge, "invoke_actor_operation"):
            result_bytes = await self._bridge.invoke_actor_operation(
                actor_name=self._actor_name,
                key=self._key,
                operation=operation,
                payload=payload,
                timeout_ms=timeout_ms,
            )
            if result_bytes is not None:
                return json.loads(result_bytes)
            return None

        # Without a native bridge the call cannot reach the server.
        logger.warning(
            f"Native bridge not available for actor invocation: "
            f"{self._actor_name}.{operation}(key={self._key})"
        )
        return None

    async def send(
        self,
        operation: str,
        input_data: Any = None,
        *,
        timeout_ms: int | None = None,
    ) -> None:
        """Invoke an operation without waiting for its result.

        The bridge must provide ``send_actor_operation``. The native
        ``BridgeWorker`` does not, so with it this call only logs a warning.

        Args:
            operation: Operation name to invoke.
            input_data: Input data (will be JSON-serialized).
            timeout_ms: Optional timeout in milliseconds.
        """
        # The operation is invoked as invoke() does, in a task of its own that
        # nothing waits for; a failure is logged.
        sending = asyncio.ensure_future(
            self.invoke(operation, input_data, timeout_ms=timeout_ms)
        )
        _sends_in_flight.add(sending)
        sending.add_done_callback(self._sent)

    def _sent(self, sending: asyncio.Future[Any]) -> None:
        _sends_in_flight.discard(sending)
        if not sending.cancelled() and sending.exception() is not None:
            logger.warning(
                "Actor send failed: %s(key=%s): %s",
                self._actor_name,
                self._key,
                sending.exception(),
            )
