"""Event handling for workflows in the Orcher Python SDK.

This module provides utilities for handling events in workflows.
Events let external systems send data to running workflows.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Generic, TypeVar

__all__ = [
    "Event",
    "EventHandler",
    "EventQueue",
    "event_handler",
]

T = TypeVar("T")


@dataclass
class Event(Generic[T]):
    """An event received by a workflow.

    Events are external inputs that can be sent to running workflows
    to trigger actions or provide data.

    Attributes:
        name: The name of the event.
        payload: The event payload data.
        timestamp: When the event was received.
        sender_workflow_id: ID of the workflow that sent this event (if any).
    """

    name: str
    payload: T
    timestamp: datetime = field(default_factory=datetime.now)
    sender_workflow_id: str | None = None

    def __repr__(self) -> str:
        return f"Event(name={self.name!r}, timestamp={self.timestamp.isoformat()})"


@dataclass
class EventHandler:
    """Metadata for an event handler method.

    Attributes:
        name: The event name this handler responds to.
        method_name: Name of the handler method.
        method: The actual handler callable.
    """

    name: str
    method_name: str
    method: Callable[..., Any]


def event_handler(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to mark a method as an event handler.

    Event handlers are called when the workflow receives an event
    with the matching name.

    Args:
        name: The name of the event to handle.

    Returns:
        A decorator that marks the method as an event handler.

    Example:
        >>> @workflow(name="order-workflow")
        ... class OrderWorkflow:
        ...     def __init__(self):
        ...         self.cancelled = False
        ...
        ...     @event_handler("cancel")
        ...     async def handle_cancel(self, ctx: WorkflowContext, reason: str) -> None:
        ...         self.cancelled = True
        ...         print(f"Order cancelled: {reason}")
        ...
        ...     async def run(self, ctx: WorkflowContext, order_id: str) -> str:
        ...         # Check for cancellation
        ...         if self.cancelled:
        ...             return "cancelled"
        ...         # ... continue processing
    """

    def decorator(method: Callable[..., Any]) -> Callable[..., Any]:
        method.__orcher_event_handler__ = True  # type: ignore
        method.__orcher_event_name__ = name  # type: ignore
        return method

    return decorator


class EventQueue(Generic[T]):
    """A queue for receiving events in workflows.

    This provides a way to wait for specific events and process them
    in order.

    Example:
        >>> queue = EventQueue[str]("approvals")
        >>>
        >>> # In event handler
        >>> @event_handler("approval")
        >>> async def on_approval(self, ctx, data):
        ...     self.queue.put(data)
        >>>
        >>> # In workflow run
        >>> approval = await queue.get(timeout=24 * 3600)
    """

    def __init__(self, name: str) -> None:
        """Initialize the event queue.

        Args:
            name: Name for this queue (for debugging).
        """
        self._name = name
        self._queue: asyncio.Queue[T] = asyncio.Queue()
        self._events: list[Event[T]] = []

    @property
    def name(self) -> str:
        """Get the queue name."""
        return self._name

    def put(self, item: T, event_name: str = "") -> None:
        """Put an item in the queue.

        Args:
            item: The item to add.
            event_name: Optional event name for tracking.
        """
        self._queue.put_nowait(item)
        self._events.append(
            Event(
                name=event_name or self._name,
                payload=item,
            )
        )

    async def get(self, timeout: float | None = None) -> T:
        """Get an item from the queue.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            The next item from the queue.

        Raises:
            asyncio.TimeoutError: If timeout expires.
        """
        if timeout is not None:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        return await self._queue.get()

    def get_nowait(self) -> T | None:
        """Get an item without waiting.

        Returns:
            The next item, or None if queue is empty.
        """
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def empty(self) -> bool:
        """Check if the queue is empty."""
        return self._queue.empty()

    @property
    def history(self) -> list[Event[T]]:
        """Get the history of events received."""
        return list(self._events)

    def __len__(self) -> int:
        return self._queue.qsize()

    def __repr__(self) -> str:
        return f"EventQueue(name={self._name!r}, size={len(self)})"


class EventRegistry:
    """Registry for event handlers in a workflow class.

    This is used internally to track which methods handle which events.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, EventHandler] = {}

    def register(self, handler: EventHandler) -> None:
        """Register an event handler.

        Args:
            handler: The event handler to register.

        Raises:
            ValueError: If a handler for this event is already registered.
        """
        if handler.name in self._handlers:
            raise ValueError(
                f"Event handler for '{handler.name}' is already registered "
                f"by method '{self._handlers[handler.name].method_name}'"
            )
        self._handlers[handler.name] = handler

    def get_handler(self, event_name: str) -> EventHandler | None:
        """Get the handler for an event.

        Args:
            event_name: The event name.

        Returns:
            The handler, or None if not found.
        """
        return self._handlers.get(event_name)

    def list_handlers(self) -> list[EventHandler]:
        """List all registered handlers."""
        return list(self._handlers.values())

    def has_handler(self, event_name: str) -> bool:
        """Check if a handler exists for the event."""
        return event_name in self._handlers

    @classmethod
    def from_class(cls, workflow_class: type) -> EventRegistry:
        """Create a registry from a workflow class.

        Scans the class for methods decorated with @event_handler.

        Args:
            workflow_class: The workflow class to scan.

        Returns:
            A registry with all event handlers from the class.
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

            if getattr(attr, "__orcher_event_handler__", False):
                event_name = getattr(attr, "__orcher_event_name__", attr_name)
                handler = EventHandler(
                    name=event_name,
                    method_name=attr_name,
                    method=attr,
                )
                registry.register(handler)

        return registry
