"""Core actor types.

Defines ActorKey, OperationMode, and metadata types used throughout
the actor subsystem.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

__all__ = [
    "ActorKey",
    "OperationMode",
    "OperationMetadata",
    "ActorMetadata",
]


class OperationMode(str, Enum):
    """Execution mode for an actor operation.

    - EXCLUSIVE: Single-writer, serialized execution per key.
    - SHARED: Multi-reader, concurrent reads allowed.
    """

    EXCLUSIVE = "exclusive"
    SHARED = "shared"

    def is_exclusive(self) -> bool:
        return self is OperationMode.EXCLUSIVE

    def is_shared(self) -> bool:
        return self is OperationMode.SHARED


@dataclass(frozen=True)
class ActorKey:
    """Unique identity for an actor instance.

    Composed of the actor type name and the instance key (e.g. user ID).
    """

    actor_name: str
    key: str

    def storage_prefix(self) -> str:
        """Storage key prefix for this actor instance."""
        return f"actor:{self.actor_name}:{self.key}"

    def to_workflow_id(self) -> uuid.UUID:
        """Deterministic UUID v5 derived from actor identity."""
        # The namespace is the RFC 4122 DNS namespace. Changing it changes every derived id.
        namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        return uuid.uuid5(namespace, f"{self.actor_name}:{self.key}")

    def __str__(self) -> str:
        return f"{self.actor_name}:{self.key}"


@dataclass
class OperationMetadata:
    """Metadata for a single actor operation."""

    actor_name: str
    operation_name: str
    mode: OperationMode
    method: Callable[..., Any] | None = None
    timeout: float | None = None

    def qualified_name(self) -> str:
        return f"{self.actor_name}.{self.operation_name}"


@dataclass
class ActorMetadata:
    """Metadata for a registered actor class."""

    name: str
    handler_class: type[Any]
    operations: dict[str, OperationMetadata] = field(default_factory=dict)
    description: str = ""

    @property
    def operation_count(self) -> int:
        return len(self.operations)

    def operation_names(self) -> list[str]:
        return list(self.operations.keys())
