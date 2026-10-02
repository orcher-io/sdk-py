"""
Base Data Converter Protocol for ORCHER Python SDK

This module defines the DataConverter protocol that all converters must implement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcher.converter.payload import Payload

__all__ = [
    "DataConverter",
    "PayloadEncoding",
]


class PayloadEncoding(Enum):
    """Standard payload encoding types.

    These encodings determine how data is serialized in the Payload.
    """

    #: Plain JSON encoding (default)
    JSON = "json/plain"

    #: Binary/raw data
    BINARY = "binary/raw"

    #: Null/None value
    NULL = "binary/null"

    #: Protocol Buffers (no built-in converter produces it)
    PROTOBUF = "binary/protobuf"

    #: MessagePack encoding
    MSGPACK = "binary/msgpack"

    #: Pickle encoding (Python-specific, use with caution)
    PICKLE = "binary/pickle"


class DataConverter(ABC):
    """Abstract base class for data converters.

    A DataConverter handles serialization and deserialization between
    Python objects and ORCHER Payloads.

    Implementations must handle:
    - None/null values
    - Primitive types (int, float, str, bool)
    - Collections (list, dict, tuple, set)
    - Custom objects (via type hints or registration)

    Example:
        >>> class MyConverter(DataConverter):
        ...     def to_payload(self, value: Any) -> Payload:
        ...         # Serialize value to Payload
        ...         ...
        ...
        ...     def from_payload(
        ...         self,
        ...         payload: Payload,
        ...         type_hint: type | None = None
        ...     ) -> Any:
        ...         # Deserialize Payload to value
        ...         ...
    """

    @abstractmethod
    def to_payload(self, value: Any) -> Payload:
        """Convert a Python value to a Payload.

        Args:
            value: Python value to convert. Can be:
                - None
                - Primitive types (int, float, str, bool, bytes)
                - Collections (list, dict, tuple, set)
                - Dataclasses
                - Custom objects (if registered)

        Returns:
            Payload with serialized data and metadata.

        Raises:
            TypeError: If the value type is not supported.
            ValueError: If serialization fails.
        """
        ...

    @abstractmethod
    def from_payload(
        self,
        payload: Payload,
        type_hint: Any = None,
    ) -> Any:
        """Convert a Payload to a Python value.

        Args:
            payload: Payload to convert.
            type_hint: Optional type hint for deserialization.
                If provided, the converter will attempt to deserialize
                the payload into an instance of this type.

        Returns:
            Python value deserialized from the payload.

        Raises:
            TypeError: If deserialization to the type hint fails.
            ValueError: If the payload is malformed.
        """
        ...

    def to_payloads(self, values: list[Any]) -> list[Payload]:
        """Convert multiple Python values to Payloads.

        Args:
            values: List of Python values to convert.

        Returns:
            List of Payloads.
        """
        return [self.to_payload(value) for value in values]

    def from_payloads(
        self,
        payloads: list[Payload],
        type_hints: list[Any] | None = None,
    ) -> list[Any]:
        """Convert multiple Payloads to Python values.

        Args:
            payloads: List of Payloads to convert.
            type_hints: Optional list of type hints for each payload.
                If provided, must have the same length as payloads.

        Returns:
            List of Python values.
        """
        hints: list[Any]
        if type_hints is None:
            hints = [None] * len(payloads)
        elif len(type_hints) != len(payloads):
            raise ValueError(
                f"type_hints length ({len(type_hints)}) must match "
                f"payloads length ({len(payloads)})"
            )
        else:
            hints = type_hints

        return [
            self.from_payload(payload, type_hint)
            for payload, type_hint in zip(payloads, hints, strict=False)
        ]

    @property
    @abstractmethod
    def encoding(self) -> PayloadEncoding:
        """Get the encoding used by this converter."""
        ...
