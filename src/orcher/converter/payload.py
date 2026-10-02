"""
Payload Type and Conversion Utilities for ORCHER Python SDK

This module defines the Payload type and provides utilities for converting
between Python values and Payloads.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcher.converter.base import DataConverter

__all__ = [
    "Payload",
    "METADATA_ENCODING",
    "METADATA_CONTENT_TYPE",
    "to_payload",
    "from_payload",
    "to_payloads",
    "from_payloads",
    "serialize_payload",
    "deserialize_payload",
    "serialize_payloads",
    "deserialize_payloads",
    "SerializedPayload",
]

# Metadata keys
METADATA_ENCODING = "encoding"
METADATA_CONTENT_TYPE = "content-type"


@dataclass
class Payload:
    """Serialized data payload with metadata.

    A Payload is the primary data transfer format between Python and ORCHER.
    It consists of:
    - data: Serialized bytes (typically JSON-encoded)
    - metadata: Key-value pairs with encoding/type information

    The metadata typically includes:
    - "encoding": The encoding type (e.g., "json/plain", "binary/raw")
    - "content-type": MIME type (e.g., "application/json")

    Example:
        >>> # Create from JSON data
        >>> payload = Payload.from_json({"name": "Alice", "age": 30})
        >>> print(payload.encoding)
        'json/plain'
        >>>
        >>> # Convert back to Python
        >>> data = payload.to_json()
        >>> assert data == {"name": "Alice", "age": 30}
        >>>
        >>> # Create from raw bytes
        >>> binary_payload = Payload.from_bytes(b"raw data")
        >>> print(binary_payload.encoding)
        'binary/raw'
    """

    data: bytes = field(default_factory=bytes)
    metadata: dict[str, bytes] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> Payload:
        """Create an empty payload."""
        return cls(data=b"", metadata={})

    @classmethod
    def null(cls) -> Payload:
        """Create a null payload (represents None)."""
        return cls(
            data=b"",
            metadata={
                METADATA_ENCODING: b"binary/null",
            },
        )

    @classmethod
    def from_json(cls, value: Any) -> Payload:
        """Create a payload from JSON-serializable data.

        Args:
            value: Any JSON-serializable Python value.

        Returns:
            Payload with JSON-encoded data.

        Example:
            >>> payload = Payload.from_json({"key": "value"})
            >>> payload.to_json()
            {'key': 'value'}
        """
        import json

        data = json.dumps(value, separators=(",", ":")).encode("utf-8")
        return cls(
            data=data,
            metadata={
                METADATA_ENCODING: b"json/plain",
                METADATA_CONTENT_TYPE: b"application/json",
            },
        )

    def to_json(self) -> Any:
        """Deserialize payload as JSON.

        Returns:
            Python value from JSON-decoded data.

        Raises:
            ValueError: If the payload is not JSON-encoded.
            json.JSONDecodeError: If the data is invalid JSON.
        """
        import json

        encoding = self.encoding
        if encoding and encoding not in ("json/plain", "json"):
            raise ValueError(f"Cannot decode payload with encoding '{encoding}' as JSON")

        if not self.data:
            return None

        return json.loads(self.data.decode("utf-8"))

    @classmethod
    def from_bytes(cls, data: bytes, content_type: str = "application/octet-stream") -> Payload:
        """Create a payload from raw bytes.

        Args:
            data: Raw bytes to store.
            content_type: MIME type of the data.

        Returns:
            Payload with binary data.
        """
        return cls(
            data=data,
            metadata={
                METADATA_ENCODING: b"binary/raw",
                METADATA_CONTENT_TYPE: content_type.encode("utf-8"),
            },
        )

    def to_bytes(self) -> bytes:
        """Get the raw data bytes.

        Returns:
            Raw bytes from the payload.
        """
        return self.data

    @classmethod
    def from_string(cls, value: str) -> Payload:
        """Create a payload from a string.

        Args:
            value: String value to store.

        Returns:
            Payload with UTF-8 encoded string.
        """
        return cls(
            data=value.encode("utf-8"),
            metadata={
                METADATA_ENCODING: b"utf-8",
                METADATA_CONTENT_TYPE: b"text/plain",
            },
        )

    def to_string(self) -> str:
        """Get payload as string.

        Returns:
            UTF-8 decoded string.
        """
        return self.data.decode("utf-8")

    @property
    def encoding(self) -> str | None:
        """Get the encoding type from metadata.

        Returns:
            Encoding string or None if not set.
        """
        enc = self.metadata.get(METADATA_ENCODING)
        return enc.decode("utf-8") if enc else None

    @property
    def content_type(self) -> str | None:
        """Get the content type from metadata.

        Returns:
            Content type string or None if not set.
        """
        ct = self.metadata.get(METADATA_CONTENT_TYPE)
        return ct.decode("utf-8") if ct else None

    def is_null(self) -> bool:
        """Check if this is a null payload (represents None)."""
        return self.encoding == "binary/null"

    def is_empty(self) -> bool:
        """Check if payload has no data."""
        return len(self.data) == 0

    def is_json(self) -> bool:
        """Check if payload is JSON-encoded."""
        encoding = self.encoding
        return encoding in ("json/plain", "json", None) and not self.is_null()

    def __len__(self) -> int:
        """Get payload size in bytes."""
        return len(self.data)

    def __bool__(self) -> bool:
        """Check if payload has data (not null or empty)."""
        return not self.is_null() and not self.is_empty()

    def __repr__(self) -> str:
        encoding = self.encoding or "unknown"
        size = len(self.data)
        return f"Payload(encoding={encoding!r}, size={size})"


# The form a Payload takes inside JSON messages exchanged with the native module.
class SerializedPayload:
    """Serialized payload format for JSON transfer.

    Used when transferring Payloads between Python and Rust.
    Byte arrays are Base64 encoded strings.
    """

    def __init__(self, data: str, metadata: dict[str, str]) -> None:
        self.data = data
        self.metadata = metadata

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "data": self.data,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SerializedPayload:
        """Create from dictionary."""
        return cls(data=d["data"], metadata=d["metadata"])


def serialize_payload(payload: Payload) -> SerializedPayload:
    """Serialize Payload for transfer (Base64 encoding).

    This is used when sending Payloads from Python to Rust.
    The data and metadata byte arrays are Base64 encoded for JSON transfer.

    Args:
        payload: Payload to serialize.

    Returns:
        SerializedPayload with Base64-encoded data.
    """
    return SerializedPayload(
        data=base64.b64encode(payload.data).decode("ascii"),
        metadata={
            key: base64.b64encode(value).decode("ascii") for key, value in payload.metadata.items()
        },
    )


def deserialize_payload(serialized: SerializedPayload) -> Payload:
    """Deserialize Payload received from Rust (Base64 decoding).

    This is used when receiving Payloads from Rust.
    The Base64 encoded data and metadata are decoded back to byte arrays.

    Args:
        serialized: SerializedPayload with Base64-encoded data.

    Returns:
        Payload with byte arrays.
    """
    return Payload(
        data=base64.b64decode(serialized.data),
        metadata={key: base64.b64decode(value) for key, value in serialized.metadata.items()},
    )


def serialize_payloads(payloads: list[Payload]) -> list[SerializedPayload]:
    """Serialize multiple Payloads."""
    return [serialize_payload(p) for p in payloads]


def deserialize_payloads(serialized: list[SerializedPayload]) -> list[Payload]:
    """Deserialize multiple Payloads."""
    return [deserialize_payload(s) for s in serialized]


# Global converter instance (lazy-loaded)
_default_converter: DataConverter | None = None


def _get_default_converter() -> DataConverter:
    """Get the default data converter (lazy initialization)."""
    global _default_converter
    if _default_converter is None:
        from orcher.converter.json import JsonDataConverter

        _default_converter = JsonDataConverter()
    return _default_converter


def to_payload(value: Any, converter: DataConverter | None = None) -> Payload:
    """Convert a Python value to a Payload.

    Args:
        value: Python value to convert.
        converter: Optional custom converter. Uses default JSON converter if not provided.

    Returns:
        Payload with serialized data.

    Example:
        >>> payload = to_payload({"name": "Alice"})
        >>> payload.encoding
        'json/plain'
    """
    if converter is None:
        converter = _get_default_converter()
    return converter.to_payload(value)


def from_payload(
    payload: Payload,
    type_hint: type[Any] | None = None,
    converter: DataConverter | None = None,
) -> Any:
    """Convert a Payload to a Python value.

    Args:
        payload: Payload to convert.
        type_hint: Optional type hint for deserialization.
        converter: Optional custom converter. Uses default JSON converter if not provided.

    Returns:
        Python value from the payload.

    Example:
        >>> payload = Payload.from_json({"name": "Alice"})
        >>> data = from_payload(payload)
        >>> data
        {'name': 'Alice'}
    """
    if converter is None:
        converter = _get_default_converter()
    return converter.from_payload(payload, type_hint)


def to_payloads(
    values: list[Any],
    converter: DataConverter | None = None,
) -> list[Payload]:
    """Convert multiple Python values to Payloads.

    Args:
        values: List of Python values to convert.
        converter: Optional custom converter.

    Returns:
        List of Payloads.
    """
    if converter is None:
        converter = _get_default_converter()
    return converter.to_payloads(values)


def from_payloads(
    payloads: list[Payload],
    type_hints: list[type[Any] | None] | None = None,
    converter: DataConverter | None = None,
) -> list[Any]:
    """Convert multiple Payloads to Python values.

    Args:
        payloads: List of Payloads to convert.
        type_hints: Optional list of type hints.
        converter: Optional custom converter.

    Returns:
        List of Python values.
    """
    if converter is None:
        converter = _get_default_converter()
    return converter.from_payloads(payloads, type_hints)
