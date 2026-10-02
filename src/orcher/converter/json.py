"""
JSON Data Converter for ORCHER Python SDK

This module provides the default JSON-based data converter.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Union, get_args, get_origin
from uuid import UUID

from orcher.converter.base import DataConverter, PayloadEncoding
from orcher.converter.payload import (
    METADATA_CONTENT_TYPE,
    METADATA_ENCODING,
    Payload,
)

__all__ = [
    "JsonDataConverter",
    "default_converter",
]


class JsonEncoder(json.JSONEncoder):
    """Extended JSON encoder with support for common Python types.

    Handles:
    - datetime, date, time objects (ISO 8601 format)
    - timedelta (total seconds)
    - UUID (string representation)
    - Decimal (string to preserve precision)
    - Enum (value)
    - dataclasses (dict representation)
    - bytes (base64 encoded)
    - sets (list)

    Each is written as a tagged object (``{"__type__": ..., "value": ...}``)
    so that JsonDecoder can restore the original type.
    """

    def default(self, o: Any) -> Any:  # noqa: N802 - match base class parameter name
        obj = o
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        if isinstance(obj, date):
            return {"__type__": "date", "value": obj.isoformat()}
        if isinstance(obj, time):
            return {"__type__": "time", "value": obj.isoformat()}
        if isinstance(obj, timedelta):
            return {"__type__": "timedelta", "value": obj.total_seconds()}

        if isinstance(obj, UUID):
            return {"__type__": "uuid", "value": str(obj)}

        # A string keeps the Decimal's exact precision.
        if isinstance(obj, Decimal):
            return {"__type__": "decimal", "value": str(obj)}

        if isinstance(obj, Enum):
            return {"__type__": "enum", "class": type(obj).__name__, "value": obj.value}

        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return {
                "__type__": "dataclass",
                "class": type(obj).__qualname__,
                "fields": dataclasses.asdict(obj),
            }

        if isinstance(obj, bytes):
            import base64

            return {"__type__": "bytes", "value": base64.b64encode(obj).decode("ascii")}

        if isinstance(obj, (set, frozenset)):
            return {"__type__": "set", "value": list(obj)}

        # Let the base class handle or raise TypeError
        return super().default(obj)


class JsonDecoder:
    """JSON decoder with support for typed objects.

    Decodes objects encoded by JsonEncoder back to their original types.
    """

    def __init__(self, type_registry: dict[str, type] | None = None) -> None:
        """Initialize decoder.

        Args:
            type_registry: Optional mapping of class names to types for
                dataclass/enum reconstruction.
        """
        self.type_registry = type_registry or {}

    def decode_object(self, obj: Any) -> Any:
        """Decode a JSON object, handling typed wrappers.

        Args:
            obj: JSON-decoded object.

        Returns:
            Decoded Python object.
        """
        if not isinstance(obj, dict):
            return obj

        type_marker = obj.get("__type__")
        if type_marker is None:
            # Regular dict, recursively decode values
            return {k: self.decode_object(v) for k, v in obj.items()}

        if type_marker == "datetime":
            return datetime.fromisoformat(obj["value"])
        if type_marker == "date":
            return date.fromisoformat(obj["value"])
        if type_marker == "time":
            return time.fromisoformat(obj["value"])
        if type_marker == "timedelta":
            return timedelta(seconds=obj["value"])
        if type_marker == "uuid":
            return UUID(obj["value"])
        if type_marker == "decimal":
            return Decimal(obj["value"])
        if type_marker == "bytes":
            import base64

            return base64.b64decode(obj["value"])
        if type_marker == "set":
            return {self.decode_object(item) for item in obj["value"]}
        if type_marker == "enum":
            class_name = obj["class"]
            if class_name in self.type_registry:
                enum_class = self.type_registry[class_name]
                return enum_class(obj["value"])
            # Return raw value if enum class not registered
            return obj["value"]
        if type_marker == "dataclass":
            class_name = obj["class"]
            if class_name in self.type_registry:
                dc_class = self.type_registry[class_name]
                fields = {k: self.decode_object(v) for k, v in obj["fields"].items()}
                return dc_class(**fields)
            # Return dict if dataclass not registered
            return {k: self.decode_object(v) for k, v in obj["fields"].items()}

        # Unknown type marker, return as-is
        return obj

    def decode(self, data: str) -> Any:
        """Decode a JSON string.

        Args:
            data: JSON string.

        Returns:
            Decoded Python object.
        """
        parsed = json.loads(data)
        return self._decode_recursive(parsed)

    def _decode_recursive(self, obj: Any) -> Any:
        """Recursively decode an object."""
        if isinstance(obj, dict):
            return self.decode_object(obj)
        if isinstance(obj, list):
            return [self._decode_recursive(item) for item in obj]
        return obj


class JsonDataConverter(DataConverter):
    """JSON-based data converter.

    This is the default converter that serializes Python objects to JSON.
    It supports:

    - Primitive types (int, float, str, bool, None)
    - Collections (list, dict, tuple, set)
    - datetime, date, time, timedelta
    - UUID, Decimal
    - Enum values
    - Dataclasses
    - bytes (base64 encoded)

    Dataclasses and enums decode back to their class when the class is
    registered with register_type() or given as the type hint. Otherwise a
    dataclass decodes as a dict and an enum as its raw value.

    Example:
        >>> converter = JsonDataConverter()
        >>>
        >>> # Simple types
        >>> payload = converter.to_payload({"key": "value"})
        >>> converter.from_payload(payload)
        {'key': 'value'}
        >>>
        >>> # Datetime
        >>> from datetime import datetime
        >>> payload = converter.to_payload(datetime.now())
        >>> converter.from_payload(payload)
        datetime(...)
    """

    def __init__(self) -> None:
        """Initialize the JSON converter."""
        self._type_registry: dict[str, type] = {}
        self._encoder = JsonEncoder
        self._decoder = JsonDecoder(self._type_registry)

    @property
    def encoding(self) -> PayloadEncoding:
        """Get the encoding used by this converter."""
        return PayloadEncoding.JSON

    def register_type(self, cls: type) -> None:
        """Register a type for deserialization.

        This allows the converter to reconstruct dataclasses and enums
        by name.

        Args:
            cls: Class to register.
        """
        self._type_registry[cls.__name__] = cls
        self._type_registry[cls.__qualname__] = cls
        self._decoder.type_registry = self._type_registry

    def to_payload(self, value: Any) -> Payload:
        """Convert a Python value to a Payload.

        Args:
            value: Python value to convert.

        Returns:
            Payload with JSON-encoded data.
        """
        if value is None:
            return Payload.null()

        # Bytes skip JSON and travel as a raw binary payload.
        if isinstance(value, (bytes, bytearray)):
            return Payload.from_bytes(bytes(value))

        try:
            data = json.dumps(value, cls=self._encoder, separators=(",", ":"))
        except TypeError as e:
            raise TypeError(f"Cannot serialize value of type {type(value).__name__}: {e}") from e

        return Payload(
            data=data.encode("utf-8"),
            metadata={
                METADATA_ENCODING: b"json/plain",
                METADATA_CONTENT_TYPE: b"application/json",
            },
        )

    def from_payload(
        self,
        payload: Payload,
        type_hint: Any = None,
    ) -> Any:
        """Convert a Payload to a Python value.

        Args:
            payload: Payload to convert.
            type_hint: Optional type hint for deserialization.

        Returns:
            Python value.
        """
        if payload.is_null():
            return None

        if payload.is_empty():
            return None

        encoding = payload.encoding
        if encoding == "binary/raw":
            return payload.data

        data = payload.data.decode("utf-8")
        value = self._decoder.decode(data)

        if type_hint is not None:
            value = self._coerce_to_type(value, type_hint)

        return value

    def _coerce_to_type(self, value: Any, type_hint: Any) -> Any:
        """Attempt to coerce a value to the specified type.

        Args:
            value: Value to coerce.
            type_hint: Target type.

        Returns:
            Coerced value.
        """
        if value is None:
            return None

        # For a union with None, use the first other member that accepts the value.
        origin = get_origin(type_hint)
        if origin is Union:
            args = get_args(type_hint)
            if type(None) in args:
                if value is None:
                    return None
                for arg in args:
                    if arg is not type(None):
                        try:
                            return self._coerce_to_type(value, arg)
                        except (TypeError, ValueError):
                            continue

        if origin is list:
            if isinstance(value, list):
                args = get_args(type_hint)
                if args:
                    item_type = args[0]
                    return [self._coerce_to_type(item, item_type) for item in value]
            return value

        if origin is dict:
            if isinstance(value, dict):
                args = get_args(type_hint)
                if len(args) == 2:
                    key_type, val_type = args
                    return {
                        self._coerce_to_type(k, key_type): self._coerce_to_type(v, val_type)
                        for k, v in value.items()
                    }
            return value

        if dataclasses.is_dataclass(type_hint) and isinstance(value, dict):
            if isinstance(type_hint, type):
                # Registering it lets later payloads tagged with this class decode to it.
                self.register_type(type_hint)
                fields = {f.name: f.type for f in dataclasses.fields(type_hint)}
                coerced = {}
                for name, val in value.items():
                    if name in fields:
                        coerced[name] = self._coerce_to_type(val, fields[name])
                    else:
                        coerced[name] = val
                return type_hint(**coerced)
            return value

        if isinstance(type_hint, type) and issubclass(type_hint, Enum):
            self.register_type(type_hint)
            if isinstance(value, str):
                # Try by name first
                try:
                    return type_hint[value]
                except KeyError:
                    pass
            # Try by value
            return type_hint(value)

        if isinstance(type_hint, type) and isinstance(value, type_hint):
            return value

        # Primitive targets convert directly, e.g. int("3").
        if type_hint in (int, float, str, bool):
            return type_hint(value)

        return value


# Shared default instance.
default_converter = JsonDataConverter()
