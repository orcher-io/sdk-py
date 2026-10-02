"""
Data Conversion Module for ORCHER Python SDK

This module provides data conversion utilities for serializing and deserializing
data between Python objects and ORCHER Payloads.

The Payload format is the primary data transfer mechanism between the Python SDK
and the ORCHER server (via Rust core).

## Payload Format

A Payload consists of:
- `data`: Serialized bytes (typically JSON)
- `metadata`: Key-value pairs describing encoding/compression

## Default Encoding

The default encoding is JSON with metadata:
```python
{
    "encoding": b"json/plain",
    "content-type": b"application/json"
}
```

## Custom Converters

A custom converter subclasses DataConverter and also implements its `encoding` property:

```python
from orcher.converter import DataConverter, Payload

class MyConverter(DataConverter):
    def to_payload(self, value: Any) -> Payload:
        # Custom serialization
        ...

    def from_payload(self, payload: Payload, type_hint: type | None = None) -> Any:
        # Custom deserialization
        ...
```

Example:
    >>> from orcher.converter import to_payload, from_payload
    >>>
    >>> # Convert Python object to Payload
    >>> payload = to_payload({"order_id": "123", "amount": 99.99})
    >>>
    >>> # Convert Payload back to Python object
    >>> data = from_payload(payload)
    >>> assert data == {"order_id": "123", "amount": 99.99}
"""

from orcher.converter.base import (
    DataConverter,
    PayloadEncoding,
)
from orcher.converter.json import (
    JsonDataConverter,
    default_converter,
)
from orcher.converter.payload import (
    METADATA_CONTENT_TYPE,
    METADATA_ENCODING,
    Payload,
    deserialize_payload,
    deserialize_payloads,
    from_payload,
    from_payloads,
    serialize_payload,
    serialize_payloads,
    to_payload,
    to_payloads,
)

__all__ = [
    # Base types
    "DataConverter",
    "PayloadEncoding",
    # Payload
    "Payload",
    "METADATA_ENCODING",
    "METADATA_CONTENT_TYPE",
    # Converter
    "JsonDataConverter",
    "default_converter",
    # Helper functions
    "to_payload",
    "from_payload",
    "to_payloads",
    "from_payloads",
    # Serialization for transfer
    "serialize_payload",
    "deserialize_payload",
    "serialize_payloads",
    "deserialize_payloads",
]
