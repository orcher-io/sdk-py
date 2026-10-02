"""Tests for the data conversion module."""

import base64
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from uuid import UUID

import pytest

from orcher.converter import (
    JsonDataConverter,
    Payload,
    PayloadEncoding,
    default_converter,
    deserialize_payload,
    deserialize_payloads,
    from_payload,
    from_payloads,
    serialize_payload,
    serialize_payloads,
    to_payload,
    to_payloads,
)

# =============================================================================
# Test Fixtures
# =============================================================================


class Color(Enum):
    """Test enum."""

    RED = "red"
    GREEN = "green"
    BLUE = "blue"


@dataclass
class Person:
    """Test dataclass."""

    name: str
    age: int
    email: str | None = None


@dataclass
class Order:
    """Test dataclass with nested types."""

    order_id: str
    items: list[str]
    total: Decimal
    created_at: datetime


# =============================================================================
# Payload Tests
# =============================================================================


class TestPayload:
    """Tests for Payload class."""

    def test_empty_payload(self) -> None:
        """Test creating an empty payload."""
        payload = Payload.empty()
        assert payload.is_empty()
        assert len(payload) == 0
        assert not payload  # __bool__

    def test_null_payload(self) -> None:
        """Test creating a null payload."""
        payload = Payload.null()
        assert payload.is_null()
        assert payload.encoding == "binary/null"
        assert not payload  # __bool__

    def test_from_json(self) -> None:
        """Test creating payload from JSON."""
        data = {"key": "value", "number": 42}
        payload = Payload.from_json(data)

        assert payload.is_json()
        assert payload.encoding == "json/plain"
        assert payload.content_type == "application/json"
        assert payload.to_json() == data

    def test_from_json_list(self) -> None:
        """Test creating payload from JSON list."""
        data = [1, 2, 3, "four"]
        payload = Payload.from_json(data)
        assert payload.to_json() == data

    def test_from_json_primitives(self) -> None:
        """Test creating payload from JSON primitives."""
        # String
        assert Payload.from_json("hello").to_json() == "hello"
        # Number
        assert Payload.from_json(42).to_json() == 42
        assert Payload.from_json(3.14).to_json() == 3.14
        # Boolean
        assert Payload.from_json(True).to_json() is True
        assert Payload.from_json(False).to_json() is False
        # Null
        assert Payload.from_json(None).to_json() is None

    def test_from_bytes(self) -> None:
        """Test creating payload from bytes."""
        data = b"binary data here"
        payload = Payload.from_bytes(data)

        assert not payload.is_json()
        assert payload.encoding == "binary/raw"
        assert payload.content_type == "application/octet-stream"
        assert payload.to_bytes() == data

    def test_from_bytes_with_content_type(self) -> None:
        """Test creating payload from bytes with custom content type."""
        payload = Payload.from_bytes(b"<html>", content_type="text/html")
        assert payload.content_type == "text/html"

    def test_from_string(self) -> None:
        """Test creating payload from string."""
        text = "Hello, World!"
        payload = Payload.from_string(text)

        assert payload.encoding == "utf-8"
        assert payload.content_type == "text/plain"
        assert payload.to_string() == text

    def test_payload_len(self) -> None:
        """Test payload length."""
        payload = Payload.from_string("hello")
        assert len(payload) == 5

    def test_payload_repr(self) -> None:
        """Test payload string representation."""
        payload = Payload.from_json({"key": "value"})
        repr_str = repr(payload)
        assert "json/plain" in repr_str
        assert "size=" in repr_str


class TestPayloadSerialization:
    """Tests for payload serialization/deserialization."""

    def test_serialize_payload(self) -> None:
        """Test serializing a payload for transfer."""
        payload = Payload.from_json({"key": "value"})
        serialized = serialize_payload(payload)

        # Data should be base64 encoded
        assert isinstance(serialized.data, str)
        decoded_data = base64.b64decode(serialized.data)
        assert decoded_data == payload.data

        # Metadata should be base64 encoded
        for key, value in serialized.metadata.items():
            assert isinstance(value, str)
            decoded = base64.b64decode(value)
            assert decoded == payload.metadata[key]

    def test_deserialize_payload(self) -> None:
        """Test deserializing a payload from transfer."""
        original = Payload.from_json({"key": "value"})
        serialized = serialize_payload(original)
        deserialized = deserialize_payload(serialized)

        assert deserialized.data == original.data
        assert deserialized.metadata == original.metadata
        assert deserialized.to_json() == original.to_json()

    def test_serialize_payloads(self) -> None:
        """Test serializing multiple payloads."""
        payloads = [
            Payload.from_json({"a": 1}),
            Payload.from_json({"b": 2}),
            Payload.from_string("hello"),
        ]
        serialized = serialize_payloads(payloads)
        assert len(serialized) == 3

    def test_deserialize_payloads(self) -> None:
        """Test deserializing multiple payloads."""
        originals = [
            Payload.from_json({"a": 1}),
            Payload.from_json({"b": 2}),
        ]
        serialized = serialize_payloads(originals)
        deserialized = deserialize_payloads(serialized)

        assert len(deserialized) == 2
        assert deserialized[0].to_json() == {"a": 1}
        assert deserialized[1].to_json() == {"b": 2}


# =============================================================================
# PayloadEncoding Tests
# =============================================================================


class TestPayloadEncoding:
    """Tests for PayloadEncoding enum."""

    def test_encoding_values(self) -> None:
        """Test encoding enum values."""
        assert PayloadEncoding.JSON.value == "json/plain"
        assert PayloadEncoding.BINARY.value == "binary/raw"
        assert PayloadEncoding.NULL.value == "binary/null"


# =============================================================================
# JsonDataConverter Tests
# =============================================================================


class TestJsonDataConverter:
    """Tests for JsonDataConverter."""

    @pytest.fixture
    def converter(self) -> JsonDataConverter:
        """Create a converter instance."""
        return JsonDataConverter()

    def test_encoding_property(self, converter: JsonDataConverter) -> None:
        """Test converter encoding property."""
        assert converter.encoding == PayloadEncoding.JSON

    # Primitive types
    def test_convert_none(self, converter: JsonDataConverter) -> None:
        """Test converting None."""
        payload = converter.to_payload(None)
        assert payload.is_null()
        assert converter.from_payload(payload) is None

    def test_convert_string(self, converter: JsonDataConverter) -> None:
        """Test converting string."""
        value = "hello world"
        payload = converter.to_payload(value)
        assert converter.from_payload(payload) == value

    def test_convert_int(self, converter: JsonDataConverter) -> None:
        """Test converting integer."""
        value = 42
        payload = converter.to_payload(value)
        assert converter.from_payload(payload) == value

    def test_convert_float(self, converter: JsonDataConverter) -> None:
        """Test converting float."""
        value = 3.14159
        payload = converter.to_payload(value)
        assert converter.from_payload(payload) == value

    def test_convert_bool(self, converter: JsonDataConverter) -> None:
        """Test converting boolean."""
        for value in [True, False]:
            payload = converter.to_payload(value)
            assert converter.from_payload(payload) == value

    # Collections
    def test_convert_list(self, converter: JsonDataConverter) -> None:
        """Test converting list."""
        value = [1, 2, 3, "four", None]
        payload = converter.to_payload(value)
        assert converter.from_payload(payload) == value

    def test_convert_dict(self, converter: JsonDataConverter) -> None:
        """Test converting dict."""
        value = {"key": "value", "nested": {"a": 1}}
        payload = converter.to_payload(value)
        assert converter.from_payload(payload) == value

    def test_convert_tuple(self, converter: JsonDataConverter) -> None:
        """Test converting tuple (becomes list in JSON)."""
        value = (1, 2, 3)
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert result == [1, 2, 3]  # Tuples become lists

    def test_convert_set(self, converter: JsonDataConverter) -> None:
        """Test converting set."""
        value = {1, 2, 3}
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert set(result) == value

    # Datetime types
    def test_convert_datetime(self, converter: JsonDataConverter) -> None:
        """Test converting datetime."""
        value = datetime(2024, 1, 15, 10, 30, 45)
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert result == value

    def test_convert_date(self, converter: JsonDataConverter) -> None:
        """Test converting date."""
        value = date(2024, 1, 15)
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert result == value

    def test_convert_time(self, converter: JsonDataConverter) -> None:
        """Test converting time."""
        value = time(10, 30, 45)
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert result == value

    def test_convert_timedelta(self, converter: JsonDataConverter) -> None:
        """Test converting timedelta."""
        value = timedelta(hours=2, minutes=30)
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert result == value

    # Other types
    def test_convert_uuid(self, converter: JsonDataConverter) -> None:
        """Test converting UUID."""
        value = UUID("12345678-1234-5678-1234-567812345678")
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert result == value

    def test_convert_decimal(self, converter: JsonDataConverter) -> None:
        """Test converting Decimal."""
        value = Decimal("123.456789")
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert result == value

    def test_convert_bytes(self, converter: JsonDataConverter) -> None:
        """Test converting bytes."""
        value = b"binary data"
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert result == value

    def test_convert_bytes_inside_dict(self, converter: JsonDataConverter) -> None:
        """Test converting bytes inside a dict."""
        value = {"data": b"binary"}
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        assert result["data"] == b"binary"

    # Enum
    def test_convert_enum(self, converter: JsonDataConverter) -> None:
        """Test converting enum."""
        converter.register_type(Color)
        value = Color.RED
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        # With type registered, we get the enum back
        assert result == Color.RED

    def test_convert_enum_with_type_hint(self, converter: JsonDataConverter) -> None:
        """Test converting enum with type hint."""
        converter.register_type(Color)
        value = Color.GREEN
        payload = converter.to_payload(value)
        result = converter.from_payload(payload, type_hint=Color)
        assert result == Color.GREEN

    # Dataclass
    def test_convert_dataclass(self, converter: JsonDataConverter) -> None:
        """Test converting dataclass."""
        converter.register_type(Person)
        value = Person(name="Alice", age=30, email="alice@example.com")
        payload = converter.to_payload(value)
        result = converter.from_payload(payload)
        # With type registered, we get the dataclass back
        assert isinstance(result, Person)
        assert result.name == "Alice"
        assert result.age == 30

    def test_convert_dataclass_with_type_hint(self, converter: JsonDataConverter) -> None:
        """Test converting dataclass with type hint."""
        value = Person(name="Bob", age=25)
        payload = converter.to_payload(value)
        result = converter.from_payload(payload, type_hint=Person)
        assert isinstance(result, Person)
        assert result.name == "Bob"
        assert result.age == 25
        assert result.email is None

    def test_convert_nested_dataclass(self, converter: JsonDataConverter) -> None:
        """Test converting dataclass with nested types."""
        value = Order(
            order_id="ORD-123",
            items=["item1", "item2"],
            total=Decimal("99.99"),
            created_at=datetime(2024, 1, 15, 10, 30),
        )
        payload = converter.to_payload(value)
        result = converter.from_payload(payload, type_hint=Order)
        assert isinstance(result, Order)
        assert result.order_id == "ORD-123"
        assert result.items == ["item1", "item2"]
        assert result.total == Decimal("99.99")
        assert result.created_at == datetime(2024, 1, 15, 10, 30)

    # Batch operations
    def test_to_payloads(self, converter: JsonDataConverter) -> None:
        """Test converting multiple values to payloads."""
        values = [1, "two", {"three": 3}]
        payloads = converter.to_payloads(values)
        assert len(payloads) == 3

    def test_from_payloads(self, converter: JsonDataConverter) -> None:
        """Test converting multiple payloads to values."""
        values = [1, "two", {"three": 3}]
        payloads = converter.to_payloads(values)
        results = converter.from_payloads(payloads)
        assert results == values

    def test_from_payloads_with_type_hints(self, converter: JsonDataConverter) -> None:
        """Test converting multiple payloads with type hints."""
        payloads = [
            converter.to_payload(42),
            converter.to_payload("hello"),
        ]
        results = converter.from_payloads(payloads, type_hints=[int, str])
        assert results == [42, "hello"]

    def test_from_payloads_mismatched_length(self, converter: JsonDataConverter) -> None:
        """Test that mismatched lengths raise error."""
        payloads = [converter.to_payload(1), converter.to_payload(2)]
        with pytest.raises(ValueError, match="must match"):
            converter.from_payloads(payloads, type_hints=[int])

    # Type coercion
    def test_coerce_to_list_type(self, converter: JsonDataConverter) -> None:
        """Test coercing to List type."""
        payload = converter.to_payload([1, 2, 3])
        result = converter.from_payload(payload, type_hint=list[int])
        assert result == [1, 2, 3]

    def test_coerce_to_dict_type(self, converter: JsonDataConverter) -> None:
        """Test coercing to Dict type."""
        payload = converter.to_payload({"a": 1, "b": 2})
        result = converter.from_payload(payload, type_hint=dict[str, int])
        assert result == {"a": 1, "b": 2}

    def test_coerce_to_optional(self, converter: JsonDataConverter) -> None:
        """Test coercing to Optional type."""
        # None value
        payload = converter.to_payload(None)
        result = converter.from_payload(payload, type_hint=str | None)
        assert result is None

        # Non-None value
        payload = converter.to_payload("hello")
        result = converter.from_payload(payload, type_hint=str | None)
        assert result == "hello"

    # Error cases
    def test_unsupported_type_raises_error(self, converter: JsonDataConverter) -> None:
        """Test that unsupported types raise TypeError."""

        class UnsupportedClass:
            pass

        with pytest.raises(TypeError, match="Cannot serialize"):
            converter.to_payload(UnsupportedClass())


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestHelperFunctions:
    """Tests for module-level helper functions."""

    def test_to_payload_function(self) -> None:
        """Test to_payload helper function."""
        payload = to_payload({"key": "value"})
        assert payload.is_json()
        assert payload.to_json() == {"key": "value"}

    def test_from_payload_function(self) -> None:
        """Test from_payload helper function."""
        payload = Payload.from_json({"key": "value"})
        result = from_payload(payload)
        assert result == {"key": "value"}

    def test_to_payloads_function(self) -> None:
        """Test to_payloads helper function."""
        payloads = to_payloads([1, 2, 3])
        assert len(payloads) == 3

    def test_from_payloads_function(self) -> None:
        """Test from_payloads helper function."""
        payloads = to_payloads([1, 2, 3])
        results = from_payloads(payloads)
        assert results == [1, 2, 3]

    def test_default_converter_instance(self) -> None:
        """Test that default_converter is a JsonDataConverter."""
        assert isinstance(default_converter, JsonDataConverter)

    def test_custom_converter(self) -> None:
        """Test using a custom converter."""
        custom = JsonDataConverter()
        custom.register_type(Color)

        payload = to_payload(Color.BLUE, converter=custom)
        result = from_payload(payload, type_hint=Color, converter=custom)
        assert result == Color.BLUE


# =============================================================================
# Integration Tests
# =============================================================================


class TestConverterIntegration:
    """Integration tests for data conversion."""

    def test_round_trip_complex_data(self) -> None:
        """Test round-trip conversion of complex nested data."""
        data = {
            "user": {
                "name": "Alice",
                "created_at": datetime(2024, 1, 15, 10, 30),
                "tags": ["admin", "user"],
            },
            "settings": {
                "theme": "dark",
                "notifications": True,
            },
            "balance": Decimal("1234.56"),
        }

        payload = to_payload(data)
        result = from_payload(payload)

        assert result["user"]["name"] == "Alice"
        assert result["user"]["created_at"] == datetime(2024, 1, 15, 10, 30)
        assert result["user"]["tags"] == ["admin", "user"]
        assert result["settings"]["theme"] == "dark"
        assert result["settings"]["notifications"] is True
        assert result["balance"] == Decimal("1234.56")

    def test_serialize_deserialize_round_trip(self) -> None:
        """Test full round-trip through serialization."""
        original_data = {"message": "hello", "count": 42}

        # Python -> Payload -> Serialized -> Payload -> Python
        payload = to_payload(original_data)
        serialized = serialize_payload(payload)
        deserialized_payload = deserialize_payload(serialized)
        result = from_payload(deserialized_payload)

        assert result == original_data

    def test_workflow_data_simulation(self) -> None:
        """Simulate workflow input/output conversion."""
        # Workflow input
        workflow_input = {
            "order_id": "ORD-12345",
            "customer_email": "customer@example.com",
            "items": [
                {"sku": "ITEM-001", "quantity": 2, "price": Decimal("29.99")},
                {"sku": "ITEM-002", "quantity": 1, "price": Decimal("49.99")},
            ],
            "submitted_at": datetime(2024, 1, 15, 14, 30, 0),
        }

        # The payload is the form sent to the server.
        input_payload = to_payload(workflow_input)
        assert input_payload.is_json()

        # Simulate receiving it back.
        received_input = from_payload(input_payload)
        assert received_input["order_id"] == "ORD-12345"
        assert len(received_input["items"]) == 2
        assert received_input["submitted_at"] == datetime(2024, 1, 15, 14, 30, 0)

        # Workflow output
        workflow_output = {
            "status": "completed",
            "processed_at": datetime(2024, 1, 15, 14, 31, 0),
            "confirmation_number": "CONF-98765",
        }

        output_payload = to_payload(workflow_output)
        result = from_payload(output_payload)
        assert result["status"] == "completed"
