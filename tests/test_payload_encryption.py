"""Payloads are encrypted and compressed as the client and the worker are told.

The options are checked where they are given, reach the native connection of
both the client and the worker, and never show a key: not in a repr, not in
an error, not in the JSON the worker host is configured with.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

import orcher
from orcher import Client, ClientConfig, ConfigurationError, Encryption
from orcher.encryption import native_payload_seal
from orcher.worker.builder import WorkerBuilder
from orcher.worker.config import WorkerConfig

# Recognizable in any output they leak into.
KEY = b"KEYKEYKEY-primary-0123456789abcd"
OLD_KEY = b"OLDOLDOLD-retired-0123456789abcd"
URL = "http://localhost:50051"

assert len(KEY) == 32 and len(OLD_KEY) == 32


def shows_a_key(text: str) -> bool:
    return any(
        secret in text
        for key in (KEY, OLD_KEY)
        for secret in (key.decode(), key.hex(), repr(key))
    )


# --- the option itself -----------------------------------------------------


def test_a_key_of_32_bytes_is_taken() -> None:
    encryption = Encryption(key=KEY)
    assert encryption.key == KEY
    assert encryption.key_id is None
    assert encryption.decryption_keys is None


@pytest.mark.parametrize("length", [0, 16, 31, 33, 64])
def test_a_key_of_any_other_length_is_refused(length: int) -> None:
    with pytest.raises(ValueError, match=f"exactly 32 bytes.*got {length}"):
        Encryption(key=b"k" * length)


@pytest.mark.parametrize("key", ["k" * 32, None, 32, list(KEY)])
def test_a_key_that_is_not_bytes_is_refused(key: Any) -> None:
    with pytest.raises(TypeError, match="must be bytes"):
        Encryption(key=key)


def test_other_bytes_types_are_taken_as_bytes() -> None:
    for key in (bytearray(KEY), memoryview(KEY)):
        encryption = Encryption(key=key)  # type: ignore[arg-type]
        assert type(encryption.key) is bytes and encryption.key == KEY


def test_a_refused_key_is_not_shown_in_the_error() -> None:
    secret = b"SECRETSECRETSECRET"
    with pytest.raises(ValueError) as refused:
        Encryption(key=secret)
    assert "SECRET" not in str(refused.value)
    with pytest.raises(ValueError) as refused:
        Encryption(key=KEY, key_id="k2", decryption_keys={"k1": secret})
    assert "SECRET" not in str(refused.value)
    assert "k1" in str(refused.value)


def test_decryption_keys_are_checked_like_the_key() -> None:
    encryption = Encryption(key=KEY, key_id="k2", decryption_keys={"k1": OLD_KEY})
    assert dict(encryption.decryption_keys or {}) == {"k1": OLD_KEY}
    with pytest.raises(ValueError, match="exactly 32 bytes"):
        Encryption(key=KEY, key_id="k2", decryption_keys={"k1": b"short"})
    with pytest.raises(TypeError, match="must be bytes"):
        Encryption(key=KEY, key_id="k2", decryption_keys={"k1": "x" * 32})  # type: ignore[dict-item]
    with pytest.raises(TypeError, match="mapping"):
        Encryption(key=KEY, decryption_keys=[("k1", OLD_KEY)])  # type: ignore[arg-type]


def test_ids_must_say_something() -> None:
    for blank in ("", "  "):
        with pytest.raises(ValueError, match="blank"):
            Encryption(key=KEY, key_id=blank)
        with pytest.raises(ValueError, match="blank"):
            Encryption(key=KEY, key_id="k2", decryption_keys={blank: OLD_KEY})


def test_a_decryption_key_cannot_take_the_encryption_keys_id() -> None:
    # It would never be used: the encryption key answers for its own id.
    with pytest.raises(ValueError, match="id of the encryption key"):
        Encryption(key=KEY, key_id="k1", decryption_keys={"k1": OLD_KEY})


def test_the_keys_cannot_be_changed_after_they_were_checked() -> None:
    retired = {"k1": OLD_KEY}
    encryption = Encryption(key=KEY, key_id="k2", decryption_keys=retired)
    retired["k0"] = b"short"
    assert list(encryption.decryption_keys or {}) == ["k1"]
    with pytest.raises(dataclasses.FrozenInstanceError):
        encryption.key = OLD_KEY  # type: ignore[misc]
    with pytest.raises(TypeError):
        encryption.decryption_keys["k0"] = OLD_KEY  # type: ignore[index]


def test_a_generated_key_is_usable_and_new_each_time() -> None:
    first, second = Encryption.generate_key(), Encryption.generate_key()
    assert len(first) == 32 and first != second
    Encryption(key=first)


def test_encryption_is_exported() -> None:
    assert orcher.Encryption is Encryption
    assert "Encryption" in orcher.__all__


# --- nothing prints a key --------------------------------------------------


def test_no_repr_shows_a_key() -> None:
    encryption = Encryption(key=KEY, key_id="k2", decryption_keys={"k1": OLD_KEY})
    shown = [
        encryption,
        ClientConfig(server_url=URL, encryption=encryption, compression="gzip"),
        WorkerConfig(server_url=URL, task_queue="q", encryption=encryption),
        WorkerBuilder().server_url(URL).task_queue("q").encryption(encryption).build(),
        Client(ClientConfig(server_url=URL, encryption=encryption)),
    ]
    for value in shown:
        for text in (repr(value), str(value), f"{value}"):
            assert not shows_a_key(text), f"{type(value).__name__} printed a key: {text}"
    # The ids are not secret, and say which keys are held.
    assert "k2" in repr(encryption) and "k1" in repr(encryption)


def test_the_native_seal_does_not_show_or_return_a_key() -> None:
    native = pytest.importorskip("orcher._native")
    seal = native.PayloadSeal(
        key=KEY, key_id="k2", decryption_keys={"k1": OLD_KEY}, compression="gzip"
    )
    assert not shows_a_key(repr(seal)) and not shows_a_key(str(seal))
    assert seal.encrypted is True and seal.compression == "gzip"
    assert not hasattr(seal, "key") and not hasattr(seal, "decryption_keys")
    config = native.ClientConfig(server_url=URL, payload_seal=seal)
    assert not shows_a_key(repr(config))
    assert not hasattr(config, "payload_seal")


# --- compression -----------------------------------------------------------


@pytest.mark.parametrize("compression", ["zstd", "GZIP", "", True, 6])
def test_only_gzip_compresses(compression: Any) -> None:
    with pytest.raises(ConfigurationError, match="compression"):
        ClientConfig(server_url=URL, compression=compression)
    with pytest.raises(ValueError, match="compression"):
        WorkerConfig(server_url=URL, task_queue="q", compression=compression)
    with pytest.raises(ValueError, match="compression"):
        WorkerBuilder().compression(compression)


def test_a_raw_key_is_not_an_encryption_and_is_not_shown() -> None:
    # Passing the key itself is the likely mistake; the error must not echo it.
    for make in (
        lambda: ClientConfig(server_url=URL, encryption=KEY),  # type: ignore[arg-type]
        lambda: WorkerConfig(server_url=URL, task_queue="q", encryption=KEY),  # type: ignore[arg-type]
        lambda: WorkerBuilder().encryption(KEY),  # type: ignore[arg-type]
    ):
        with pytest.raises((TypeError, ConfigurationError)) as refused:
            make()
        assert "orcher.Encryption" in str(refused.value)
        assert not shows_a_key(str(refused.value))


# --- the options reach the native layer -------------------------------------


class RecordingNative:
    """Stands in for ``orcher._native``: records what the client and the
    worker host are started with."""

    def __init__(self) -> None:
        self.seals: list[dict[str, Any]] = []
        self.client_configs: list[dict[str, Any]] = []
        self.host_starts: list[tuple[str, str, Any]] = []
        outer = self

        class StopError(Exception):
            pass

        class ClientConnection:
            @staticmethod
            async def connect(config: Any) -> Any:
                return SimpleNamespace()

        class WorkerHost:
            @staticmethod
            async def start(config_json: str, registrations_json: str, payload_seal: Any) -> None:
                outer.host_starts.append((config_json, registrations_json, payload_seal))
                raise StopError

        self.StopError = StopError
        self.ClientConnection = ClientConnection
        self.WorkerHost = WorkerHost

    def PayloadSeal(self, **options: Any) -> dict[str, Any]:  # noqa: N802
        self.seals.append(options)
        return options

    def ClientConfig(self, **options: Any) -> dict[str, Any]:  # noqa: N802
        self.client_configs.append(options)
        return options


def connected_client_seal(config: ClientConfig) -> Any:
    """The payload seal ``config``'s client connects with."""
    native = RecordingNative()
    with mock.patch.object(orcher, "_native", native, create=True):
        asyncio.run(Client(config).connect())
    (native_config,) = native.client_configs
    return native_config["payload_seal"]


def started_worker(worker: Any) -> tuple[dict[str, Any], Any, str]:
    """The JSON configuration and the payload seal ``worker`` starts its host
    with, and that JSON as text."""
    from orcher.worker import worker as worker_module

    native = RecordingNative()
    with (
        mock.patch.object(worker_module, "is_native_available", return_value=True),
        mock.patch.object(worker_module, "get_native_module", return_value=native),
        pytest.raises(native.StopError),
    ):
        asyncio.run(worker._initialize())
    ((config_json, registrations_json, seal),) = native.host_starts
    return json.loads(config_json), seal, config_json + registrations_json


def test_without_options_there_is_no_seal() -> None:
    assert connected_client_seal(ClientConfig(server_url=URL)) is None
    from orcher.worker.worker import Worker

    _, seal, _ = started_worker(Worker(WorkerConfig(server_url=URL, task_queue="q")))
    assert seal is None


def test_the_client_connects_with_its_encryption_and_compression() -> None:
    encryption = Encryption(key=KEY, key_id="k2", decryption_keys={"k1": OLD_KEY})
    seal = connected_client_seal(
        ClientConfig(server_url=URL, encryption=encryption, compression="gzip")
    )
    assert seal == {
        "key": KEY,
        "key_id": "k2",
        "decryption_keys": {"k1": OLD_KEY},
        "compression": "gzip",
    }


def test_the_client_can_compress_without_encrypting() -> None:
    assert connected_client_seal(ClientConfig(server_url=URL, compression="gzip")) == {
        "compression": "gzip"
    }


def test_the_worker_host_starts_with_the_same_options_as_the_client() -> None:
    from orcher.worker.worker import Worker

    encryption = Encryption(key=KEY, key_id="k2", decryption_keys={"k1": OLD_KEY})
    client_seal = connected_client_seal(
        ClientConfig(server_url=URL, encryption=encryption, compression="gzip")
    )
    config = WorkerConfig(
        server_url=URL, task_queue="q", encryption=encryption, compression="gzip"
    )
    _, worker_seal, _ = started_worker(Worker(config))
    assert worker_seal == client_seal


def test_the_builder_carries_both_options_to_the_host() -> None:
    encryption = Encryption(key=KEY)
    worker = (
        WorkerBuilder()
        .server_url(URL)
        .task_queue("q")
        .encryption(encryption)
        .compression("gzip")
        .build()
    )
    assert worker.config.encryption is encryption
    assert worker.config.compression == "gzip"
    _, seal, _ = started_worker(worker)
    assert seal == {"key": KEY, "key_id": None, "decryption_keys": {}, "compression": "gzip"}


def test_the_builder_can_turn_them_off_again() -> None:
    builder = WorkerBuilder().encryption(Encryption(key=KEY)).compression("gzip")
    worker = builder.encryption(None).compression(None).server_url(URL).task_queue("q").build()
    assert worker.config.encryption is None and worker.config.compression is None


def test_no_key_is_written_into_the_hosts_json() -> None:
    from orcher.worker.worker import Worker

    encryption = Encryption(key=KEY, key_id="k2", decryption_keys={"k1": OLD_KEY})
    config = WorkerConfig(server_url=URL, task_queue="q", encryption=encryption)
    host_config, _, text = started_worker(Worker(config))
    assert not shows_a_key(text)
    assert "encryption" not in host_config and "payload_seal" not in host_config


# --- the native layer takes what Python passes ------------------------------


def test_the_native_calls_take_a_payload_seal() -> None:
    native = pytest.importorskip("orcher._native")
    seal = native_payload_seal(native, Encryption(key=KEY), "gzip")
    assert type(seal) is native.PayloadSeal
    inspect.signature(native.WorkerHost.start).bind("{}", "{}", seal)
    native.ClientConfig(server_url=URL, payload_seal=seal)
    assert native_payload_seal(native, None, None) is None


def test_the_native_seal_refuses_what_it_cannot_use() -> None:
    native = pytest.importorskip("orcher._native")
    for options in (
        {"key": b"short"},
        {"key": KEY, "key_id": " "},
        {"key": KEY, "key_id": "k2", "decryption_keys": {"k1": b"short"}},
        {"key": KEY, "key_id": "k1", "decryption_keys": {"k1": OLD_KEY}},
        {"decryption_keys": {"k1": OLD_KEY}},
        {"key_id": "k1"},
        {"compression": "zstd"},
    ):
        with pytest.raises(ValueError) as refused:
            native.PayloadSeal(**options)
        assert not shows_a_key(str(refused.value)), options


def test_the_old_codec_classes_are_gone() -> None:
    # They encoded a Payload by hand into a form nothing sent and the engine
    # connection does not read; `encryption=` and `compression=` replace them.
    native = pytest.importorskip("orcher._native")
    for name in ("GzipCodec", "EncryptionCodec", "CodecChain"):
        assert not hasattr(native, name)
    with pytest.raises(ImportError):
        __import__("orcher.codec")
