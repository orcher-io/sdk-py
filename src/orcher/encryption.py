"""Payload encryption and compression.

Give a client and its workers an :class:`Encryption` and every payload is
encrypted with AES-256-GCM just before it is sent to the engine and decrypted
just after it arrives: workflow inputs and results, task inputs and results,
step results, events, query and update arguments and answers, child workflows,
actor calls and actor state. The engine never holds a key. It stores the
ciphertext and returns it unchanged, so the values cannot be read in its
database or its dashboard. Workflow, task and actor code sees plain values,
as without encryption.

Not encrypted: workflow, task, actor, event, query and update names, ids,
task queues, labels, annotations, headers, and error messages and stack
traces. Keep secrets out of those.

A client and every worker that exchange payloads need the same key. A worker
with a different key, or none, cannot read what it is handed: the workflow or
task is not failed, it stays waiting until a worker with the right key takes
it, and the worker logs an error each time it tries.

Example:
    >>> import os
    >>> from orcher import ClientConfig, Encryption, Worker
    >>>
    >>> encryption = Encryption(key=bytes.fromhex(os.environ["PAYLOAD_KEY"]), key_id="2026-11")
    >>> config = ClientConfig(server_url="http://localhost:50051", encryption=encryption)
    >>> worker = (
    ...     Worker.builder()
    ...     .server_url("http://localhost:50051")
    ...     .task_queue("orders")
    ...     .encryption(encryption)
    ...     .build()
    ... )

Rotating a key: give the new key a new ``key_id`` and keep the old key under
its id in ``decryption_keys``. New payloads are written with the new key, and
payloads that name the old id are still read. Only payloads written with a
``key_id`` name their key, so set one from the start.
"""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, get_args

__all__ = [
    "Compression",
    "Encryption",
    "KEY_LENGTH",
]

#: The length of an encryption key, in bytes (AES-256).
KEY_LENGTH = 32

#: The compressions a client or worker can apply to payloads.
Compression = Literal["gzip"]


def _checked_key(what: str, key: object) -> bytes:
    """``key`` as bytes. The error says its type or length, never the key."""
    if not isinstance(key, bytes | bytearray | memoryview):
        raise TypeError(f"{what} must be bytes, got {type(key).__name__}")
    key = bytes(key)
    if len(key) != KEY_LENGTH:
        raise ValueError(
            f"{what} must be exactly {KEY_LENGTH} bytes (AES-256-GCM), got {len(key)}"
        )
    return key


def _checked_key_id(what: str, key_id: object) -> str:
    if not isinstance(key_id, str):
        raise TypeError(f"{what} must be a str, got {type(key_id).__name__}")
    if not key_id.strip():
        raise ValueError(f"{what} must not be blank")
    return key_id


@dataclass(frozen=True)
class Encryption:
    """AES-256-GCM encryption of every payload exchanged with the engine.

    Pass the same one to ``ClientConfig(encryption=...)`` and to the worker
    (``Worker.builder().encryption(...)`` or ``WorkerConfig(encryption=...)``).

    Attributes:
        key: The key payloads are encrypted with: exactly 32 bytes. Generate
            one with :meth:`generate_key` and keep it in a secret store.
        key_id: A name for the key, stored unencrypted beside every payload it
            encrypts. It is what lets a reader holding several keys pick the
            right one, so it is needed to rotate keys later. It must not be
            derived from the key.
        decryption_keys: Keys used only for reading, by their id: payloads
            that name one of these ids are decrypted with it, and nothing is
            encrypted with it. This keeps payloads readable after their key
            is rotated out.

    Raises:
        ValueError: A key that is not exactly 32 bytes, a blank id, or a
            decryption key under the same id as ``key``.
        TypeError: A key that is not bytes, or an id that is not a str.

    Neither ``repr`` nor ``str`` shows a key.
    """

    key: bytes = field(repr=False)
    key_id: str | None = None
    decryption_keys: Mapping[str, bytes] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", _checked_key("encryption key", self.key))
        if self.key_id is not None:
            _checked_key_id("key_id", self.key_id)
        if self.decryption_keys is not None:
            if not isinstance(self.decryption_keys, Mapping):
                raise TypeError(
                    "decryption_keys must be a mapping of key id to key, "
                    f"got {type(self.decryption_keys).__name__}"
                )
            checked: dict[str, bytes] = {}
            for key_id, key in self.decryption_keys.items():
                _checked_key_id("a decryption key's id", key_id)
                if key_id == self.key_id:
                    raise ValueError(
                        f"decryption_keys holds a key under {key_id!r}, the id of the "
                        "encryption key"
                    )
                checked[key_id] = _checked_key(f"decryption key {key_id!r}", key)
            # A copy that cannot be changed afterwards, so what was checked is
            # what is used.
            object.__setattr__(self, "decryption_keys", MappingProxyType(checked))

    @staticmethod
    def generate_key() -> bytes:
        """A new random 32-byte key, from the operating system's secure source."""
        return secrets.token_bytes(KEY_LENGTH)

    def __repr__(self) -> str:
        ids = sorted(self.decryption_keys) if self.decryption_keys else []
        return f"Encryption(key=<hidden>, key_id={self.key_id!r}, decryption_key_ids={ids!r})"


def check_compression(compression: object) -> None:
    """Raise ValueError unless ``compression`` is None or a known compression."""
    if compression is not None and compression not in get_args(Compression):
        known = ", ".join(repr(name) for name in get_args(Compression))
        raise ValueError(f"compression must be {known} or None, got {compression!r}")


def check_encryption(encryption: object) -> None:
    """Raise TypeError unless ``encryption`` is None or an :class:`Encryption`."""
    if encryption is not None and not isinstance(encryption, Encryption):
        # The type only: the value may be a raw key.
        raise TypeError(
            f"encryption must be an orcher.Encryption, got {type(encryption).__name__}"
        )


def native_payload_seal(
    native: Any, encryption: Encryption | None, compression: str | None
) -> Any | None:
    """What the native client and worker host are started with: ``native``'s
    ``PayloadSeal`` for these options, or None when neither is set.

    With both, payloads are compressed first and encrypted last.
    """
    check_encryption(encryption)
    check_compression(compression)
    if encryption is None and compression is None:
        return None
    if encryption is None:
        return native.PayloadSeal(compression=compression)
    return native.PayloadSeal(
        key=encryption.key,
        key_id=encryption.key_id,
        decryption_keys=dict(encryption.decryption_keys or {}),
        compression=compression,
    )
