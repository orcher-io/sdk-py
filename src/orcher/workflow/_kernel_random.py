"""The kernel's deterministic random source, version 1.

A run under the kernel's step-id scheme draws its random values from one
generator seeded from its history lineage, with algorithms the kernel fixes,
so every SDK draws the same values for the run. This is that algorithm
(sdk-core ``src/kernel/random.rs``), checked against the values the kernel
records for fixed lineages (``tests/fixtures/random_v1.json``):

- **Seed**: SHA-256 of ``orcher.kernel.v1.random``, a zero byte and the
  lineage's UTF-8 bytes; the 32 bytes are four little-endian u64 words of
  state (an all-zero state takes 1 as its first word).
- **Generator**: xoshiro256**.
- **Float** in [0, 1): ``(next >> 11) * 2**-53``.
- **Below n**, unbiased: ``threshold = (2**64 - n) % n``; draw until
  ``x >= threshold``; return ``x % n``.
- **Range** [low, high): ``low + below(high - low)``.
- **Bool**: the top bit of ``next``.
- **Shuffle** in place: for ``i`` from ``len - 1`` down to 1, swap ``i`` with
  ``below(i + 1)``.
- **Sample** ``k`` of ``n``: indices ``0..n``; for ``i`` in ``0..k``, swap ``i``
  with ``i + below(n - i)``; the first ``k``.
- **UUID** (version 4): the little-endian bytes of two ``next`` values; byte 6
  becomes ``(b6 & 0x0f) | 0x40``, byte 8 ``(b8 & 0x3f) | 0x80``.
"""

from __future__ import annotations

import hashlib
import uuid
from typing import TypeVar

T = TypeVar("T")

_MASK = (1 << 64) - 1
_SEED_LABEL = b"orcher.kernel.v1.random"


def seed_for_lineage(lineage: str) -> bytes:
    """The seed of the run whose history lineage is ``lineage``."""
    return hashlib.sha256(_SEED_LABEL + b"\x00" + lineage.encode("utf-8")).digest()


def _rotl(x: int, k: int) -> int:
    return ((x << k) | (x >> (64 - k))) & _MASK


class RunRandom:
    """A run's deterministic random source, from its 32-byte seed."""

    def __init__(self, seed: bytes) -> None:
        if len(seed) != 32:
            raise ValueError(f"a seed is 32 bytes, not {len(seed)}")
        state = [int.from_bytes(seed[i : i + 8], "little") for i in range(0, 32, 8)]
        if state == [0, 0, 0, 0]:
            state[0] = 1
        self._s = state

    @classmethod
    def for_lineage(cls, lineage: str) -> RunRandom:
        """The source of the run whose history lineage is ``lineage``."""
        return cls(seed_for_lineage(lineage))

    def next_u64(self) -> int:
        """The next 64 random bits."""
        s = self._s
        result = (_rotl((s[1] * 5) & _MASK, 7) * 9) & _MASK
        t = (s[1] << 17) & _MASK
        s[2] ^= s[0]
        s[3] ^= s[1]
        s[1] ^= s[2]
        s[0] ^= s[3]
        s[2] ^= t
        s[3] = _rotl(s[3], 45)
        return result

    def next_f64(self) -> float:
        """A float in [0, 1)."""
        return (self.next_u64() >> 11) * (1.0 / (1 << 53))

    def below(self, n: int) -> int:
        """An integer in [0, n), unbiased."""
        if n <= 0:
            raise ValueError("below() needs a bound greater than 0")
        threshold = ((1 << 64) - n) % n
        while True:
            x = self.next_u64()
            if x >= threshold:
                return x % n

    def range(self, low: int, high: int) -> int:
        """An integer in [low, high)."""
        if low >= high:
            raise ValueError(f"range({low}, {high}) is empty")
        return low + self.below(high - low)

    def next_bool(self) -> bool:
        """A random bool."""
        return self.next_u64() >> 63 == 1

    def shuffle(self, items: list[T]) -> None:
        """Shuffle ``items`` in place."""
        for i in range(len(items) - 1, 0, -1):
            j = self.below(i + 1)
            items[i], items[j] = items[j], items[i]

    def sample(self, n: int, k: int) -> list[int]:
        """``k`` distinct indices of ``n``, in the order drawn."""
        if k > n:
            raise ValueError(f"cannot sample {k} of {n}")
        indices = list(range(n))
        for i in range(k):
            j = i + self.below(n - i)
            indices[i], indices[j] = indices[j], indices[i]
        return indices[:k]

    def uuid(self) -> str:
        """A version 4 UUID, hyphenated lowercase."""
        first = self.next_u64().to_bytes(8, "little")
        raw = bytearray(first + self.next_u64().to_bytes(8, "little"))
        raw[6] = (raw[6] & 0x0F) | 0x40
        raw[8] = (raw[8] & 0x3F) | 0x80
        return str(uuid.UUID(bytes=bytes(raw)))
