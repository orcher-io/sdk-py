"""Deterministic random number generator for workflows.

This module provides the WorkflowRandom class for deterministic random
operations during workflow execution and replay.
"""

from __future__ import annotations

import random as _random
from typing import TypeVar

__all__ = ["WorkflowRandom"]

T = TypeVar("T")


class WorkflowRandom:
    """Deterministic random number generator for workflows.

    This class provides random operations that are deterministic during
    replay. The seed is based on the workflow execution, ensuring the
    same sequence of random numbers on each replay.

    WARNING: Do not use Python's built-in `random` module in workflows.
    Always use `ctx.random` instead.
    """

    def __init__(self, seed: int) -> None:
        """Initialize with a deterministic seed."""
        self._rng = _random.Random(seed)

    def random(self) -> float:
        """Return a random float in [0.0, 1.0)."""
        return self._rng.random()

    def randint(self, a: int, b: int) -> int:
        """Return a random integer N such that a <= N <= b."""
        return self._rng.randint(a, b)

    def choice(self, seq: list[T]) -> T:
        """Return a random element from the non-empty sequence."""
        return self._rng.choice(seq)

    def shuffle(self, seq: list[T]) -> None:
        """Shuffle the sequence in place."""
        self._rng.shuffle(seq)

    def sample(self, population: list[T], k: int) -> list[T]:
        """Return k unique elements from the population."""
        return self._rng.sample(population, k)

    def uuid(self) -> str:
        """Generate a deterministic UUID.

        Returns:
            A UUID-formatted string, identical on every replay.
        """
        # 16 bytes from the seeded generator, formatted 8-4-4-4-12. No RFC 4122
        # version or variant bits are set.
        bytes_val = bytes(self._rng.randint(0, 255) for _ in range(16))
        hex_str = bytes_val.hex()
        return f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:]}"
