"""Tests for ``RetryPolicy``, the single public retry policy type.

``RetryPolicy`` is the ``timedelta``-based durable policy in ``orcher.types``,
re-exported as ``orcher.RetryPolicy``. The ``orcher.errors`` package has no
retry policy or in-process retry helper of its own.

These tests guarantee:
  * there is one canonical type, and ``orcher.errors`` does not export one;
  * a ``RetryPolicy`` serializes to the dict the engine expects;
  * a ``@task(retry_policy={...})`` dict is read with the documented
    ``*_seconds`` keys, so interval units cannot silently drift.
"""

from __future__ import annotations

from datetime import timedelta

import orcher
from orcher import RetryPolicy
from orcher.types import RetryPolicy as TypesRetryPolicy
from orcher.workflow.context import _normalize_retry_policy


class TestOneCanonicalType:
    def test_top_level_is_types_timedelta_policy(self) -> None:
        assert RetryPolicy is TypesRetryPolicy
        policy = RetryPolicy.default()
        # Intervals are timedeltas, not millisecond integers.
        assert isinstance(policy.initial_interval, timedelta)
        assert isinstance(policy.max_interval, timedelta)
        assert policy.max_attempts == 3
        assert policy.backoff_coefficient == 2.0

    def test_errors_module_no_longer_exports_retrypolicy(self) -> None:
        import orcher.errors as errors

        assert not hasattr(errors, "RetryPolicy")
        assert not hasattr(errors, "BackoffStrategy")
        assert not hasattr(errors, "with_retry")
        assert "RetryPolicy" not in getattr(errors, "__all__", [])

    def test_no_in_process_retry_module(self) -> None:
        import importlib

        try:
            importlib.import_module("orcher.errors.retry")
        except ModuleNotFoundError:
            return
        raise AssertionError("orcher.errors.retry should have been removed")


class TestDurableNormalization:
    def test_retrypolicy_object_serializes_to_seconds(self) -> None:
        policy = RetryPolicy(
            max_attempts=5,
            initial_interval=timedelta(seconds=2),
            max_interval=timedelta(seconds=30),
            backoff_coefficient=3.0,
            non_retryable_error_types=["ValueError"],
        )
        out = _normalize_retry_policy(policy)
        assert out == {
            "max_attempts": 5,
            "initial_interval": {"secs": 2, "nanos": 0},
            "max_interval": {"secs": 30, "nanos": 0},
            "backoff_coefficient": 3.0,
            "non_retryable_errors": ["ValueError"],
        }

    def test_none_normalizes_to_none(self) -> None:
        assert _normalize_retry_policy(None) is None


class TestTaskDictUnitContract:
    """A @task(retry_policy={...}) dict is read with the keys the decorator documents.

    The keys carry the unit (``*_seconds``), so a mismatch would be a silent
    unit error rather than a failure."""

    def test_documented_seconds_keys_are_honored(self) -> None:
        rp = {
            "max_attempts": 4,
            "initial_interval_seconds": 5,
            "max_interval_seconds": 45,
            "backoff_coefficient": 1.5,
            "non_retryable_errors": ["KeyError"],
        }
        out = _normalize_retry_policy(rp)
        assert out == {
            "max_attempts": 4,
            "initial_interval": {"secs": 5, "nanos": 0},
            "max_interval": {"secs": 45, "nanos": 0},
            "backoff_coefficient": 1.5,
            "non_retryable_errors": ["KeyError"],
        }

    def test_retry_shorthand_dict_uses_defaults(self) -> None:
        # `@task(retry=N)` becomes {"max_attempts": N}; intervals fall back to defaults.
        out = _normalize_retry_policy({"max_attempts": 7})
        assert out["max_attempts"] == 7
        assert out["initial_interval"] == {"secs": 1, "nanos": 0}
        assert out["max_interval"] == {"secs": 60, "nanos": 0}
        assert out["backoff_coefficient"] == 2.0


class TestTaskDecoratorAcceptsRetryPolicy:
    def test_task_stores_retrypolicy_object(self) -> None:
        @orcher.task(name="charge-card-b4")
        async def charge(ctx, amount: int) -> int:  # noqa: ANN001
            return amount

        policy = RetryPolicy(max_attempts=6, initial_interval=timedelta(seconds=3))

        @orcher.task(name="charge-card-b4-policy", retry_policy=policy)
        async def charge_with_policy(ctx, amount: int) -> int:  # noqa: ANN001
            return amount

        stored = charge_with_policy.__orcher_task_retry_policy__
        assert stored is policy
        # The stored policy serializes for the engine unchanged.
        out = _normalize_retry_policy(stored)
        assert out["max_attempts"] == 6
        assert out["initial_interval"] == {"secs": 3, "nanos": 0}

    def test_task_retry_shorthand_sets_dict(self) -> None:
        @orcher.task(name="charge-card-b4-retry", retry=3)
        async def charge(ctx, amount: int) -> int:  # noqa: ANN001
            return amount

        assert charge.__orcher_task_retry_policy__ == {"max_attempts": 3}
