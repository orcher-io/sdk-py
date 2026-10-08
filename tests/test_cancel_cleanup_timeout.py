"""`WorkflowHandle.cancel(cleanup_timeout=...)` reaches the native module.

The limit crosses to the native handle in milliseconds; a plain `cancel()`
sends none, so the engine sets no limit. The native signature is checked too,
so the keyword the handle passes is one the module accepts.
"""

from __future__ import annotations

import inspect
from datetime import timedelta
from typing import Any

import pytest

from orcher.client.workflow_handle import WorkflowHandle


class FakeNativeHandle:
    workflow_id = "order-123"
    run_id = "run-1"

    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def cancel(self, cleanup_timeout_ms: int | None = None) -> None:
        self.calls.append(cleanup_timeout_ms)


def handle() -> tuple[WorkflowHandle[Any], FakeNativeHandle]:
    native = FakeNativeHandle()
    return WorkflowHandle._from_native(object(), native), native  # type: ignore[arg-type]


async def test_a_cancellation_sets_no_cleanup_limit_by_default() -> None:
    h, native = handle()
    await h.cancel()
    assert native.calls == [None]


async def test_a_cancellation_passes_its_cleanup_limit_in_milliseconds() -> None:
    h, native = handle()
    await h.cancel(cleanup_timeout=timedelta(seconds=90, milliseconds=500))
    assert native.calls == [90_500]


async def test_a_negative_cleanup_limit_is_rejected_before_the_engine() -> None:
    h, native = handle()
    with pytest.raises(ValueError, match="cleanup_timeout"):
        await h.cancel(cleanup_timeout=timedelta(seconds=-1))
    assert native.calls == []


def test_the_native_cancel_takes_the_cleanup_limit() -> None:
    native = pytest.importorskip("orcher._native")
    inspect.signature(native.WorkflowHandle.cancel).bind(object(), 90_500)
    inspect.signature(native.WorkflowHandle.cancel).bind(object(), cleanup_timeout_ms=None)
