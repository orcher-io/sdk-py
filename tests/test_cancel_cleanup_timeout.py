"""`WorkflowHandle.cancel(cleanup_timeout=...)` reaches the engine.

The limit crosses in the cancel call; a plain `cancel()` sends none, so the
engine sets no limit. A negative limit is refused before anything is sent.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from orcher.client.workflow_handle import WorkflowHandle
from tests._fake_connection import FakeConnection, client_on, done


def handle() -> tuple[WorkflowHandle[Any], FakeConnection]:
    connection = FakeConnection(done())
    return WorkflowHandle(
        client_on(connection), workflow_id="order-123", run_id="run-1"
    ), connection


async def test_a_cancellation_sets_no_cleanup_limit_by_default() -> None:
    h, connection = handle()
    await h.cancel()
    assert not connection.calls[0].cancel.HasField("cleanup_timeout")


async def test_a_cancellation_passes_its_cleanup_limit() -> None:
    h, connection = handle()
    await h.cancel(cleanup_timeout=timedelta(seconds=90, milliseconds=500))
    sent = connection.calls[0].cancel.cleanup_timeout.ToTimedelta()
    assert sent == timedelta(seconds=90, milliseconds=500)


async def test_a_negative_cleanup_limit_is_rejected_before_the_engine() -> None:
    h, connection = handle()
    with pytest.raises(ValueError, match="cleanup_timeout"):
        await h.cancel(cleanup_timeout=timedelta(seconds=-1))
    assert connection.calls == []
