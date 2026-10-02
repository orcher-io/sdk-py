"""What a failed task tells the engine about its error.

The engine decides a retry from the failure type it is told, and a retry
policy lists types by class name. The reported type must therefore be the
exception's own class name. A generic type such as ``TaskExecutionError`` for
every failure would mean a policy's non-retryable list never matches and nothing
could stop a retry.
"""

from typing import Any

import pytest

from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker


class CardDeclined(Exception):  # noqa: N818 - named as the other SDKs name it
    pass


class AccountClosed(Exception):  # noqa: N818 - named as the other SDKs name it
    non_retryable = True


class _Bridge:
    def __init__(self) -> None:
        self.failures: list[tuple[Any, ...]] = []

    async def fail_task(self, *args: Any) -> None:
        self.failures.append(args)


async def _report(error: Exception) -> tuple[Any, ...]:
    worker = Worker(WorkerConfig(server_url="http://localhost:50051", task_queue="q"))
    bridge = _Bridge()
    worker._bridge_worker = bridge

    async def raise_it(_request: dict[str, Any]) -> Any:
        raise error

    worker._execute_task = raise_it  # type: ignore[method-assign]
    await worker._handle_task({"task_id": "t", "task_token": "tok"}, "k")
    assert len(bridge.failures) == 1
    return bridge.failures[0]


@pytest.mark.asyncio
async def test_a_failure_is_reported_under_its_class_name() -> None:
    assert await _report(CardDeclined("declined")) == ("tok", "declined", "CardDeclined", False)


@pytest.mark.asyncio
async def test_a_non_retryable_attribute_marks_the_failure() -> None:
    assert await _report(AccountClosed("closed")) == ("tok", "closed", "AccountClosed", True)


@pytest.mark.asyncio
async def test_the_attribute_can_be_set_on_one_instance() -> None:
    error = ValueError("bad input")
    error.non_retryable = True  # type: ignore[attr-defined]
    assert await _report(error) == ("tok", "bad input", "ValueError", True)
