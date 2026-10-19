"""A saga's failed compensation is never silent.

A compensation that raises does not stop the ones after it; once all have
run, the failure is raised as ``SagaCompensationError``. ``compensate()`` used
to log it and return as if the saga had been undone, and a failed step whose
compensating failed raised only the step's own error.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from orcher import Saga, SagaCompensationError, WorkflowError


def recorder(log: list[str]) -> tuple[Callable[..., Any], Callable[..., Any]]:
    def ok(tag: str) -> Callable[[], Awaitable[str]]:
        async def run() -> str:
            log.append(tag)
            return tag

        return run

    def fail(tag: str) -> Callable[[], Awaitable[str]]:
        async def run() -> str:
            log.append(tag)
            raise RuntimeError(f"{tag} failed")

        return run

    return ok, fail


async def test_a_failed_step_whose_compensating_failed_raises_both() -> None:
    log: list[str] = []
    ok, fail = recorder(log)
    saga = Saga()
    await saga.add_step(ok("do:a"), ok("undo:a"), name="undo_a")
    await saga.add_step(ok("do:b"), fail("undo:b"), name="undo_b")

    with pytest.raises(SagaCompensationError) as caught:
        await saga.add_step(fail("do:c"))

    assert log == ["do:a", "do:b", "do:c", "undo:b", "undo:a"]
    err = caught.value
    assert isinstance(err, WorkflowError)
    assert err.failed == ["undo_b"]
    assert err.reason == "undo:b failed"
    assert str(err.step_failure) == "do:c failed"
    assert err.__cause__ is err.step_failure


async def test_compensate_raises_once_every_compensation_has_run() -> None:
    log: list[str] = []
    ok, fail = recorder(log)
    saga = Saga()
    await saga.add_step(ok("do:a"), ok("undo:a"), name="undo_a")
    saga.add_compensation(fail("undo:b"), name="undo_b")

    with pytest.raises(SagaCompensationError) as caught:
        await saga.compensate()

    assert log == ["do:a", "undo:b", "undo:a"]
    assert caught.value.failed == ["undo_b"]
    assert caught.value.step_failure is None


async def test_a_clean_compensation_still_returns_its_count() -> None:
    log: list[str] = []
    ok, _ = recorder(log)
    saga = Saga()
    await saga.add_step(ok("do:a"), ok("undo:a"))
    await saga.add_step(ok("do:b"), ok("undo:b"))

    assert await saga.compensate() == 2
    assert log == ["do:a", "do:b", "undo:b", "undo:a"]
