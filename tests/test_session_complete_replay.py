"""``SessionContext.complete()`` is a journaled step: once the journal holds
its task's result, a replay does not schedule the completion again."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from orcher.errors import WorkflowSuspendedError
from orcher.workflow.context import WorkflowContext, WorkflowInfo


def context(cached: dict[str, object]) -> WorkflowContext:
    info = WorkflowInfo(
        workflow_id="wf",
        run_id="run",
        workflow_type="t",
        task_queue="q",
        namespace="ns",
        attempt=1,
        started_at=datetime.now(UTC),
    )
    ctx = WorkflowContext(info=info, replaying=False)
    ctx._cached_results.update(cached)
    return ctx


def scheduled(ctx: WorkflowContext) -> list[str]:
    return [c["ScheduleTask"]["task_id"] for c in ctx._commands if "ScheduleTask" in c]


async def test_complete_is_not_scheduled_again_once_journaled() -> None:
    # Activation 1 schedules the session's creation.
    first = context({})
    with pytest.raises(WorkflowSuspendedError):
        await first.create_session()
    (create_id,) = scheduled(first)
    opened = {f"task:{create_id}": {"session_id": "s", "session_queue": "sq"}}

    # Activation 2: the session is open, and complete() schedules its task.
    second = context(opened)
    (await second.create_session()).complete()
    (complete_id,) = scheduled(second)
    assert complete_id != create_id

    # Activation 3: the completion's result is journaled, so it is not
    # scheduled again.
    third = context({**opened, f"task:{complete_id}": None})
    session = await third.create_session()
    session.complete()
    assert scheduled(third) == []
    assert not session.is_open
