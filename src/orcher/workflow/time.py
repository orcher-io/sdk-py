"""Workflow time, read from the journal.

This module provides the WorkflowTime class, the clock a workflow reads with
``ctx.time``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

__all__ = ["WorkflowTime"]


class WorkflowTime:
    """The workflow's clock, read from its journal.

    A workflow must make the same decisions when it is replayed, on any
    machine and however much later, so it cannot read the machine's clock.
    This clock starts at the moment the engine journaled the workflow's start,
    and moves forward only when the workflow receives something it waited
    for: a task's result, a fired timer, a child's outcome or an event. It
    then reads as the moment the engine journaled that, and never moves back.

    A closure run with ``ctx.execute()`` does not move it: the closure runs
    inside the activation and its result is journaled only afterwards, so a
    replay would read a later time after it than the run that executed it.

    Every value comes from the journal, so a replay reads the same time at
    each point in the code as the original run, and the workflow may branch on
    it. Code that reads the time inside one branch of an ``asyncio.gather``
    may see a sibling branch's result move it, depending on which branches had
    their results.

    WARNING: Do not use ``datetime.now()`` in workflows. Use ``ctx.time.now()``.
    """

    def __init__(self, started_at: datetime) -> None:
        """Create a clock for a workflow that started at ``started_at``."""
        self._started_at = started_at
        self._now = started_at

    def now(self) -> datetime:
        """The workflow's current time.

        That is when the engine journaled the latest thing the workflow has
        waited for and received, or its start before it has received anything.
        It does not change while the workflow code runs between two such
        points, and it is the same at this point in the code on every replay.
        """
        return self._now

    def started_at(self) -> datetime:
        """When the workflow started. Unlike :meth:`now`, it never moves."""
        return self._started_at

    def elapsed(self) -> timedelta:
        """Time from the workflow's start to its current time, both journal times."""
        return self._now - self._started_at

    def _advance_to_ms(self, at_ms: int) -> None:
        """Move the clock forward to ``at_ms`` (milliseconds since the epoch).

        A time earlier than the clock's is ignored, so results received out of
        journal order cannot move it back.
        """
        at = datetime.fromtimestamp(at_ms / 1000, tz=UTC)
        if self._now.tzinfo is None:
            # A clock started from a naive time keeps naive UTC times.
            at = at.replace(tzinfo=None)
        if at > self._now:
            self._now = at
