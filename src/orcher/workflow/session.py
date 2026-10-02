"""Worker session management for guaranteed task-to-worker pinning.

Sessions ensure that a series of related tasks execute on the same worker,
which is critical for:
- **GPU/ML workloads**: Model loaded in worker memory; subsequent inference tasks reuse it
- **File processing**: Download → transform → upload must run where the file exists
- **Connection-heavy tasks**: Worker holds DB connections or session state

Example::

    session = await ctx.create_session(SessionOptions(
        creation_timeout=timedelta(seconds=30),
        execution_timeout=timedelta(seconds=600),
    ))

    model = await session.execute_task(load_model, name="bert-base")
    result = await session.execute_task(run_inference, data=input_data)

    session.complete()
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcher.workflow.context import WorkflowContext

logger = logging.getLogger(__name__)

# Internal task type names — must match Rust SDK constants exactly.
SESSION_CREATE_TASK = "__orcher_create_session"
SESSION_COMPLETE_TASK = "__orcher_complete_session"

# Queue naming separator — must match Rust SDK constant.
SESSION_QUEUE_SEPARATOR = "__session__"


@dataclass
class SessionOptions:
    """Configuration for creating a worker session."""

    creation_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    """Maximum time to wait for a worker to accept the session."""

    execution_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=600))
    """Total session lifetime."""

    max_concurrent_tasks: int = 1
    """Maximum concurrent tasks within the session."""

    heartbeat_interval: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    """Heartbeat interval for session liveness detection."""


class SessionState(str, Enum):
    """Session lifecycle state."""

    OPEN = "open"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass
class SessionInfo:
    """Metadata about an active session, returned by the session creation task."""

    session_id: str
    session_queue: str
    worker_identity: str
    state: SessionState


@dataclass
class CreateSessionInput:
    """Input payload for the ``__orcher_create_session`` internal task."""

    session_id: str
    creation_timeout_ms: int
    execution_timeout_ms: int
    max_concurrent_tasks: int
    heartbeat_interval_ms: int


@dataclass
class CompleteSessionInput:
    """Input payload for the ``__orcher_complete_session`` internal task."""

    session_id: str


class SessionContext:
    """A context wrapper that routes all tasks to a session's worker-specific queue.

    Created by :meth:`WorkflowContext.create_session`. All ``execute_task``
    calls through this context override ``task_queue`` to the session's
    exclusive queue, guaranteeing execution on the same worker.
    """

    def __init__(self, ctx: WorkflowContext, info: SessionInfo) -> None:
        self._ctx = ctx
        self._info = info

    async def execute_task(
        self,
        task_fn: Callable[..., Any],
        **kwargs: Any,
    ) -> Any:
        """Execute a registered task on the session's pinned worker.

        Behaves identically to :meth:`WorkflowContext.execute_task` except the
        task is routed to the session's worker-specific queue.
        """
        if self._info.state != SessionState.OPEN:
            raise RuntimeError(
                f"Cannot execute task on session '{self._info.session_id}': "
                f"state is {self._info.state.value}"
            )

        # A one-shot override: execute_task routes this task to the session queue.
        self._ctx._task_queue_override = self._info.session_queue
        try:
            return await self._ctx.execute_task(task_fn, **kwargs)
        finally:
            # Cleared even when execute_task suspends, so no later task inherits it.
            self._ctx._task_queue_override = None

    def complete(self) -> None:
        """Mark the session as completed and release the worker's session slot."""
        if self._info.state != SessionState.OPEN:
            logger.warning(
                "Attempted to complete session '%s' that is not open (state=%s)",
                self._info.session_id,
                self._info.state.value,
            )
            return

        self._info.state = SessionState.CLOSED

        input_data = CompleteSessionInput(session_id=self._info.session_id)
        input_bytes = json.dumps({"session_id": input_data.session_id}).encode("utf-8")

        # A step like any other: one number from the workflow's step counter.
        sequence = self._ctx._next_sequence()
        task_id = f"{SESSION_COMPLETE_TASK}_{sequence}"

        input_payload = {
            "data": list(input_bytes),
            "metadata": {"encoding": list(b"json")},
        }

        schedule_cmd = {
            "sequence": sequence,
            "task_id": task_id,
            "task_type": SESSION_COMPLETE_TASK,
            "task_queue": self._info.session_queue,
            "input": [input_payload],
            "timeout": {"secs": 30, "nanos": 0},
            "retry_policy": None,
            "headers": [],
        }

        self._ctx._commands.append({"ScheduleTask": schedule_cmd})

        logger.debug(
            "Session completion scheduled: session_id=%s, queue=%s",
            self._info.session_id,
            self._info.session_queue,
        )

    @property
    def info(self) -> SessionInfo:
        """Get the session metadata."""
        return self._info

    @property
    def is_open(self) -> bool:
        """Check if the session is still open."""
        return self._info.state == SessionState.OPEN

    @property
    def session_id(self) -> str:
        return self._info.session_id

    @property
    def worker_identity(self) -> str:
        return self._info.worker_identity

    @property
    def session_queue(self) -> str:
        return self._info.session_queue


def build_session_queue(original_queue: str, worker_resource_id: str) -> str:
    """Build a session queue name from the original queue and worker resource ID."""
    return f"{original_queue}{SESSION_QUEUE_SEPARATOR}{worker_resource_id}"


def is_session_queue(queue: str) -> bool:
    """Check if a queue name is a session queue."""
    return SESSION_QUEUE_SEPARATOR in queue


def original_queue_from_session(session_queue: str) -> str | None:
    """Extract the original queue name from a session queue name."""
    parts = session_queue.split(SESSION_QUEUE_SEPARATOR)
    return parts[0] if parts else None
