"""Task information dataclass.

This module provides the TaskInfo dataclass containing metadata
about a task execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

__all__ = ["TaskInfo"]


@dataclass(frozen=True)
class TaskInfo:
    """Information about the current task execution.

    Attributes:
        task_id: Unique identifier for this task execution.
        task_type: The type/name of the task.
        workflow_id: ID of the workflow that scheduled this task.
        run_id: Run ID of the workflow execution.
        task_queue: The task queue this task runs on.
        namespace: The namespace of the task.
        attempt: Current attempt number (1-based).
        scheduled_at: When the task was scheduled.
        started_at: When this attempt started.
        heartbeat_timeout: Maximum time between heartbeats, or None if unset.
        start_to_close_timeout: Maximum time this attempt may run, or None.
    """

    task_id: str
    task_type: str
    workflow_id: str
    run_id: str
    task_queue: str
    namespace: str
    attempt: int
    scheduled_at: datetime
    started_at: datetime
    # The limits this task runs under, as the engine recorded them. ``None``
    # means the engine set no limit. A task uses the heartbeat timeout to pace
    # its own heartbeats.
    heartbeat_timeout: timedelta | None = None
    start_to_close_timeout: timedelta | None = None
