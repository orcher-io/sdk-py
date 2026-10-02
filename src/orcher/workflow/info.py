"""Workflow information dataclass.

This module provides the WorkflowInfo dataclass containing metadata
about a workflow execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

__all__ = ["WorkflowInfo"]


@dataclass(frozen=True)
class WorkflowInfo:
    """Information about the current workflow execution.

    Attributes:
        workflow_id: Unique identifier for this workflow.
        run_id: Unique identifier for this execution run.
        workflow_type: The type/name of the workflow.
        task_queue: The task queue this workflow runs on.
        namespace: The namespace of the workflow.
        attempt: Current attempt number (1-based).
        started_at: When this execution started.
        version: Version label of the workflow (metadata only).
    """

    workflow_id: str
    run_id: str
    workflow_type: str
    task_queue: str
    namespace: str
    attempt: int
    started_at: datetime
    version: str = "1.0.0"
