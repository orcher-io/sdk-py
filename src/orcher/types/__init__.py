"""Core types for ORCHER Python SDK."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

__all__ = [
    "WorkflowExecution",
    "WorkflowStatus",
    "Payload",
    "RetryPolicy",
    "ParentExecutionInfo",
    "WorkflowExecutionConfig",
    "WorkflowExecutionDescription",
    "WorkflowExecutionInfo",
    "WorkflowListPage",
    "WorkflowIdReusePolicy",
]


@dataclass(frozen=True)
class WorkflowExecution:
    """Represents a workflow execution (workflow_id + run_id).

    Attributes:
        workflow_id: Unique workflow identifier.
        run_id: Unique run identifier for this execution.
    """

    workflow_id: str
    run_id: str

    def __str__(self) -> str:
        return f"{self.workflow_id}:{self.run_id}"


class WorkflowIdReusePolicy(Enum):
    """Whether a start may use a workflow id an earlier run already carried.

    A workflow id names at most one open run: starting one whose run is still
    open raises ``WorkflowError`` (``WORKFLOW_ALREADY_EXISTS``), naming that run
    as its ``run_id``, under every policy but ``TERMINATE_IF_RUNNING``. The
    policies differ in what they allow once the previous run has closed.
    """

    ALLOW_DUPLICATE = "ALLOW_DUPLICATE"
    """Start once the previous run has closed, however it closed."""
    ALLOW_DUPLICATE_FAILED_ONLY = "ALLOW_DUPLICATE_FAILED_ONLY"
    """Start once the previous run has closed without completing: failed,
    cancelled, terminated or timed out."""
    REJECT_DUPLICATE = "REJECT_DUPLICATE"
    """Never start an id a run has carried before."""
    TERMINATE_IF_RUNNING = "TERMINATE_IF_RUNNING"
    """Terminate the open run, if there is one, and start."""


class WorkflowStatus(Enum):
    """Workflow execution status."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TERMINATED = "TERMINATED"
    TIMED_OUT = "TIMED_OUT"
    RESTARTED_FRESH = "RESTARTED_FRESH"

    @classmethod
    def _from_native(cls, native: object) -> WorkflowStatus:
        """Convert the native status object into this enum.

        The native layer returns its own `WorkflowStatus` pyclass, whose
        ``__eq__`` accepts only another instance of itself. Converting it here
        lets callers compare the result with ``WorkflowStatus.COMPLETED`` and
        the other members of this enum.

        The mapping is explicit rather than a case transform: the native names
        are PascalCase (``TimedOut``) and the members are SCREAMING_SNAKE
        (``TIMED_OUT``), so ``.upper()`` would produce ``TIMEDOUT`` and fail to
        match. An unknown value raises instead of guessing, so a caller never
        receives a wrong status.
        """
        name = getattr(native, "name", None)
        if name is None:
            raise TypeError(f"cannot convert {native!r} to WorkflowStatus")

        try:
            return cls.from_wire_name(name)
        except ValueError:
            raise ValueError(
                f"unknown workflow status {name!r} from the native layer; "
                "the native and Python status sets have diverged"
            ) from None

    @classmethod
    def from_wire_name(cls, name: str) -> WorkflowStatus:
        """Map a Rust variant name (``TimedOut``) to its member.

        Serde serializes Rust enum variants by name, and the native status
        object reports the same spelling, so both paths share this one mapping
        and cannot disagree about which statuses exist.
        """
        mapping = {
            "Running": cls.RUNNING,
            "Completed": cls.COMPLETED,
            "Failed": cls.FAILED,
            "Cancelled": cls.CANCELLED,
            "Terminated": cls.TERMINATED,
            "TimedOut": cls.TIMED_OUT,
            "RestartedFresh": cls.RESTARTED_FRESH,
        }
        try:
            return mapping[name]
        except KeyError:
            raise ValueError(f"unknown workflow status name {name!r}") from None

    def is_terminal(self) -> bool:
        """Check if workflow is in a terminal state."""
        return self in {
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
            WorkflowStatus.TERMINATED,
            WorkflowStatus.TIMED_OUT,
            WorkflowStatus.RESTARTED_FRESH,
        }

    def is_running(self) -> bool:
        """Check if workflow is still running."""
        return self == WorkflowStatus.RUNNING


@dataclass
class Payload:
    """Serialized data payload with metadata.

    Attributes:
        data: Serialized data as bytes.
        metadata: Metadata about the payload (encoding, content type, etc.).
    """

    data: bytes = field(default_factory=bytes)
    metadata: dict[str, bytes] = field(default_factory=dict)

    @classmethod
    def from_json(cls, value: Any) -> Payload:
        """Create a payload from JSON-serializable data."""
        import json

        data = json.dumps(value).encode("utf-8")
        return cls(
            data=data,
            metadata={
                "encoding": b"json",
                "content-type": b"application/json",
            },
        )

    def to_json(self) -> Any:
        """Deserialize payload as JSON."""
        import json

        return json.loads(self.data.decode("utf-8"))

    @classmethod
    def from_string(cls, value: str) -> Payload:
        """Create a payload from a string."""
        return cls(
            data=value.encode("utf-8"),
            metadata={
                "encoding": b"utf-8",
                "content-type": b"text/plain",
            },
        )

    def to_string(self) -> str:
        """Get payload as string (if UTF-8)."""
        return self.data.decode("utf-8")

    def is_empty(self) -> bool:
        """Check if payload is empty."""
        return len(self.data) == 0

    def __len__(self) -> int:
        """Get payload size in bytes."""
        return len(self.data)


@dataclass
class RetryPolicy:
    """Retry policy for workflows and tasks.

    Attributes:
        max_attempts: Maximum number of retry attempts (0 = unlimited).
        initial_interval: Initial retry interval.
        max_interval: Maximum retry interval.
        backoff_coefficient: Backoff multiplier for each retry.
        non_retryable_error_types: List of error types that should not be retried.
    """

    max_attempts: int = 3
    initial_interval: timedelta = field(default_factory=lambda: timedelta(seconds=1))
    max_interval: timedelta = field(default_factory=lambda: timedelta(seconds=60))
    backoff_coefficient: float = 2.0
    non_retryable_error_types: list[str] = field(default_factory=list)

    @classmethod
    def default(cls) -> RetryPolicy:
        """Create a default retry policy."""
        return cls()

    @classmethod
    def no_retry(cls) -> RetryPolicy:
        """Create a retry policy that does not retry."""
        return cls(max_attempts=1)


@dataclass(frozen=True)
class ParentExecutionInfo:
    """Information about a parent workflow execution."""

    namespace: str
    workflow_id: str
    execution_id: str


@dataclass(frozen=True)
class WorkflowExecutionConfig:
    """Workflow execution configuration."""

    task_queue: str
    execution_timeout_seconds: int | None = None
    run_timeout_seconds: int | None = None
    default_task_timeout_seconds: int | None = None
    retry_policy: RetryPolicy | None = None
    cron_schedule: str = ""


@dataclass(frozen=True)
class WorkflowExecutionDescription:
    """Detailed description of a workflow execution.

    Returned by ``WorkflowHandle.describe()``.
    """

    execution: WorkflowExecution
    workflow_type: str
    task_queue: str
    status: WorkflowStatus
    start_time: datetime | None
    close_time: datetime | None
    journal_length: int
    attempt: int
    labels: dict[str, str]
    memo: dict[str, Any]
    tags: list[str]
    started_by: str
    cron_schedule: str
    state_transition_count: int
    parent_execution: ParentExecutionInfo | None
    pending_tasks: int
    pending_timers: int
    pending_events: int
    execution_config: WorkflowExecutionConfig | None

    @classmethod
    def _from_dict(cls, d: dict[str, Any]) -> WorkflowExecutionDescription:
        """Create from native dict response (camelCase keys from serde)."""
        exec_data = d.get("execution", {})
        execution = WorkflowExecution(
            workflow_id=exec_data.get("workflowId", ""),
            run_id=exec_data.get("runId", ""),
        )

        status_str = d.get("status", "Running")
        try:
            status = WorkflowStatus.from_wire_name(status_str)
        except ValueError:
            # describe() stays lenient: an unrecognized status should not make
            # the whole description unreadable.
            status = WorkflowStatus.RUNNING

        parent = None
        if d.get("parentExecution"):
            p = d["parentExecution"]
            parent = ParentExecutionInfo(
                namespace=p.get("namespace", ""),
                workflow_id=p.get("workflowId", ""),
                execution_id=p.get("executionId", ""),
            )

        config = None
        if d.get("executionConfig"):
            c = d["executionConfig"]
            config = WorkflowExecutionConfig(
                task_queue=c.get("taskQueue", ""),
                execution_timeout_seconds=c.get("executionTimeoutSeconds"),
                run_timeout_seconds=c.get("runTimeoutSeconds"),
                default_task_timeout_seconds=c.get("defaultTaskTimeoutSeconds"),
                cron_schedule=c.get("cronSchedule", ""),
            )

        return cls(
            execution=execution,
            workflow_type=d.get("workflowType", ""),
            task_queue=d.get("taskQueue", ""),
            status=status,
            start_time=_parse_system_time(d.get("startTime")),
            close_time=_parse_system_time(d.get("closeTime")),
            journal_length=d.get("journalLength", 0),
            attempt=d.get("attempt", 0),
            labels=d.get("searchAttributes", {}),
            memo=d.get("memo", {}),
            tags=d.get("tags", []),
            started_by=d.get("startedBy", ""),
            cron_schedule=d.get("cronSchedule", ""),
            state_transition_count=d.get("stateTransitionCount", 0),
            parent_execution=parent,
            pending_tasks=d.get("pendingTasks", 0),
            pending_timers=d.get("pendingTimers", 0),
            pending_events=d.get("pendingEvents", 0),
            execution_config=config,
        )


# ---- Status mapping for list/search results; unknown statuses map to RUNNING ----

_STATUS_MAP: dict[str, WorkflowStatus] = {
    "Running": WorkflowStatus.RUNNING,
    "Completed": WorkflowStatus.COMPLETED,
    "Failed": WorkflowStatus.FAILED,
    "Cancelled": WorkflowStatus.CANCELLED,
    "Terminated": WorkflowStatus.TERMINATED,
    "TimedOut": WorkflowStatus.TIMED_OUT,
    "RestartedFresh": WorkflowStatus.RESTARTED_FRESH,
}


def _parse_system_time(t: Any) -> datetime | None:
    """Parse serde-serialized SystemTime ``{secs_since_epoch, nanos_since_epoch}``."""
    if not t or not isinstance(t, dict):
        return None
    secs = t.get("secs_since_epoch", 0)
    nanos = t.get("nanos_since_epoch", 0)
    return datetime.fromtimestamp(secs + nanos / 1e9)


@dataclass(frozen=True)
class WorkflowExecutionInfo:
    """Summary information about a workflow execution.

    Returned in list/search results.
    """

    workflow_id: str
    execution_id: str
    workflow_type: str
    task_queue: str
    namespace: str
    status: WorkflowStatus
    start_time: datetime | None
    close_time: datetime | None
    execution_duration_seconds: int | None
    journal_length: int
    parent_execution: ParentExecutionInfo | None
    labels: dict[str, str]
    memo: dict[str, Any]
    tags: list[str]
    started_by: str
    attempt: int
    cron_schedule: str
    state_transition_count: int

    @classmethod
    def _from_dict(cls, d: dict[str, Any]) -> WorkflowExecutionInfo:
        """Create from native dict response (camelCase keys from serde)."""
        status = _STATUS_MAP.get(d.get("status", "Running"), WorkflowStatus.RUNNING)

        parent = None
        if d.get("parentExecution"):
            p = d["parentExecution"]
            parent = ParentExecutionInfo(
                namespace=p.get("namespace", ""),
                workflow_id=p.get("workflowId", ""),
                execution_id=p.get("executionId", ""),
            )

        return cls(
            workflow_id=d.get("workflowId", ""),
            execution_id=d.get("executionId", ""),
            workflow_type=d.get("workflowType", ""),
            task_queue=d.get("taskQueue", ""),
            namespace=d.get("namespace", ""),
            status=status,
            start_time=_parse_system_time(d.get("startTime")),
            close_time=_parse_system_time(d.get("closeTime")),
            execution_duration_seconds=d.get("executionDurationSeconds"),
            journal_length=d.get("journalLength", 0),
            parent_execution=parent,
            labels=d.get("searchAttributes", {}),
            memo=d.get("memo", {}),
            tags=d.get("tags", []),
            started_by=d.get("startedBy", ""),
            attempt=d.get("attempt", 0),
            cron_schedule=d.get("cronSchedule", ""),
            state_transition_count=d.get("stateTransitionCount", 0),
        )


@dataclass(frozen=True)
class WorkflowListPage:
    """A page of workflow execution results.

    Returned by ``Client.list_workflows()`` and ``Client.search_workflows()``.
    """

    executions: list[WorkflowExecutionInfo]
    next_page_token: bytes

    @property
    def has_more(self) -> bool:
        """Check if there are more pages available."""
        return len(self.next_page_token) > 0

    @classmethod
    def _from_dict(cls, d: dict[str, Any]) -> WorkflowListPage:
        """Create from native dict response (camelCase keys from serde)."""
        executions = [
            WorkflowExecutionInfo._from_dict(e)
            for e in d.get("executions", [])
        ]
        token = d.get("nextPageToken", [])
        # serde serializes Vec<u8> as a JSON array of integers
        if isinstance(token, list):
            token = bytes(token)
        elif isinstance(token, str):
            token = token.encode("utf-8")
        return cls(executions=executions, next_page_token=token)
