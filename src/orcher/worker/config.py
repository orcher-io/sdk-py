"""
Worker configuration.

Defines the WorkerConfig dataclass that configures the Worker runtime.
"""

import os
import uuid
from dataclasses import dataclass, field


def _generate_identity() -> str:
    """Generate a unique worker identity from host name, pid, and a random suffix."""
    hostname = os.uname().nodename
    pid = os.getpid()
    return f"service-{hostname}-{pid}-{uuid.uuid4().hex[:8]}"


def _undeclared_if_blank(value: str | None) -> str | None:
    """Treat an empty or whitespace release as "not declared".

    The wire type is a bare string. Without this, an unset environment
    variable would bind executions to a release named "". That value looks
    like a real release, so versioning would appear to work when it does not.
    """
    if value is None or not value.strip():
        return None
    return value


@dataclass
class WorkerConfig:
    """
    Configuration for a Worker.

    Specifies how the Worker connects to the Orcher server and sets
    execution parameters such as concurrency limits.

    Attributes:
        server_url: Orcher server address (e.g., "http://localhost:50051")
        namespace: Namespace to operate in (default: "default")
        task_queue: Task queue to poll from
        max_concurrent_workflow_executions: Maximum concurrent workflow executions (default: 100)
        max_concurrent_task_executions: Maximum concurrent task executions (default: 100)
        identity: Unique identity for this worker (auto-generated if not provided)
        workflow_poll_interval_ms: Interval between workflow polls in milliseconds (default: 100)
        task_poll_interval_ms: Interval between task polls in milliseconds (default: 100)
        workflow_poller_count: Number of concurrent workflow pollers (default: 4)
        task_poller_count: Number of concurrent task pollers (default: 4)
        shutdown_grace_time_ms: Grace period for shutdown in milliseconds (default: 30000)
        force_shutdown_timeout_ms: Force shutdown timeout in milliseconds (default: 60000)
        version_id: Optional code release this worker runs. The server binds an
            execution to it on first claim, so replay stays on the code the
            execution started on. from_env reads it from {prefix}VERSION_ID.
        binary_checksum: Optional binary checksum for versioning
        actor_poller_count: Number of concurrent actor pollers (default: 4)
        max_concurrent_actor_operations: Maximum concurrent actor operations (default: 100)
        organization_id: Optional organization ID, sent as the `X-Organization-Id` header
        api_key: Optional API key, sent as `authorization: Bearer <key>` on every
            request the worker makes, for servers that require one. from_env
            reads it from {prefix}API_KEY. Never shown by repr.

    Example:
        >>> config = WorkerConfig(
        ...     server_url="http://localhost:50051",
        ...     namespace="default",
        ...     task_queue="order-queue",
        ...     max_concurrent_workflow_executions=50,
        ...     max_concurrent_task_executions=100,
        ... )
    """

    # Required fields
    server_url: str
    task_queue: str

    # Optional fields with defaults
    namespace: str = "default"
    max_concurrent_workflow_executions: int = 100
    max_concurrent_task_executions: int = 100
    identity: str = field(default_factory=_generate_identity)

    # Polling configuration
    workflow_poll_interval_ms: int = 100
    task_poll_interval_ms: int = 100
    workflow_poller_count: int = 4
    task_poller_count: int = 4

    # Shutdown configuration
    shutdown_grace_time_ms: int = 30000
    force_shutdown_timeout_ms: int = 60000

    # Versioning (optional)
    version_id: str | None = None
    binary_checksum: str | None = None

    # Actor configuration
    actor_poller_count: int = 4
    max_concurrent_actor_operations: int = 100

    # Multi-tenancy (optional). When set, the worker sends it to the server in
    # the `X-Organization-Id` header, enabling organization-level quotas and
    # billing attribution.
    organization_id: str | None = None

    # Authentication (optional). Kept out of repr so that logging a config
    # never logs the key.
    api_key: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self._validate()

    def _validate(self) -> None:
        """Validate configuration values."""
        if not self.server_url or not self.server_url.strip():
            raise ValueError("server_url is required and cannot be empty")

        if not self.task_queue or not self.task_queue.strip():
            raise ValueError("task_queue is required and cannot be empty")

        if not self.namespace or not self.namespace.strip():
            raise ValueError("namespace is required and cannot be empty")

        if self.max_concurrent_workflow_executions < 1:
            raise ValueError("max_concurrent_workflow_executions must be >= 1")

        if self.max_concurrent_task_executions < 1:
            raise ValueError("max_concurrent_task_executions must be >= 1")

        if self.workflow_poll_interval_ms < 1:
            raise ValueError("workflow_poll_interval_ms must be >= 1")

        if self.task_poll_interval_ms < 1:
            raise ValueError("task_poll_interval_ms must be >= 1")

        if self.workflow_poller_count < 1:
            raise ValueError("workflow_poller_count must be >= 1")

        if self.task_poller_count < 1:
            raise ValueError("task_poller_count must be >= 1")

        if self.shutdown_grace_time_ms < 0:
            raise ValueError("shutdown_grace_time_ms must be >= 0")

        if self.force_shutdown_timeout_ms < 0:
            raise ValueError("force_shutdown_timeout_ms must be >= 0")

    @classmethod
    def from_env(cls, prefix: str = "ORCHER_") -> "WorkerConfig":
        """
        Create a WorkerConfig from environment variables.

        Environment variables are read with the given prefix:
        - {prefix}SERVER_URL
        - {prefix}NAMESPACE
        - {prefix}TASK_QUEUE
        - {prefix}MAX_CONCURRENT_WORKFLOWS
        - {prefix}MAX_CONCURRENT_TASKS
        - {prefix}IDENTITY
        - {prefix}WORKFLOW_POLL_INTERVAL_MS, {prefix}TASK_POLL_INTERVAL_MS
        - {prefix}WORKFLOW_POLLER_COUNT, {prefix}TASK_POLLER_COUNT
        - {prefix}SHUTDOWN_GRACE_TIME_MS, {prefix}FORCE_SHUTDOWN_TIMEOUT_MS
        - {prefix}VERSION_ID, {prefix}BINARY_CHECKSUM, {prefix}ORGANIZATION_ID
        - {prefix}API_KEY

        Only SERVER_URL and TASK_QUEUE are required.

        Args:
            prefix: Environment variable prefix (default: "ORCHER_")

        Returns:
            WorkerConfig instance

        Raises:
            ValueError: If required environment variables are not set

        Example:
            >>> # With ORCHER_SERVER_URL=http://localhost:50051
            >>> # and ORCHER_TASK_QUEUE=my-queue set
            >>> config = WorkerConfig.from_env()
        """
        server_url = os.environ.get(f"{prefix}SERVER_URL")
        if not server_url:
            raise ValueError(f"{prefix}SERVER_URL environment variable is required")

        task_queue = os.environ.get(f"{prefix}TASK_QUEUE")
        if not task_queue:
            raise ValueError(f"{prefix}TASK_QUEUE environment variable is required")

        return cls(
            server_url=server_url,
            task_queue=task_queue,
            namespace=os.environ.get(f"{prefix}NAMESPACE", "default"),
            max_concurrent_workflow_executions=int(
                os.environ.get(f"{prefix}MAX_CONCURRENT_WORKFLOWS", "100")
            ),
            max_concurrent_task_executions=int(
                os.environ.get(f"{prefix}MAX_CONCURRENT_TASKS", "100")
            ),
            identity=os.environ.get(f"{prefix}IDENTITY") or _generate_identity(),
            workflow_poll_interval_ms=int(
                os.environ.get(f"{prefix}WORKFLOW_POLL_INTERVAL_MS", "100")
            ),
            task_poll_interval_ms=int(os.environ.get(f"{prefix}TASK_POLL_INTERVAL_MS", "100")),
            workflow_poller_count=int(os.environ.get(f"{prefix}WORKFLOW_POLLER_COUNT", "4")),
            task_poller_count=int(os.environ.get(f"{prefix}TASK_POLLER_COUNT", "4")),
            shutdown_grace_time_ms=int(os.environ.get(f"{prefix}SHUTDOWN_GRACE_TIME_MS", "30000")),
            force_shutdown_timeout_ms=int(
                os.environ.get(f"{prefix}FORCE_SHUTDOWN_TIMEOUT_MS", "60000")
            ),
            version_id=_undeclared_if_blank(os.environ.get(f"{prefix}VERSION_ID")),
            binary_checksum=os.environ.get(f"{prefix}BINARY_CHECKSUM"),
            organization_id=os.environ.get(f"{prefix}ORGANIZATION_ID"),
            # Blank is "no key": an exported-but-empty variable must not send
            # an empty bearer token.
            api_key=_undeclared_if_blank(os.environ.get(f"{prefix}API_KEY")),
        )
