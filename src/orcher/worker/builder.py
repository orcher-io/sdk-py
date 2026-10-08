"""
Worker Builder

Fluent API for constructing Worker instances with optional auto-discovery.
"""

from __future__ import annotations

import os
from concurrent.futures import Executor
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from orcher.interceptors.base import (
    Interceptor,
    InterceptorFactory,
    TaskInterceptor,
    WorkflowInterceptor,
)
from orcher.worker.config import WorkerConfig, _undeclared_if_blank

if TYPE_CHECKING:
    from orcher.worker.worker import Worker


def _to_millis(value: timedelta | int) -> int:
    """Normalize a duration option to milliseconds.

    Accepts a ``timedelta``, which states its unit at the call site, or a bare
    ``int`` of milliseconds. A bare number lets a seconds/milliseconds mix-up
    pass silently, so ``timedelta`` is preferred; ``int`` remains supported.
    """
    if isinstance(value, timedelta):
        return int(value.total_seconds() * 1000)
    return value


class WorkerBuilder:
    """
    Builder for constructing Worker instances.

    Provides a fluent API for configuring and creating Worker instances.
    Supports optional auto-discovery of decorated classes.

    Example:
        >>> # Option 1: With auto-discovery
        >>> worker = (
        ...     Worker.builder()
        ...     .server_url("http://localhost:50051")
        ...     .namespace("default")
        ...     .task_queue("my-queue")
        ...     .auto_discover("./src")
        ...     .build()
        ... )

        >>> # Option 2: Manual imports (no auto-discovery)
        >>> from myapp.tasks import PaymentTasks
        >>> from myapp.workflows import OrderWorkflow
        >>>
        >>> worker = (
        ...     Worker.builder()
        ...     .server_url("http://localhost:50051")
        ...     .namespace("default")
        ...     .task_queue("my-queue")
        ...     .build()
        ... )
    """

    def __init__(self) -> None:
        """Initialize the builder with default values."""
        self._server_url: str | None = None
        self._namespace: str = "default"
        self._task_queue: str | None = None
        self._max_concurrent_workflow_executions: int = 100
        self._max_concurrent_task_executions: int = 100
        self._identity: str | None = None
        self._workflow_poll_interval_ms: int = 100
        self._task_poll_interval_ms: int = 100
        self._workflow_poller_count: int = 4
        self._task_poller_count: int = 4
        self._shutdown_grace_time_ms: int = 30000
        self._force_shutdown_timeout_ms: int = 60000
        # Default from the environment, matching WorkerConfig.from_env and the
        # other SDKs. The release id usually comes from CI, so ORCHER_VERSION_ID
        # must work whether the worker is built here or by from_env. An
        # explicit .version_id() takes precedence.
        self._version_id: str | None = _undeclared_if_blank(os.environ.get("ORCHER_VERSION_ID"))
        self._binary_checksum: str | None = None
        self._organization_id: str | None = None
        # Keys are issued out of band, so the environment is the usual way one
        # reaches a process. Read the same variable as from_env and the client;
        # an explicit .api_key() takes precedence.
        self._api_key: str | None = _undeclared_if_blank(os.environ.get("ORCHER_API_KEY"))
        self._tls_ca_cert_path: str | None = None
        self._tls_client_cert_path: str | None = None
        self._tls_client_key_path: str | None = None

        # Auto-discovery settings
        self._auto_discover_paths: list[str] | None = None
        self._auto_discover_patterns: list[str] | None = None
        self._auto_discover_exclude: list[str] | None = None

        # Actor configuration
        self._actor_poller_count: int = 4
        self._max_concurrent_actor_operations: int = 100

        # Executor for sync tasks (CPU-bound or blocking I/O)
        self._task_executor: Executor | None = None

        # Interceptors
        self._workflow_interceptors: list[WorkflowInterceptor] = []
        self._task_interceptors: list[TaskInterceptor] = []

    def server_url(self, url: str) -> WorkerBuilder:
        """
        Set the server URL.

        Args:
            url: Server URL (e.g., "http://localhost:50051")

        Returns:
            Builder instance for chaining
        """
        self._server_url = url
        return self

    def namespace(self, namespace: str) -> WorkerBuilder:
        """
        Set the namespace.

        Args:
            namespace: Namespace to operate in

        Returns:
            Builder instance for chaining
        """
        self._namespace = namespace
        return self

    def task_queue(self, queue: str) -> WorkerBuilder:
        """
        Set the task queue.

        Args:
            queue: Task queue name

        Returns:
            Builder instance for chaining
        """
        self._task_queue = queue
        return self

    def max_concurrent_workflows(self, max_count: int) -> WorkerBuilder:
        """
        Set maximum concurrent workflow executions.

        Args:
            max_count: Maximum concurrent workflows

        Returns:
            Builder instance for chaining
        """
        self._max_concurrent_workflow_executions = max_count
        return self

    def max_concurrent_tasks(self, max_count: int) -> WorkerBuilder:
        """
        Set maximum concurrent task executions.

        Args:
            max_count: Maximum concurrent tasks

        Returns:
            Builder instance for chaining
        """
        self._max_concurrent_task_executions = max_count
        return self

    def identity(self, identity: str) -> WorkerBuilder:
        """
        Set worker identity.

        Args:
            identity: Worker identity string

        Returns:
            Builder instance for chaining
        """
        self._identity = identity
        return self

    def workflow_poll_interval(self, interval: timedelta | int) -> WorkerBuilder:
        """
        Set workflow polling interval.

        Args:
            interval: Polling interval as a ``timedelta``, or milliseconds as an ``int``

        Returns:
            Builder instance for chaining
        """
        self._workflow_poll_interval_ms = _to_millis(interval)
        return self

    def task_poll_interval(self, interval: timedelta | int) -> WorkerBuilder:
        """
        Set task polling interval.

        Args:
            interval: Polling interval as a ``timedelta``, or milliseconds as an ``int``

        Returns:
            Builder instance for chaining
        """
        self._task_poll_interval_ms = _to_millis(interval)
        return self

    def workflow_poller_count(self, count: int) -> WorkerBuilder:
        """
        Set number of concurrent workflow pollers.

        Args:
            count: Number of workflow pollers

        Returns:
            Builder instance for chaining
        """
        self._workflow_poller_count = count
        return self

    def task_poller_count(self, count: int) -> WorkerBuilder:
        """
        Set number of concurrent task pollers.

        Args:
            count: Number of task pollers

        Returns:
            Builder instance for chaining
        """
        self._task_poller_count = count
        return self

    def actor_poller_count(self, count: int) -> WorkerBuilder:
        """Set number of concurrent actor pollers.

        Args:
            count: Number of actor pollers

        Returns:
            Builder instance for chaining
        """
        self._actor_poller_count = count
        return self

    def max_concurrent_actor_operations(self, max_count: int) -> WorkerBuilder:
        """Set maximum concurrent actor operations.

        Args:
            max_count: Maximum concurrent actor operations

        Returns:
            Builder instance for chaining
        """
        self._max_concurrent_actor_operations = max_count
        return self

    def shutdown_grace_time(self, timeout: timedelta | int) -> WorkerBuilder:
        """
        Set shutdown grace time.

        Args:
            timeout: Grace time as a ``timedelta``, or milliseconds as an ``int``

        Returns:
            Builder instance for chaining
        """
        self._shutdown_grace_time_ms = _to_millis(timeout)
        return self

    def force_shutdown_timeout(self, timeout: timedelta | int) -> WorkerBuilder:
        """
        Set force shutdown timeout.

        Args:
            timeout: Timeout as a ``timedelta``, or milliseconds as an ``int``

        Returns:
            Builder instance for chaining
        """
        self._force_shutdown_timeout_ms = _to_millis(timeout)
        return self

    def version_id(self, version_id: str) -> WorkerBuilder:
        """
        Declare the code release this worker is running.

        Opaque — a git sha, an image digest, a release tag. The server records
        it on an execution the first time this worker claims one, so that
        execution keeps replaying against the code it started on.

        Args:
            version_id: Release identifier

        Returns:
            Builder instance for chaining
        """
        self._version_id = version_id
        return self

    def binary_checksum(self, checksum: str) -> WorkerBuilder:
        """
        Set binary checksum.

        Args:
            checksum: Binary checksum

        Returns:
            Builder instance for chaining
        """
        self._binary_checksum = checksum
        return self

    def organization_id(self, org_id: str) -> WorkerBuilder:
        """
        Set the organization ID for multi-tenancy.

        This enables organization-level quotas and billing attribution.
        When set, the worker sends it to the server in the
        `X-Organization-Id` header on every request.

        Args:
            org_id: Organization ID

        Returns:
            Builder instance for chaining

        Example:
            >>> builder.organization_id("org_abc123")
        """
        self._organization_id = org_id
        return self

    def api_key(self, key: str | None) -> WorkerBuilder:
        """
        Set the API key the worker authenticates with.

        Sent as `authorization: Bearer <key>` on every request the worker
        makes: polling, reporting results, heartbeats, registration and actor
        calls. Defaults to the `ORCHER_API_KEY` environment variable.

        Args:
            key: API key. None or blank sends none.

        Returns:
            Builder instance for chaining

        Example:
            >>> builder.api_key(os.environ["ORCHER_API_KEY"])
        """
        self._api_key = _undeclared_if_blank(key)
        return self

    def tls(
        self,
        ca_cert_path: str | os.PathLike[str] | None = None,
        client_cert_path: str | os.PathLike[str] | None = None,
        client_key_path: str | os.PathLike[str] | None = None,
    ) -> WorkerBuilder:
        """
        Set the TLS certificates for every connection the worker opens.

        An ``https://`` server URL needs none of this: it connects over TLS
        verified against the system trust store by default. Supply a CA for a
        private or self-signed authority, and a client certificate and key for
        mTLS. The paths match the client's ``tls_*`` options.

        Args:
            ca_cert_path: CA certificate PEM file. None verifies the server
                against the system trust store.
            client_cert_path: Client certificate PEM file, for mTLS. Requires
                client_key_path.
            client_key_path: Client private key PEM file, for mTLS. Requires
                client_cert_path.

        Returns:
            Builder instance for chaining

        Raises:
            ValueError: If only one of client_cert_path and client_key_path is given.

        Example:
            >>> builder.server_url("https://orcher.internal:443").tls(
            ...     ca_cert_path="ca.pem",
            ...     client_cert_path="client.pem",
            ...     client_key_path="client-key.pem",
            ... )
        """
        if (client_cert_path is None) != (client_key_path is None):
            raise ValueError("client_cert_path and client_key_path must be set together")
        self._tls_ca_cert_path = os.fspath(ca_cert_path) if ca_cert_path is not None else None
        self._tls_client_cert_path = (
            os.fspath(client_cert_path) if client_cert_path is not None else None
        )
        self._tls_client_key_path = (
            os.fspath(client_key_path) if client_key_path is not None else None
        )
        return self

    def task_executor(self, executor: Executor) -> WorkerBuilder:
        """
        Set the executor for sync tasks.

        Use this to provide a custom executor for running synchronous tasks:
        - ThreadPoolExecutor: For I/O-bound sync tasks (default if not specified)
        - ProcessPoolExecutor: For CPU-bound tasks (pandas, numpy, ML inference)

        Async tasks always run directly in the event loop regardless of this setting.

        Args:
            executor: The executor instance (ThreadPoolExecutor or ProcessPoolExecutor)

        Returns:
            Builder instance for chaining

        Example:
            >>> from concurrent.futures import ProcessPoolExecutor
            >>> worker = (
            ...     Worker.builder()
            ...     .server_url("http://localhost:50051")
            ...     .task_queue("ml-queue")
            ...     .task_executor(ProcessPoolExecutor(max_workers=4))
            ...     .build()
            ... )
        """
        self._task_executor = executor
        return self

    def interceptor(self, interceptor: Interceptor | InterceptorFactory) -> WorkerBuilder:
        """Add an interceptor for cross-cutting concerns.

        An interceptor is routed to the workflow or task chain based on its
        type. If it implements both WorkflowInterceptor and TaskInterceptor,
        it is added to both chains.

        A factory such as the built-in ``LoggingInterceptor``,
        ``MetricsInterceptor`` or ``TracingInterceptor`` is not an interceptor
        itself; passing one adds ``factory.workflow()`` to the workflow chain
        and ``factory.task()`` to the task chain.

        Args:
            interceptor: A WorkflowInterceptor, a TaskInterceptor, one that is
                both, or an InterceptorFactory.

        Returns:
            Builder instance for chaining

        Raises:
            TypeError: If ``interceptor`` is none of these, rather than
                ignoring it.

        Example:
            >>> from orcher.interceptors.builtin import LoggingInterceptor
            >>> worker = (
            ...     Worker.builder()
            ...     .server_url("http://localhost:50051")
            ...     .task_queue("my-queue")
            ...     .interceptor(LoggingInterceptor())
            ...     .build()
            ... )
        """
        if isinstance(interceptor, WorkflowInterceptor | TaskInterceptor):
            if isinstance(interceptor, WorkflowInterceptor):
                self._workflow_interceptors.append(interceptor)
            if isinstance(interceptor, TaskInterceptor):
                self._task_interceptors.append(interceptor)
            return self

        if isinstance(interceptor, InterceptorFactory):
            workflow_interceptor = interceptor.workflow()
            task_interceptor = interceptor.task()
            if not isinstance(workflow_interceptor, WorkflowInterceptor) or not isinstance(
                task_interceptor, TaskInterceptor
            ):
                raise TypeError(
                    f"{type(interceptor).__name__}.workflow() and .task() must return a "
                    "WorkflowInterceptor and a TaskInterceptor; got "
                    f"{type(workflow_interceptor).__name__} and "
                    f"{type(task_interceptor).__name__}"
                )
            self._workflow_interceptors.append(workflow_interceptor)
            self._task_interceptors.append(task_interceptor)
            return self

        raise TypeError(
            "interceptor() expects a WorkflowInterceptor, a TaskInterceptor, or a "
            "factory with workflow() and task() methods (such as LoggingInterceptor); "
            f"got {type(interceptor).__name__}"
        )

    def workflow_interceptor(self, interceptor: WorkflowInterceptor) -> WorkerBuilder:
        """Add a workflow-specific interceptor.

        Args:
            interceptor: A WorkflowInterceptor instance.

        Returns:
            Builder instance for chaining
        """
        self._workflow_interceptors.append(interceptor)
        return self

    def task_interceptor(self, interceptor: TaskInterceptor) -> WorkerBuilder:
        """Add a task-specific interceptor.

        Args:
            interceptor: A TaskInterceptor instance.

        Returns:
            Builder instance for chaining
        """
        self._task_interceptors.append(interceptor)
        return self

    def auto_discover(
        self,
        paths: list[str] | None = None,
        *,
        patterns: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> WorkerBuilder:
        """
        Enable optional auto-discovery of decorated classes.

        Scans Python files in the given paths and imports them so their
        decorators register. Without this call, import the decorated classes
        yourself before build(). Files that fail to import are skipped.

        Args:
            paths: List of directories to scan (default: ["./src"])
            patterns: Glob patterns for files to include (default: ["**/*.py"])
            exclude: Glob patterns for files to exclude
                (default: ["**/__pycache__/**", "**/*.pyc", "**/*_test.py"])

        Returns:
            Builder instance for chaining

        Example:
            >>> # Use defaults
            >>> builder.auto_discover()

            >>> # Custom paths
            >>> builder.auto_discover(["./src/tasks", "./src/workflows"])

            >>> # Custom patterns
            >>> builder.auto_discover(
            ...     patterns=["**/tasks/*.py", "**/workflows/*.py"],
            ...     exclude=["**/*_test.py"]
            ... )
        """
        self._auto_discover_paths = paths or ["./src"]
        self._auto_discover_patterns = patterns or ["**/*.py"]
        self._auto_discover_exclude = exclude or ["**/__pycache__/**", "**/*.pyc", "**/*_test.py"]
        return self

    def build(self) -> Worker:
        """
        Build the Worker instance.

        If auto-discovery is enabled, scans and imports files first. The
        Worker then registers every handler found in the GlobalRegistry.

        Returns:
            Worker instance

        Raises:
            ValueError: If required options are missing

        Example:
            >>> worker = builder.build()
            >>> await worker.run()
        """
        if not self._server_url:
            raise ValueError("server_url is required. Call .server_url(url) before .build()")

        if not self._task_queue:
            raise ValueError("task_queue is required. Call .task_queue(queue) before .build()")

        if self._auto_discover_paths is not None:
            self._run_auto_discovery()

        config_kwargs: dict[str, Any] = {
            "server_url": self._server_url,
            "namespace": self._namespace,
            "task_queue": self._task_queue,
            "max_concurrent_workflow_executions": self._max_concurrent_workflow_executions,
            "max_concurrent_task_executions": self._max_concurrent_task_executions,
            "workflow_poll_interval_ms": self._workflow_poll_interval_ms,
            "task_poll_interval_ms": self._task_poll_interval_ms,
            "workflow_poller_count": self._workflow_poller_count,
            "task_poller_count": self._task_poller_count,
            "shutdown_grace_time_ms": self._shutdown_grace_time_ms,
            "force_shutdown_timeout_ms": self._force_shutdown_timeout_ms,
            "actor_poller_count": self._actor_poller_count,
            "max_concurrent_actor_operations": self._max_concurrent_actor_operations,
        }

        if self._identity:
            config_kwargs["identity"] = self._identity

        if self._version_id:
            config_kwargs["version_id"] = self._version_id

        if self._binary_checksum:
            config_kwargs["binary_checksum"] = self._binary_checksum

        if self._organization_id:
            config_kwargs["organization_id"] = self._organization_id

        if self._api_key:
            config_kwargs["api_key"] = self._api_key

        config_kwargs["tls_ca_cert_path"] = self._tls_ca_cert_path
        config_kwargs["tls_client_cert_path"] = self._tls_client_cert_path
        config_kwargs["tls_client_key_path"] = self._tls_client_key_path

        config = WorkerConfig(**config_kwargs)

        # Imported here to avoid a circular import.
        from orcher.worker.worker import Worker

        return Worker(
            config,
            task_executor=self._task_executor,
            workflow_interceptors=self._workflow_interceptors,
            task_interceptors=self._task_interceptors,
        )

    def _run_auto_discovery(self) -> None:
        """
        Run auto-discovery to import decorated modules.

        This imports Python files matching the configured patterns,
        which triggers decorator registration with the GlobalRegistry.
        """
        import fnmatch
        import importlib.util
        import sys
        from pathlib import Path

        if not self._auto_discover_paths:
            return

        imported_count = 0
        failed_count = 0

        for base_path in self._auto_discover_paths:
            base = Path(base_path).resolve()
            if not base.exists():
                continue

            for pattern in self._auto_discover_patterns or ["**/*.py"]:
                for py_file in base.glob(pattern):
                    if self._auto_discover_exclude:
                        relative = str(py_file.relative_to(base))
                        if any(
                            fnmatch.fnmatch(relative, exc) for exc in self._auto_discover_exclude
                        ):
                            continue

                    if py_file.name == "__init__.py":
                        continue

                    try:
                        # Unique per file so same-named files in different
                        # directories do not collide in sys.modules.
                        module_name = f"_auto_discover_{py_file.stem}_{id(py_file)}"

                        spec = importlib.util.spec_from_file_location(module_name, py_file)
                        if spec and spec.loader:
                            module = importlib.util.module_from_spec(spec)
                            sys.modules[module_name] = module
                            spec.loader.exec_module(module)
                            imported_count += 1
                    except Exception:
                        failed_count += 1
                        # Skip files that cannot be imported here, such as
                        # scripts with syntax errors or unavailable imports.

    @classmethod
    def create(cls) -> WorkerBuilder:
        """
        Create a new WorkerBuilder instance.

        Returns:
            New builder instance

        Example:
            >>> worker = WorkerBuilder.create().server_url("...").task_queue("q").build()
        """
        return cls()
