"""Client implementation for ORCHER Python SDK."""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from orcher.client.config import ClientConfig
from orcher.client.workflow_handle import WorkflowHandle
from orcher.errors import ClientError
from orcher.errors._translate import translate
from orcher.types import RetryPolicy, WorkflowIdReusePolicy, WorkflowListPage

__all__ = [
    "Client",
]


def _retry_policy_to_native(rp: RetryPolicy | dict[str, Any]) -> dict[str, Any]:
    """Convert a workflow retry policy into the millisecond dict the native binding reads.

    Accepts a ``RetryPolicy`` or a plain dict; a dict is passed through unchanged.
    """
    if isinstance(rp, RetryPolicy):
        return {
            "max_attempts": rp.max_attempts,
            "initial_interval_ms": int(rp.initial_interval.total_seconds() * 1000),
            "max_interval_ms": int(rp.max_interval.total_seconds() * 1000),
            "backoff_coefficient": rp.backoff_coefficient,
            "non_retryable_error_types": list(rp.non_retryable_error_types),
        }
    if isinstance(rp, dict):
        return rp
    raise TypeError(f"retry_policy must be a RetryPolicy or dict, got {type(rp).__name__}")


class Client:
    """Client for interacting with ORCHER server.

    The Client provides methods to start workflows, get handles to existing
    workflows, and manage workflow executions. Every operation goes through
    the native Rust module, which talks to the server over gRPC.

    Example:
        >>> config = ClientConfig(server_url="http://localhost:50051")
        >>> async with Client(config) as client:
        ...     handle = await client.start_workflow(
        ...         workflow_type="my-workflow",
        ...         workflow_id="workflow-1",
        ...         task_queue="my-queue",
        ...     )
        ...     result = await handle.result()

    The client can also be used without the async context manager:

        >>> client = Client(config)
        >>> await client.connect()
        >>> # ... use client ...
        >>> await client.close()
    """

    def __init__(self, config: ClientConfig) -> None:
        """Initialize the client.

        Args:
            config: Client configuration.
        """
        self._config = config
        self._connected = False
        self._native_client: Any = None

    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected."""
        return self._connected

    async def connect(self) -> None:
        """Establish connection to the ORCHER server.

        This method must be called before using the client, unless
        using the async context manager.

        Raises:
            ClientError: If connection fails.
        """
        if self._connected:
            return

        try:
            from orcher import _native

            native_config = _native.ClientConfig(
                server_url=self._config.server_url,
                namespace=self._config.namespace,
                timeout_ms=int(self._config.request_timeout.total_seconds() * 1000),
                connect_timeout_ms=int(self._config.connection_timeout.total_seconds() * 1000),
                identity=self._config.identity,
                tls_ca_cert_path=self._config.tls_ca_cert_path,
                tls_client_cert_path=self._config.tls_client_cert_path,
                tls_client_key_path=self._config.tls_client_key_path,
                api_key=self._config.api_key,
            )

            native_client = _native.Client()
            await native_client.connect(native_config)

            self._native_client = native_client
            self._connected = True
        except ImportError as e:
            raise ClientError.connection_failed(
                self._config.server_url,
                f"Native module not available: {e}",
            ) from e
        except Exception as e:
            raise ClientError.connection_failed(
                self._config.server_url,
                str(e),
            ) from e

    async def close(self) -> None:
        """Close the client connection.

        This method should be called when done using the client to
        release resources.
        """
        if not self._connected:
            return

        if self._native_client is not None:
            await self._native_client.close()

        self._native_client = None
        self._connected = False

    async def __aenter__(self) -> Client:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.close()

    def _ensure_connected(self) -> None:
        """Ensure client is connected, raise error if not."""
        if not self._connected or self._native_client is None:
            raise ClientError.connection_failed(
                self._config.server_url,
                "Client is not connected. Call connect() first or use async context manager.",
            )

    async def start_workflow(
        self,
        workflow_type: str,
        *,
        workflow_id: str | None = None,
        task_queue: str | None = None,
        args: tuple[Any, ...] = (),
        retry_policy: RetryPolicy | dict[str, Any] | None = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        cron_schedule: str | None = None,
        id_reuse_policy: WorkflowIdReusePolicy | None = None,
    ) -> WorkflowHandle[Any]:
        """Start a new workflow execution.

        Args:
            workflow_type: Type/name of the workflow to execute.
            workflow_id: Unique identifier for this workflow execution.
            task_queue: Task queue where the workflow will be executed.
            args: Arguments to pass to the workflow.
            retry_policy: Optional retry policy for the workflow.
            execution_timeout: Maximum time for the entire workflow execution.
            run_timeout: Maximum time for a single run of the workflow.
            task_timeout: Maximum time for workflow tasks.
            cron_schedule: Optional cron expression for periodic execution
                (e.g., "0 9 * * *" for daily at 9am).
            id_reuse_policy: What to do when ``workflow_id`` is already in use.
                ``None`` leaves it to the server, which allows a new run once
                the previous one has closed.

        Returns:
            A WorkflowHandle for interacting with the started workflow.

        Raises:
            WorkflowError: If ``workflow_id`` is already in use — its run is
                still open, or closed and ``id_reuse_policy`` does not allow
                reusing it. Its code is ``WORKFLOW_ALREADY_EXISTS`` and its
                ``run_id`` names the run in the way.
            ClientError: If there's a communication error.

        Example:
            >>> handle = await client.start_workflow(
            ...     workflow_type="order-processing",
            ...     workflow_id="order-123",
            ...     task_queue="orders",
            ...     args=(order_data,),
            ... )
        """
        self._ensure_connected()

        # Both are optional, as in the Rust and TypeScript SDKs: a missing id
        # becomes a random UUID and a missing queue becomes "default".
        workflow_id = workflow_id or str(uuid.uuid4())
        task_queue = task_queue or "default"

        # A single argument is sent as-is; several are sent as a tuple.
        input_data = args[0] if len(args) == 1 else (args if args else None)

        kwargs: dict[str, Any] = {}
        if execution_timeout is not None:
            kwargs["execution_timeout_ms"] = int(execution_timeout.total_seconds() * 1000)
        if run_timeout is not None:
            kwargs["run_timeout_ms"] = int(run_timeout.total_seconds() * 1000)
        if task_timeout is not None:
            kwargs["task_timeout_ms"] = int(task_timeout.total_seconds() * 1000)
        if cron_schedule is not None:
            kwargs["cron_schedule"] = cron_schedule
        if retry_policy is not None:
            kwargs["retry_policy"] = _retry_policy_to_native(retry_policy)
        if id_reuse_policy is not None:
            kwargs["id_reuse_policy"] = WorkflowIdReusePolicy(id_reuse_policy).value

        try:
            native_handle = await self._native_client.start_workflow(
                workflow_id,
                workflow_type,
                task_queue,
                input_data,
                **kwargs,
            )
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            # A refused start becomes the WorkflowError documented above, naming
            # the existing run. Any other error propagates unchanged.
            translated = translate(exc, workflow_id=workflow_id)
            if translated is exc:
                raise
            raise translated from exc

        return WorkflowHandle._from_native(self, native_handle)

    async def get_workflow(
        self,
        workflow_id: str,
        run_id: str | None = None,
    ) -> WorkflowHandle[Any]:
        """Get a handle to an existing workflow.

        Args:
            workflow_id: ID of the workflow.
            run_id: Optional run ID. If not provided, gets the latest run.

        Returns:
            A WorkflowHandle for interacting with the workflow.

        Raises:
            WorkflowError: If the workflow is not found.
            ClientError: If there's a communication error.
        """
        self._ensure_connected()

        native_handle = await self._native_client.get_workflow_handle(
            workflow_id,
            run_id,
        )

        return WorkflowHandle._from_native(self, native_handle)

    async def list_workflows(
        self,
        *,
        page_size: int = 100,
        next_page_token: bytes | None = None,
        workflow_type: str | None = None,
        task_queue: str | None = None,
        status_filter: list[str] | None = None,
        sort_order: str | None = None,
    ) -> WorkflowListPage:
        """List workflow executions with optional filters.

        Args:
            page_size: Maximum results per page (default 100).
            next_page_token: Pagination token from a previous response.
            workflow_type: Filter by workflow type.
            task_queue: Filter by task queue.
            status_filter: Filter by status (e.g., ["RUNNING", "COMPLETED"]).
            sort_order: Sort order ("start_time_asc", "start_time_desc",
                "close_time_asc", "close_time_desc").

        Returns:
            A WorkflowListPage with executions and pagination token.

        Example:
            >>> page = await client.list_workflows(
            ...     workflow_type="order-processing",
            ...     status_filter=["RUNNING"],
            ... )
            >>> for info in page.executions:
            ...     print(info.workflow_id, info.status)
        """
        self._ensure_connected()

        raw = await self._native_client.list_workflows(
            page_size=page_size,
            next_page_token=next_page_token,
            workflow_type=workflow_type,
            task_queue=task_queue,
            status_filter=status_filter,
            sort_order=sort_order,
        )

        return WorkflowListPage._from_dict(raw)

    async def search_workflows(
        self,
        query: str,
        *,
        page_size: int = 100,
        next_page_token: bytes | None = None,
    ) -> WorkflowListPage:
        """Search workflow executions using query syntax.

        Supports SQL-like query strings for advanced filtering.

        Args:
            query: SQL-like query string (e.g.,
                "WorkflowType = 'OrderProcessing' AND Status = 'Running'").
            page_size: Maximum results per page (default 100).
            next_page_token: Pagination token from a previous response.

        Returns:
            A WorkflowListPage with matching executions and pagination token.

        Example:
            >>> page = await client.search_workflows(
            ...     "WorkflowType = 'OrderProcessing' AND Status = 'Running'"
            ... )
            >>> print(f"Found {len(page.executions)} workflows")
        """
        self._ensure_connected()

        raw = await self._native_client.search_workflows(
            query,
            page_size=page_size,
            next_page_token=next_page_token,
        )

        return WorkflowListPage._from_dict(raw)

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        return f"Client(server_url={self._config.server_url!r}, status={status})"
