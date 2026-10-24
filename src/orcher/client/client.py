"""Client implementation for ORCHER Python SDK."""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from orcher._internal.arguments import pack_arguments
from orcher._proto import client_pb2 as pb
from orcher.client import _protocol
from orcher.client.config import ClientConfig
from orcher.client.workflow_handle import WorkflowHandle
from orcher.errors import ClientError, WorkflowError
from orcher.types import RetryPolicy, WorkflowIdReusePolicy, WorkflowListPage, WorkflowStatus

__all__ = [
    "Client",
]


#: The attempts a retry policy allows when a dict leaves ``max_attempts`` out,
#: the first one included: the SDK's documented ``RetryPolicy`` default.
DEFAULT_MAX_ATTEMPTS = 3


def _retry_policy(rp: RetryPolicy | dict[str, Any]) -> pb.RetryPolicy:
    """A workflow retry policy as the client protocol carries it.

    Accepts a ``RetryPolicy`` or a dict with millisecond intervals
    (``initial_interval_ms``, ``max_interval_ms``). A dict that leaves
    ``max_attempts`` out allows :data:`DEFAULT_MAX_ATTEMPTS`; 0 is unlimited.
    """
    if isinstance(rp, RetryPolicy):
        return pb.RetryPolicy(
            max_attempts=max(rp.max_attempts, 0),
            initial_interval=_protocol.duration(rp.initial_interval),
            max_interval=_protocol.duration(rp.max_interval),
            backoff_coefficient=rp.backoff_coefficient,
            non_retryable_errors=list(rp.non_retryable_error_types),
        )
    if isinstance(rp, dict):
        policy = pb.RetryPolicy(
            max_attempts=max(int(rp.get("max_attempts", DEFAULT_MAX_ATTEMPTS)), 0),
            backoff_coefficient=float(rp.get("backoff_coefficient", 2.0)),
            non_retryable_errors=list(rp.get("non_retryable_error_types", [])),
        )
        if rp.get("initial_interval_ms") is not None:
            policy.initial_interval.CopyFrom(
                _protocol.duration(timedelta(milliseconds=rp["initial_interval_ms"]))
            )
        if rp.get("max_interval_ms") is not None:
            policy.max_interval.CopyFrom(
                _protocol.duration(timedelta(milliseconds=rp["max_interval_ms"]))
            )
        return policy
    raise TypeError(f"retry_policy must be a RetryPolicy or dict, got {type(rp).__name__}")


_ID_REUSE_POLICIES = {
    WorkflowIdReusePolicy.ALLOW_DUPLICATE: pb.ID_REUSE_POLICY_ALLOW_DUPLICATE,
    WorkflowIdReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY: (
        pb.ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
    ),
    WorkflowIdReusePolicy.REJECT_DUPLICATE: pb.ID_REUSE_POLICY_REJECT_DUPLICATE,
    WorkflowIdReusePolicy.TERMINATE_IF_RUNNING: pb.ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
}

_LIST_ORDERS = {
    "start_time_asc": pb.LIST_ORDER_START_TIME_ASC,
    "start_time_desc": pb.LIST_ORDER_START_TIME_DESC,
    "close_time_asc": pb.LIST_ORDER_CLOSE_TIME_ASC,
    "close_time_desc": pb.LIST_ORDER_CLOSE_TIME_DESC,
}


class Client:
    """Client for interacting with ORCHER server.

    The Client provides methods to start workflows, get handles to existing
    workflows, and manage workflow executions. Every operation is a client
    protocol call through the native module, which talks to the server.

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

            self._native_client = await _native.ClientConnection.connect(native_config)
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

    async def invoke_actor_operation(
        self,
        *,
        actor_name: str,
        key: str,
        operation: str,
        payload: bytes,
        timeout_ms: int | None = None,
    ) -> bytes:
        """Invoke an actor operation and wait for its result.

        The server queues the operation under its concurrency rules, a worker
        that registered the actor runs it, and its result comes back. Most
        code calls this through
        :class:`~orcher.actor.client.ActorInvocationClient`, which encodes the
        input and decodes the result as JSON.

        Args:
            actor_name: The actor type.
            key: Which instance of the actor.
            operation: The operation to run.
            payload: The operation's input, as its JSON encoding.
            timeout_ms: How long to wait for the result; 30 seconds when None.

        Returns:
            The operation's result, as its JSON encoding.

        Raises:
            ClientError: If the client is not connected.
            RuntimeError: If the operation does not succeed.
        """
        self._ensure_connected()
        invoke = pb.InvokeActor(
            actor_name=actor_name,
            key=key,
            operation=operation,
            payload=payload,
        )
        invoke.operation_timeout.CopyFrom(
            _protocol.duration(
                timedelta(milliseconds=timeout_ms if timeout_ms is not None else 30_000)
            )
        )
        try:
            answer = await self._call(pb.ClientCall(invoke_actor=invoke))
        except (ClientError, WorkflowError) as e:
            raise RuntimeError(f"Actor operation failed: {e}") from e
        return bytes(answer.value)

    async def close(self) -> None:
        """Close the client connection.

        This method should be called when done using the client to
        release resources.
        """
        if not self._connected:
            return

        self._native_client = None
        self._connected = False

    async def __aenter__(self) -> Client:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def _call(
        self, message: pb.ClientCall, *, workflow_id: str = "unknown"
    ) -> pb.ClientAnswer:
        """Make one client protocol call; a refusal raises the documented error."""
        self._ensure_connected()
        return await _protocol.call(self._native_client, message, workflow_id=workflow_id)

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
            args: Arguments to pass to the workflow, one to one onto its
                parameters after the context. A single dict argument is
                spread as keyword arguments.
            retry_policy: Optional retry policy for the workflow. When omitted,
                a failed workflow is not retried. When given, a failed workflow
                is retried as a fresh run for up to ``max_attempts`` runs, the
                first one included; ``max_attempts=0`` retries without limit,
                and a dict that leaves ``max_attempts`` out allows 3.
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

        # One argument is sent as itself, several so that the worker passes
        # them one to one as positional arguments (see pack_arguments).
        input_data = pack_arguments(args)

        start = pb.StartWorkflow(
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            task_queue=task_queue,
            input=_protocol.encode_json(input_data),
            cron_schedule=cron_schedule or "",
        )
        if execution_timeout is not None:
            start.execution_timeout.CopyFrom(_protocol.duration(execution_timeout))
        if run_timeout is not None:
            start.run_timeout.CopyFrom(_protocol.duration(run_timeout))
        if task_timeout is not None:
            start.task_timeout.CopyFrom(_protocol.duration(task_timeout))
        if retry_policy is not None:
            start.retry_policy.CopyFrom(_retry_policy(retry_policy))
        if id_reuse_policy is not None:
            start.id_reuse_policy = _ID_REUSE_POLICIES[WorkflowIdReusePolicy(id_reuse_policy)]

        answer = await self._call(pb.ClientCall(start_workflow=start), workflow_id=workflow_id)
        started = answer.started.workflow
        return WorkflowHandle(self, workflow_id=started.workflow_id, run_id=started.run_id)

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
        return WorkflowHandle(self, workflow_id=workflow_id, run_id=run_id or "")

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
        list_call = pb.ListWorkflows(
            page_size=page_size,
            page_token=next_page_token or b"",
            workflow_type=workflow_type or "",
            task_queue=task_queue or "",
            statuses=[_protocol.status_value(WorkflowStatus(s)) for s in status_filter or []],
            order=_LIST_ORDERS.get(sort_order or "", pb.LIST_ORDER_UNSPECIFIED),
        )
        answer = await self._call(pb.ClientCall(list_workflows=list_call))
        return _protocol.list_page(answer.page)

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
        search = pb.SearchWorkflows(
            query=query,
            page_size=page_size,
            page_token=next_page_token or b"",
        )
        answer = await self._call(pb.ClientCall(search_workflows=search))
        return _protocol.list_page(answer.page)

    def __repr__(self) -> str:
        status = "connected" if self._connected else "disconnected"
        return f"Client(server_url={self._config.server_url!r}, status={status})"
