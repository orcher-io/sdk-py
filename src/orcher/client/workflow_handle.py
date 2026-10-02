"""Workflow handle for interacting with workflow executions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from orcher.errors._translate import translates_native_errors
from orcher.types import WorkflowExecution, WorkflowExecutionDescription, WorkflowStatus

if TYPE_CHECKING:
    from orcher.client.client import Client

__all__ = [
    "WorkflowHandle",
]

T = TypeVar("T")


class WorkflowHandle(Generic[T]):
    """Handle to a running or completed workflow.

    Provides methods to interact with a specific workflow execution,
    including waiting for results, sending events, and querying state.
    Every operation goes through the native Rust module over gRPC.

    Attributes:
        workflow_id: Unique identifier of the workflow.
        run_id: Unique identifier of this execution run.
    """

    def __init__(
        self,
        client: Client,
        execution: WorkflowExecution,
        *,
        _native_handle: Any = None,
    ) -> None:
        """Initialize a workflow handle.

        Args:
            client: The client used to communicate with the server.
            execution: The workflow execution identifier.
            _native_handle: Internal native handle (use _from_native instead).
        """
        self._client = client
        self._execution = execution
        self._native_handle = _native_handle

    @classmethod
    def _from_native(cls, client: Client, native_handle: Any) -> WorkflowHandle[Any]:
        """Create a WorkflowHandle from a native handle.

        Args:
            client: The client instance.
            native_handle: The native _native.WorkflowHandle from Rust.

        Returns:
            A WorkflowHandle wrapping the native handle.
        """
        execution = WorkflowExecution(
            workflow_id=native_handle.workflow_id,
            run_id=native_handle.run_id,
        )
        return cls(client, execution, _native_handle=native_handle)

    @property
    def workflow_id(self) -> str:
        """Get the workflow ID."""
        return self._execution.workflow_id

    @property
    def run_id(self) -> str:
        """Get the run ID."""
        return self._execution.run_id

    @property
    def execution(self) -> WorkflowExecution:
        """Get the workflow execution identifier."""
        return self._execution

    def _ensure_native(self) -> Any:
        """Get the native handle, raising if not available."""
        if self._native_handle is None:
            raise RuntimeError(
                "WorkflowHandle has no native handle. "
                "Use Client.start_workflow() or Client.get_workflow() to create handles."
            )
        return self._native_handle

    @translates_native_errors
    async def result(self, timeout: float | None = None) -> T:
        """Wait for and return the workflow result.

        This method blocks until the workflow completes (successfully or with error).

        Args:
            timeout: Optional timeout in seconds. If None, uses the client default.

        Returns:
            The workflow result.

        Raises:
            WorkflowError: If the workflow fails, is cancelled, or times out.
            ClientError: If there's a communication error.
        """
        native = self._ensure_native()
        timeout_ms = int(timeout * 1000) if timeout is not None else None
        return await native.result(timeout_ms)

    @translates_native_errors
    async def status(self) -> WorkflowStatus:
        """Get the current status of the workflow.

        Returns:
            The current workflow status.

        Raises:
            WorkflowError: If the workflow is not found.
            ClientError: If there's a communication error.
        """
        native = self._ensure_native()
        # Convert to the Python enum so the result compares equal to
        # WorkflowStatus members such as WorkflowStatus.COMPLETED.
        return WorkflowStatus._from_native(await native.status())

    @translates_native_errors
    async def describe(self) -> WorkflowExecutionDescription:
        """Describe the workflow execution.

        Returns comprehensive metadata about this workflow execution,
        including status, timing, pending tasks/timers/events, and configuration.

        Returns:
            Detailed workflow execution description.

        Raises:
            WorkflowError: If the workflow is not found.
            ClientError: If there's a communication error.
        """
        native = self._ensure_native()
        raw = await native.describe()
        return WorkflowExecutionDescription._from_dict(raw)

    @translates_native_errors
    async def send_event(self, event_name: str, data: Any = None) -> None:
        """Send an event to the workflow.

        Args:
            event_name: Name of the event to send.
            data: Optional data payload for the event.

        Raises:
            WorkflowError: If the workflow is not found or event fails.
            ClientError: If there's a communication error.
        """
        native = self._ensure_native()
        await native.send_event(event_name, data)

    @translates_native_errors
    async def query(self, query_name: str, *args: Any) -> Any:
        """Query the workflow state.

        Args:
            query_name: Name of the query handler to invoke.
            *args: Arguments to pass to the query handler.

        Returns:
            The query result.

        Raises:
            WorkflowError: If the workflow is not found or query fails.
            ClientError: If there's a communication error.
        """
        native = self._ensure_native()
        query_args = args[0] if len(args) == 1 else (args if args else None)
        return await native.query(query_name, query_args)

    @translates_native_errors
    async def update(self, update_name: str, *args: Any) -> Any:
        """Send an update to the workflow.

        The update handler can read and change workflow state, and the caller
        waits for its result. The result is recorded in the journal, so replay
        reproduces it.

        Args:
            update_name: Name of the update handler to invoke.
            *args: Arguments to pass to the update handler.

        Returns:
            The update result.

        Raises:
            WorkflowError: If the workflow is not found or update fails.
            ClientError: If there's a communication error.
        """
        native = self._ensure_native()
        update_args = args[0] if len(args) == 1 else (args if args else None)
        return await native.update(update_name, update_args)

    @translates_native_errors
    async def cancel(self) -> None:
        """Request workflow cancellation.

        This sends a cancellation request to the workflow. The workflow
        can handle this gracefully or ignore it.

        Raises:
            WorkflowError: If the workflow is not found.
            ClientError: If there's a communication error.
        """
        native = self._ensure_native()
        await native.cancel()

    @translates_native_errors
    async def terminate(self, reason: str = "") -> None:
        """Forcefully terminate the workflow.

        Unlike cancel(), this immediately terminates the workflow without
        giving it a chance to clean up.

        Args:
            reason: Optional reason for termination.

        Raises:
            WorkflowError: If the workflow is not found.
            ClientError: If there's a communication error.
        """
        native = self._ensure_native()
        await native.terminate(reason)

    @translates_native_errors
    async def reset(self, target_event_id: int, reason: str = "") -> str:
        """Reset the workflow to a specific journal point and re-execute from there.

        Creates a new execution that replays the journal up to ``target_event_id``,
        then makes fresh decisions from that point. The current execution is
        marked with terminal status "reset".

        Args:
            target_event_id: Journal event_id to reset to (events up to and
                including this are preserved).
            reason: Human-readable reason for the reset.

        Returns:
            The new execution ID created by the reset.

        Raises:
            WorkflowError: If the workflow is not found or event_id is invalid.
            ClientError: If there's a communication error.
        """
        native = self._ensure_native()
        return await native.reset_workflow(target_event_id, reason)

    def __repr__(self) -> str:
        return f"WorkflowHandle(workflow_id={self.workflow_id!r}, run_id={self.run_id!r})"
