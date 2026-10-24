"""Workflow handle for interacting with workflow executions."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from orcher._internal.arguments import pack_arguments
from orcher._proto import client_pb2 as pb
from orcher.client import _protocol
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
    Every operation is a client protocol call through the client's
    connection.

    Attributes:
        workflow_id: Unique identifier of the workflow.
        run_id: Unique identifier of this execution run.
    """

    def __init__(self, client: Client, *, workflow_id: str, run_id: str = "") -> None:
        """Initialize a workflow handle.

        Args:
            client: The client used to communicate with the server.
            workflow_id: The workflow's id.
            run_id: The run; empty for the workflow's latest run.
        """
        self._client = client
        self._execution = WorkflowExecution(workflow_id=workflow_id, run_id=run_id)

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

    def _ref(self) -> pb.WorkflowRef:
        return pb.WorkflowRef(workflow_id=self.workflow_id, run_id=self.run_id)

    async def _call(self, message: pb.ClientCall) -> pb.ClientAnswer:
        return await self._client._call(message, workflow_id=self.workflow_id)

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
        wait = pb.WaitForResult(workflow=self._ref())
        if timeout is not None:
            wait.wait.CopyFrom(_protocol.duration(timedelta(seconds=timeout)))
        answer = await self._call(pb.ClientCall(wait_for_result=wait))
        result: T = _protocol.decode_json(answer.result.value)
        return result

    async def status(self) -> WorkflowStatus:
        """Get the current status of the workflow.

        Returns:
            The current workflow status.

        Raises:
            WorkflowError: If the workflow is not found.
            ClientError: If there's a communication error.
        """
        answer = await self._call(pb.ClientCall(get_status=pb.GetStatus(workflow=self._ref())))
        return _protocol.status(answer.status)

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
        answer = await self._call(pb.ClientCall(describe=pb.Describe(workflow=self._ref())))
        return _protocol.description(answer.description)

    async def send_event(self, event_name: str, data: Any = None) -> None:
        """Send an event to the workflow.

        Args:
            event_name: Name of the event to send.
            data: Optional data payload for the event.

        Raises:
            WorkflowError: If the workflow is not found or event fails.
            ClientError: If there's a communication error.
        """
        event = pb.SendEvent(
            workflow=self._ref(),
            event_name=event_name,
            payload=_protocol.encode_json(data),
        )
        await self._call(pb.ClientCall(send_event=event))

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
        query = pb.Query(
            workflow=self._ref(),
            query_name=query_name,
            args=_protocol.encode_json(pack_arguments(args)),
        )
        answer = await self._call(pb.ClientCall(query=query))
        return _protocol.decode_json(answer.value)

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
        update = pb.Update(
            workflow=self._ref(),
            update_name=update_name,
            args=_protocol.encode_json(pack_arguments(args)),
        )
        answer = await self._call(pb.ClientCall(update=update))
        return _protocol.decode_json(answer.value)

    async def cancel(self, cleanup_timeout: timedelta | None = None) -> None:
        """Request workflow cancellation.

        This sends a cancellation request to the workflow. The workflow
        can handle this gracefully or ignore it.

        Example::

            await handle.cancel(cleanup_timeout=timedelta(seconds=30))

        Args:
            cleanup_timeout: How long the workflow may spend cleaning up after
                it observes the cancellation, after which the engine terminates
                it. ``None`` sets no limit. Engines from before cancellation
                cleanup ignore it and cancel at once.

        Raises:
            ValueError: If ``cleanup_timeout`` is negative.
            WorkflowError: If the workflow is not found.
            ClientError: If there's a communication error.
        """
        cancel = pb.Cancel(workflow=self._ref())
        if cleanup_timeout is not None:
            if cleanup_timeout < timedelta(0):
                raise ValueError(f"cleanup_timeout must not be negative, got {cleanup_timeout}")
            cancel.cleanup_timeout.CopyFrom(_protocol.duration(cleanup_timeout))
        await self._call(pb.ClientCall(cancel=cancel))

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
        await self._call(pb.ClientCall(terminate=pb.Terminate(workflow=self._ref(), reason=reason)))

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
        reset = pb.Reset(workflow=self._ref(), event_id=target_event_id, reason=reason)
        answer = await self._call(pb.ClientCall(reset=reset))
        return answer.reset_to.run_id

    def __repr__(self) -> str:
        return f"WorkflowHandle(workflow_id={self.workflow_id!r}, run_id={self.run_id!r})"
