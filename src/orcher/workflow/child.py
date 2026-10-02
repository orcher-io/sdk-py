"""Child workflow handle for the Orcher Python SDK.

This module provides the ChildWorkflowHandle class for interacting
with child workflows started from a parent workflow.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from orcher.workflow.context import WorkflowContext

__all__ = ["ChildWorkflowHandle"]

T = TypeVar("T")


class ChildWorkflowHandle(Generic[T]):
    """Handle to a child workflow.

    Provides methods to interact with a child workflow started from
    a parent workflow.
    """

    def __init__(
        self,
        workflow_id: str,
        run_id: str,
        workflow_type: str,
        context: WorkflowContext | None = None,
    ) -> None:
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._workflow_type = workflow_type
        self._context = context

    @property
    def workflow_id(self) -> str:
        """Get the child workflow ID."""
        return self._workflow_id

    @property
    def run_id(self) -> str:
        """Get the child workflow run ID."""
        return self._run_id

    async def result(self) -> T:
        """Wait for the child workflow to end and return its result.

        Raises:
            WorkflowError: If the child failed, or ended without a result:
                canceled, terminated or timed out.
        """
        if self._context is None:
            raise NotImplementedError("Child workflow result requires execution context")
        return self._context._resolve_child(self._workflow_id)  # type: ignore[no-any-return]

    async def send_event(self, event_name: str, data: Any = None) -> None:
        """Send an event to the child workflow.

        The child receives it with ``ctx.wait_for_event()``. The send is
        journaled as a step of this workflow, so a replay does not send it
        again.
        """
        if self._context is None:
            raise NotImplementedError("Child workflow events require execution context")
        await self._context.send_event(self._workflow_id, event_name, data)

    async def cancel(self) -> None:
        """Request cancellation of the child workflow.

        The child is canceled the way a client cancels a workflow, and
        :meth:`result` then raises ``WorkflowError``. Like :meth:`send_event`,
        the request is journaled as a step and not repeated on replay;
        canceling a child that has already finished does nothing.
        """
        if self._context is None:
            raise NotImplementedError("Canceling a child workflow requires execution context")
        await self._context.cancel_child(self._workflow_id)

    def __repr__(self) -> str:
        return (
            f"ChildWorkflowHandle(workflow_id={self._workflow_id!r}, type={self._workflow_type!r})"
        )
