"""Workflow-related error types."""

from typing import Any

from orcher.errors.base import OrcherError
from orcher.errors.codes import ErrorCode, ErrorSeverity

__all__ = [
    "WorkflowError",
    "WorkflowSuspendedError",
]


class WorkflowError(OrcherError):
    """Errors related to workflow execution.

    Attributes:
        workflow_id: ID of the workflow that encountered the error.
        run_id: Run ID of the workflow execution.
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.workflow_id = workflow_id
        self.run_id = run_id
        super().__init__(code, message, **kwargs)

    @classmethod
    def not_found(cls, workflow_id: str, run_id: str | None = None) -> "WorkflowError":
        """Create a workflow not found error."""
        msg = f"Workflow not found: {workflow_id}"
        if run_id:
            msg += f" (run_id={run_id})"
        return cls(
            ErrorCode.WORKFLOW_NOT_FOUND,
            msg,
            workflow_id=workflow_id,
            run_id=run_id,
        )

    @classmethod
    def already_exists(cls, workflow_id: str, run_id: str | None = None) -> "WorkflowError":
        """Create a workflow already exists error.

        ``run_id`` names the run that already holds the workflow id. A caller
        that meant to start the workflow only once can wait on that run.
        """
        msg = f"Workflow already exists: {workflow_id}"
        if run_id:
            msg += f" (run_id={run_id})"
        return cls(
            ErrorCode.WORKFLOW_ALREADY_EXISTS,
            msg,
            workflow_id=workflow_id,
            run_id=run_id,
        )

    @classmethod
    def execution_failed(
        cls, workflow_id: str, message: str, *, run_id: str | None = None
    ) -> "WorkflowError":
        """Create a workflow execution failed error."""
        return cls(
            ErrorCode.WORKFLOW_EXECUTION_FAILED,
            message,
            workflow_id=workflow_id,
            run_id=run_id,
        )

    @classmethod
    def cancelled(cls, workflow_id: str) -> "WorkflowError":
        """Create a workflow cancelled error."""
        return cls(
            ErrorCode.WORKFLOW_CANCELLED,
            f"Workflow was cancelled: {workflow_id}",
            workflow_id=workflow_id,
        )

    @classmethod
    def terminated(cls, workflow_id: str, reason: str | None = None) -> "WorkflowError":
        """Create a workflow terminated error."""
        msg = f"Workflow was terminated: {workflow_id}"
        if reason:
            msg += f" - {reason}"
        return cls(
            ErrorCode.WORKFLOW_TERMINATED,
            msg,
            workflow_id=workflow_id,
        )

    @classmethod
    def non_deterministic(cls, workflow_id: str, reason: str) -> "WorkflowError":
        """Create a non-determinism violation error."""
        return cls(
            ErrorCode.WORKFLOW_NON_DETERMINISTIC,
            f"Non-determinism violation in workflow {workflow_id}: {reason}",
            workflow_id=workflow_id,
            severity=ErrorSeverity.CRITICAL,
        )


class WorkflowSuspendedError(BaseException):
    """Signal that a workflow is suspended until an async operation completes.

    This is control flow, not an error. When a workflow calls
    `ctx.execute_task()`, `ctx.sleep()`, or another async operation whose
    result is not yet available, this exception tells the executor to suspend
    the workflow.

    The executor catches it, persists the workflow state and pending commands,
    and resumes the workflow when the operation completes.

    It inherits from BaseException, not Exception, so `except Exception:`
    handlers in workflow code do not catch it and the suspension always
    reaches the executor.

    Attributes:
        reason: Description of what the workflow is waiting for.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"Workflow suspended: {reason}")
