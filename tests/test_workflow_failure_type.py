"""What a failed workflow tells the core about its error.

The core fails an execution whose SDK reports ``NonDeterminism`` as a
non-retryable ``NonDeterminismError``; anything else is an ordinary workflow
error.
"""

from orcher.errors import WorkflowError
from orcher.worker.worker import _workflow_error_type


def test_non_determinism_is_reported_as_such() -> None:
    error = WorkflowError.non_deterministic("wf-1", "step 3 changed")
    assert _workflow_error_type(error) == "NonDeterminism"


def test_any_other_failure_is_workflow_code() -> None:
    assert _workflow_error_type(WorkflowError.execution_failed("wf-1", "boom")) == "WorkflowCode"
    assert _workflow_error_type(ValueError("boom")) == "WorkflowCode"
