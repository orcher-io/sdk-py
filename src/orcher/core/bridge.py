"""Type bridge utilities for converting between Python and native types.

This module provides functions to convert between Python SDK types and
the native Rust types exposed via PyO3.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcher.types import Payload, RetryPolicy

__all__ = [
    "to_native_payload",
    "from_native_payload",
    "to_native_retry_policy",
    "from_native_retry_policy",
    "to_native_workflow_execution",
    "from_native_workflow_execution",
]


def to_native_payload(payload: Payload) -> Any:
    """Convert a Python Payload to native PyPayload.

    Args:
        payload: The Python Payload object.

    Returns:
        The native PyPayload object.
    """
    from orcher.core.native import get_native_module

    native = get_native_module()

    return native.PyPayload(
        data=payload.data,
        metadata=payload.metadata,
    )


def from_native_payload(native_payload: Any) -> Payload:
    """Convert a native PyPayload to Python Payload.

    Args:
        native_payload: The native PyPayload object.

    Returns:
        The Python Payload object.
    """
    from orcher.types import Payload

    return Payload(
        data=native_payload.data,
        metadata=native_payload.metadata,
    )


def to_native_retry_policy(policy: RetryPolicy) -> Any:
    """Convert a Python RetryPolicy to native PyRetryPolicy.

    Args:
        policy: The Python RetryPolicy object.

    Returns:
        The native PyRetryPolicy object.
    """
    from orcher.core.native import get_native_module

    native = get_native_module()

    return native.PyRetryPolicy(
        initial_interval_ms=int(policy.initial_interval.total_seconds() * 1000)
        if policy.initial_interval
        else None,
        backoff_coefficient=policy.backoff_coefficient,
        maximum_interval_ms=int(policy.max_interval.total_seconds() * 1000)
        if policy.max_interval
        else None,
        maximum_attempts=policy.max_attempts,
        non_retryable_error_types=policy.non_retryable_error_types,
    )


def from_native_retry_policy(native_policy: Any) -> RetryPolicy:
    """Convert a native PyRetryPolicy to Python RetryPolicy.

    Args:
        native_policy: The native PyRetryPolicy object.

    Returns:
        The Python RetryPolicy object.
    """
    from datetime import timedelta

    from orcher.types import RetryPolicy

    return RetryPolicy(
        initial_interval=timedelta(milliseconds=native_policy.initial_interval_ms)
        if native_policy.initial_interval_ms
        else timedelta(seconds=1),
        backoff_coefficient=native_policy.backoff_coefficient,
        max_interval=timedelta(milliseconds=native_policy.maximum_interval_ms)
        if native_policy.maximum_interval_ms
        else timedelta(seconds=60),
        max_attempts=native_policy.maximum_attempts,
        non_retryable_error_types=native_policy.non_retryable_error_types,
    )


def to_native_workflow_execution(
    workflow_id: str,
    run_id: str | None = None,
) -> Any:
    """Convert workflow execution identifiers to native PyWorkflowExecution.

    Args:
        workflow_id: The workflow ID.
        run_id: Optional run ID.

    Returns:
        The native PyWorkflowExecution object.
    """
    from orcher.core.native import get_native_module

    native = get_native_module()

    return native.PyWorkflowExecution(
        workflow_id=workflow_id,
        run_id=run_id,
    )


def from_native_workflow_execution(native_execution: Any) -> dict[str, Any]:
    """Convert a native PyWorkflowExecution to a dictionary.

    Args:
        native_execution: The native PyWorkflowExecution object.

    Returns:
        Dictionary with workflow_id and run_id.
    """
    return {
        "workflow_id": native_execution.workflow_id,
        "run_id": native_execution.run_id,
    }
