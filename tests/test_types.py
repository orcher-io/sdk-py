"""Tests for ORCHER core types."""

from datetime import timedelta

import pytest


class TestWorkflowExecution:
    """Tests for WorkflowExecution dataclass."""

    def test_creation(self) -> None:
        """Test creating a workflow execution."""
        from orcher.types import WorkflowExecution

        execution = WorkflowExecution(
            workflow_id="test-workflow",
            run_id="test-run",
        )
        assert execution.workflow_id == "test-workflow"
        assert execution.run_id == "test-run"

    def test_str_representation(self) -> None:
        """Test string representation."""
        from orcher.types import WorkflowExecution

        execution = WorkflowExecution(
            workflow_id="my-workflow",
            run_id="run-123",
        )
        assert str(execution) == "my-workflow:run-123"

    def test_immutable(self) -> None:
        """Test that WorkflowExecution is immutable (frozen)."""
        from orcher.types import WorkflowExecution

        execution = WorkflowExecution(
            workflow_id="test",
            run_id="run",
        )
        with pytest.raises(AttributeError):
            execution.workflow_id = "changed"  # type: ignore

    def test_equality(self) -> None:
        """Test equality comparison."""
        from orcher.types import WorkflowExecution

        exec1 = WorkflowExecution("wf", "run")
        exec2 = WorkflowExecution("wf", "run")
        exec3 = WorkflowExecution("wf", "different")

        assert exec1 == exec2
        assert exec1 != exec3


class TestWorkflowStatus:
    """Tests for WorkflowStatus enum."""

    def test_terminal_states(self) -> None:
        """Test identifying terminal states."""
        from orcher.types import WorkflowStatus

        terminal = [
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
            WorkflowStatus.TERMINATED,
            WorkflowStatus.TIMED_OUT,
            WorkflowStatus.RESTARTED_FRESH,
        ]
        for status in terminal:
            assert status.is_terminal(), f"{status} should be terminal"

    def test_running_state(self) -> None:
        """Test RUNNING is not terminal."""
        from orcher.types import WorkflowStatus

        assert not WorkflowStatus.RUNNING.is_terminal()
        assert WorkflowStatus.RUNNING.is_running()

    def test_non_running_states(self) -> None:
        """Test non-running states."""
        from orcher.types import WorkflowStatus

        assert not WorkflowStatus.COMPLETED.is_running()
        assert not WorkflowStatus.FAILED.is_running()


class TestPayload:
    """Tests for Payload dataclass."""

    def test_empty_payload(self) -> None:
        """Test creating an empty payload."""
        from orcher.types import Payload

        payload = Payload()
        assert payload.is_empty()
        assert len(payload) == 0

    def test_from_json(self) -> None:
        """Test creating payload from JSON."""
        from orcher.types import Payload

        data = {"name": "test", "value": 42}
        payload = Payload.from_json(data)

        assert not payload.is_empty()
        assert payload.metadata.get("encoding") == b"json"
        assert payload.to_json() == data

    def test_from_string(self) -> None:
        """Test creating payload from string."""
        from orcher.types import Payload

        payload = Payload.from_string("hello world")
        assert payload.to_string() == "hello world"
        assert payload.metadata.get("encoding") == b"utf-8"

    def test_payload_length(self) -> None:
        """Test payload length."""
        from orcher.types import Payload

        payload = Payload.from_string("hello")
        assert len(payload) == 5


class TestRetryPolicy:
    """Tests for RetryPolicy dataclass."""

    def test_default_values(self) -> None:
        """Test default retry policy values."""
        from orcher.types import RetryPolicy

        policy = RetryPolicy.default()
        assert policy.max_attempts == 3
        assert policy.initial_interval == timedelta(seconds=1)
        assert policy.max_interval == timedelta(seconds=60)
        assert policy.backoff_coefficient == 2.0
        assert policy.non_retryable_error_types == []

    def test_no_retry_policy(self) -> None:
        """Test no-retry policy."""
        from orcher.types import RetryPolicy

        policy = RetryPolicy.no_retry()
        assert policy.max_attempts == 1

    def test_custom_policy(self) -> None:
        """Test custom retry policy."""
        from orcher.types import RetryPolicy

        policy = RetryPolicy(
            max_attempts=5,
            initial_interval=timedelta(milliseconds=500),
            max_interval=timedelta(seconds=30),
            backoff_coefficient=1.5,
            non_retryable_error_types=["ValueError"],
        )
        assert policy.max_attempts == 5
        assert policy.backoff_coefficient == 1.5
        assert "ValueError" in policy.non_retryable_error_types


# `orcher.Failure` (orcher.errors.failure.Failure) is the single canonical failure
# type; it is covered by test_errors.py.
