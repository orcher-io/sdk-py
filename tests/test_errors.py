"""Tests for the error handling module."""

from datetime import datetime

import pytest

from orcher.errors import (
    ApplicationFailure,
    CancelledFailure,
    ChildWorkflowFailure,
    ClientError,
    ConfigurationError,
    ErrorChain,
    ErrorCode,
    ErrorContext,
    ErrorSeverity,
    Failure,
    FailureType,
    OrcherError,
    TaskError,
    TaskFailure,
    TerminatedFailure,
    TimeoutFailure,
    WorkflowError,
    WorkflowFailure,
    capture_exception,
    failure_from_exception,
    with_context,
)

# =============================================================================
# Base Error Types Tests
# =============================================================================


class TestErrorCode:
    """Tests for ErrorCode enum."""

    def test_workflow_error_codes(self) -> None:
        """Test workflow error codes are in 1xxx range."""
        workflow_codes = [
            ErrorCode.WORKFLOW_EXECUTION_FAILED,
            ErrorCode.WORKFLOW_TIMEOUT,
            ErrorCode.WORKFLOW_NOT_FOUND,
        ]
        for code in workflow_codes:
            assert 1000 <= code.value < 2000

    def test_task_error_codes(self) -> None:
        """Test task error codes are in 2xxx range."""
        task_codes = [
            ErrorCode.TASK_EXECUTION_FAILED,
            ErrorCode.TASK_TIMEOUT,
            ErrorCode.TASK_CANCELLED,
        ]
        for code in task_codes:
            assert 2000 <= code.value < 3000


class TestOrcherError:
    """Tests for OrcherError base class."""

    def test_create_error(self) -> None:
        """Test creating an error."""
        error = OrcherError(
            ErrorCode.WORKFLOW_EXECUTION_FAILED,
            "Something went wrong",
        )
        assert error.code == ErrorCode.WORKFLOW_EXECUTION_FAILED
        assert error.message == "Something went wrong"
        assert error.severity == ErrorSeverity.ERROR

    def test_error_with_cause(self) -> None:
        """Test creating an error with a cause."""
        cause = ValueError("Original error")
        error = OrcherError(
            ErrorCode.INTERNAL_ERROR,
            "Wrapper error",
            cause=cause,
        )
        assert error.cause is cause

    def test_error_with_details(self) -> None:
        """Test creating an error with details."""
        error = OrcherError(
            ErrorCode.TASK_EXECUTION_FAILED,
            "Task failed",
            details={"task_id": "task-123", "attempt": 3},
        )
        assert error.details["task_id"] == "task-123"
        assert error.details["attempt"] == 3

    def test_is_retryable(self) -> None:
        """Test retryable detection."""
        # Retryable errors
        retryable = OrcherError(ErrorCode.TASK_TIMEOUT, "Timeout")
        assert retryable.is_retryable is True

        # Non-retryable errors
        non_retryable = OrcherError(ErrorCode.WORKFLOW_CANCELLED, "Cancelled")
        assert non_retryable.is_retryable is False

    def test_error_repr(self) -> None:
        """Test error string representation."""
        error = OrcherError(ErrorCode.WORKFLOW_NOT_FOUND, "Not found")
        repr_str = repr(error)
        assert "OrcherError" in repr_str
        assert "WORKFLOW_NOT_FOUND" in repr_str


class TestWorkflowError:
    """Tests for WorkflowError."""

    def test_not_found(self) -> None:
        """Test creating a workflow not found error."""
        error = WorkflowError.not_found("wf-123", "run-456")
        assert error.code == ErrorCode.WORKFLOW_NOT_FOUND
        assert error.workflow_id == "wf-123"
        assert error.run_id == "run-456"
        assert "wf-123" in error.message

    def test_already_exists(self) -> None:
        """Test creating a workflow already exists error."""
        error = WorkflowError.already_exists("wf-123")
        assert error.code == ErrorCode.WORKFLOW_ALREADY_EXISTS
        assert error.workflow_id == "wf-123"

    def test_execution_failed(self) -> None:
        """Test creating a workflow execution failed error."""
        error = WorkflowError.execution_failed(
            "wf-123",
            "Processing failed",
            run_id="run-456",
        )
        assert error.code == ErrorCode.WORKFLOW_EXECUTION_FAILED
        assert error.workflow_id == "wf-123"

    def test_non_deterministic(self) -> None:
        """Test creating a non-determinism error."""
        error = WorkflowError.non_deterministic(
            "wf-123",
            "Timer mismatch",
        )
        assert error.code == ErrorCode.WORKFLOW_NON_DETERMINISTIC
        assert error.severity == ErrorSeverity.CRITICAL
        assert error.is_retryable is False


class TestTaskError:
    """Tests for TaskError."""

    def test_execution_failed(self) -> None:
        """Test creating a task execution failed error."""
        error = TaskError.execution_failed(
            "task-123",
            "Database connection failed",
            task_type="process_payment",
        )
        assert error.code == ErrorCode.TASK_EXECUTION_FAILED
        assert error.task_id == "task-123"
        assert error.task_type == "process_payment"

    def test_timeout(self) -> None:
        """Test creating a task timeout error."""
        error = TaskError.timeout("task-123", task_type="long_task")
        assert error.code == ErrorCode.TASK_TIMEOUT
        assert error.task_id == "task-123"

    def test_cancelled(self) -> None:
        """Test creating a task cancelled error."""
        error = TaskError.cancelled("task-123")
        assert error.code == ErrorCode.TASK_CANCELLED
        assert error.is_retryable is False


class TestClientError:
    """Tests for ClientError."""

    def test_connection_failed(self) -> None:
        """Test creating a connection failed error."""
        error = ClientError.connection_failed(
            "localhost:50051",
            "Connection refused",
        )
        assert error.code == ErrorCode.CLIENT_CONNECTION_FAILED
        assert "localhost:50051" in error.message

    def test_unauthorized(self) -> None:
        """Test creating an unauthorized error."""
        error = ClientError.unauthorized("Invalid token")
        assert error.code == ErrorCode.CLIENT_UNAUTHORIZED
        assert error.is_retryable is False


class TestConfigurationError:
    """Tests for ConfigurationError."""

    def test_missing_required(self) -> None:
        """Test creating a missing required field error."""
        error = ConfigurationError.missing_required("server_url")
        assert error.code == ErrorCode.CONFIGURATION_INVALID
        assert "server_url" in error.message

    def test_invalid_value(self) -> None:
        """Test creating an invalid value error."""
        error = ConfigurationError.invalid_value(
            "timeout_ms",
            -100,
            "must be positive",
        )
        assert "timeout_ms" in error.message
        assert "-100" in error.message


# =============================================================================
# Error Context Tests
# =============================================================================


class TestErrorContext:
    """Tests for ErrorContext."""

    def test_create_context(self) -> None:
        """Test creating an error context."""
        ctx = ErrorContext(
            message="Failed to process order",
            operation="process_order",
            workflow_id="wf-123",
        )
        assert ctx.message == "Failed to process order"
        assert ctx.operation == "process_order"
        assert ctx.workflow_id == "wf-123"

    def test_fluent_interface(self) -> None:
        """Test fluent interface for building context."""
        ctx = (
            ErrorContext("Error")
            .with_operation("my_op")
            .with_component("my_component")
            .with_workflow("wf-123")
            .with_task("task-456")
            .with_metadata("key", "value")
        )
        assert ctx.operation == "my_op"
        assert ctx.component == "my_component"
        assert ctx.workflow_id == "wf-123"
        assert ctx.task_id == "task-456"
        assert ctx.metadata["key"] == "value"

    def test_format(self) -> None:
        """Test context formatting."""
        ctx = ErrorContext(
            message="Test error",
            operation="test_op",
            workflow_id="wf-123",
        )
        formatted = ctx.format()
        assert "Test error" in formatted
        assert "test_op" in formatted
        assert "wf-123" in formatted


class TestErrorChain:
    """Tests for ErrorChain."""

    def test_empty_chain(self) -> None:
        """Test empty error chain."""
        chain = ErrorChain()
        assert chain.is_empty is True
        assert chain.count == 0
        assert chain.first is None
        assert chain.last is None

    def test_add_errors(self) -> None:
        """Test adding errors to chain."""
        chain = ErrorChain()
        chain.add(ValueError("Error 1"), context="First error")
        chain.add(KeyError("Error 2"), context="Second error")

        assert chain.count == 2
        assert chain.is_empty is False
        assert isinstance(chain.first, ValueError)
        assert isinstance(chain.last, KeyError)

    def test_raise_if_any(self) -> None:
        """Test raise_if_any method."""
        chain = ErrorChain()
        chain.add(ValueError("Error"))

        with pytest.raises(Exception) as exc_info:
            chain.raise_if_any("Multiple errors")

        assert "Multiple errors" in str(exc_info.value)
        assert "1 error" in str(exc_info.value)

    def test_raise_if_any_empty(self) -> None:
        """Test raise_if_any with empty chain."""
        chain = ErrorChain()
        chain.raise_if_any()  # Should not raise

    def test_iteration(self) -> None:
        """Test iterating over error chain."""
        chain = ErrorChain()
        chain.add(ValueError("Error 1"))
        chain.add(KeyError("Error 2"))

        errors = list(chain)
        assert len(errors) == 2


class TestWithContext:
    """Tests for with_context helper."""

    def test_with_context(self) -> None:
        """Test with_context helper function."""
        ctx = with_context(
            "Failed to process",
            operation="process",
            workflow_id="wf-123",
            custom_field="value",
        )
        assert ctx.message == "Failed to process"
        assert ctx.operation == "process"
        assert ctx.workflow_id == "wf-123"
        assert ctx.metadata["custom_field"] == "value"


class TestCaptureException:
    """Tests for capture_exception."""

    def test_capture_exception(self) -> None:
        """Test capturing an exception with context."""
        try:
            raise ValueError("Test error")
        except Exception:
            exc, ctx = capture_exception("During test", extra="info")

        assert isinstance(exc, ValueError)
        assert ctx is not None
        assert ctx.message == "During test"
        assert ctx.metadata["extra"] == "info"
        assert ctx.stack_trace is not None

    def test_capture_no_exception(self) -> None:
        """Test capture_exception when no exception."""
        exc, ctx = capture_exception("No error")
        assert exc is None
        assert ctx is None


# =============================================================================
# Failure Types Tests
# =============================================================================


class TestFailure:
    """Tests for base Failure class."""

    def test_create_failure(self) -> None:
        """Test creating a failure."""
        failure = Failure(
            message="Something went wrong",
            failure_type=FailureType.APPLICATION,
            source="my_service",
        )
        assert failure.message == "Something went wrong"
        assert failure.failure_type == FailureType.APPLICATION
        assert failure.source == "my_service"
        assert failure.retryable is True

    def test_failure_chain(self) -> None:
        """Test failure cause chaining."""
        root = Failure("Root cause", failure_type=FailureType.APPLICATION)
        middle = Failure("Middle", failure_type=FailureType.TASK).with_cause(root)
        top = Failure("Top level", failure_type=FailureType.WORKFLOW).with_cause(middle)

        assert top.root_cause is root
        assert len(top.failure_chain) == 3

    def test_to_dict(self) -> None:
        """Test failure serialization."""
        failure = Failure(
            message="Test",
            failure_type=FailureType.APPLICATION,
            source="test",
            retryable=False,
        )
        data = failure.to_dict()

        assert data["message"] == "Test"
        assert data["failure_type"] == "APPLICATION"
        assert data["retryable"] is False

    def test_from_dict(self) -> None:
        """Test failure deserialization."""
        data = {
            "message": "Test",
            "failure_type": "TASK",
            "source": "test_task",
            "retryable": True,
            "details": {"key": "value"},
            "timestamp": datetime.now().isoformat(),
        }
        failure = Failure.from_dict(data)

        assert failure.message == "Test"
        assert failure.failure_type == FailureType.TASK
        assert failure.source == "test_task"

    def test_non_retryable(self) -> None:
        """Test marking failure as non-retryable."""
        failure = Failure("Error").non_retryable()
        assert failure.retryable is False


class TestApplicationFailure:
    """Tests for ApplicationFailure."""

    def test_create_application_failure(self) -> None:
        """Test creating an application failure."""
        failure = ApplicationFailure(
            message="Insufficient funds",
            error_type="InsufficientFundsError",
        )
        assert failure.failure_type == FailureType.APPLICATION
        assert failure.error_type == "InsufficientFundsError"

    def test_from_error(self) -> None:
        """Test creating from error message."""
        failure = ApplicationFailure.from_error(
            "Invalid input",
            error_type="ValidationError",
            retryable=False,
            field="email",
        )
        assert failure.message == "Invalid input"
        assert failure.error_type == "ValidationError"
        assert failure.retryable is False
        assert failure.details["field"] == "email"


class TestTaskFailure:
    """Tests for TaskFailure."""

    def test_create_task_failure(self) -> None:
        """Test creating a task failure."""
        failure = TaskFailure(
            message="Database error",
            task_id="task-123",
            task_type="fetch_user",
            attempt=2,
            max_attempts=3,
        )
        assert failure.failure_type == FailureType.TASK
        assert failure.task_id == "task-123"
        assert failure.is_final_attempt is False

    def test_from_exception(self) -> None:
        """Test creating from exception."""
        try:
            raise ValueError("Test error")
        except Exception as e:
            failure = TaskFailure.from_exception(
                e,
                task_id="task-123",
                task_type="my_task",
                attempt=3,
                max_attempts=3,
            )

        assert "Test error" in failure.message
        assert failure.task_id == "task-123"
        assert failure.is_final_attempt is True
        assert failure.stack_trace != ""


class TestWorkflowFailure:
    """Tests for WorkflowFailure."""

    def test_create_workflow_failure(self) -> None:
        """Test creating a workflow failure."""
        failure = WorkflowFailure(
            message="Workflow failed",
            workflow_id="wf-123",
            workflow_type="OrderWorkflow",
            run_id="run-456",
        )
        assert failure.failure_type == FailureType.WORKFLOW
        assert failure.workflow_id == "wf-123"
        assert failure.retryable is False


class TestTimeoutFailure:
    """Tests for TimeoutFailure."""

    def test_task_timeout(self) -> None:
        """Test creating a task timeout."""
        failure = TimeoutFailure.task_timeout(
            task_id="task-123",
            task_type="slow_task",
            timeout_type="start_to_close",
            timeout_duration_ms=30000,
        )
        assert failure.failure_type == FailureType.TIMEOUT
        assert failure.timeout_type == "start_to_close"
        assert failure.retryable is True

    def test_workflow_timeout(self) -> None:
        """Test creating a workflow timeout."""
        failure = TimeoutFailure.workflow_timeout(
            workflow_id="wf-123",
            workflow_type="LongWorkflow",
            timeout_type="execution",
            timeout_duration_ms=3600000,
        )
        assert failure.failure_type == FailureType.TIMEOUT
        assert failure.retryable is False


class TestCancelledFailure:
    """Tests for CancelledFailure."""

    def test_task_cancelled(self) -> None:
        """Test creating a task cancellation."""
        failure = CancelledFailure.task_cancelled("task-123", "User requested")
        assert failure.failure_type == FailureType.CANCELLED
        assert failure.retryable is False
        assert "User requested" in failure.message

    def test_workflow_cancelled(self) -> None:
        """Test creating a workflow cancellation."""
        failure = CancelledFailure.workflow_cancelled("wf-123")
        assert failure.failure_type == FailureType.CANCELLED


class TestTerminatedFailure:
    """Tests for TerminatedFailure."""

    def test_workflow_terminated(self) -> None:
        """Test creating a workflow termination."""
        failure = TerminatedFailure.workflow_terminated(
            "wf-123",
            "Admin termination",
        )
        assert failure.failure_type == FailureType.TERMINATED
        assert failure.retryable is False
        assert failure.reason == "Admin termination"


class TestChildWorkflowFailure:
    """Tests for ChildWorkflowFailure."""

    def test_from_child_failure(self) -> None:
        """Test creating from a child's failure."""
        child_failure = WorkflowFailure(
            message="Child failed",
            workflow_id="child-123",
            workflow_type="ChildWorkflow",
        )

        failure = ChildWorkflowFailure.from_child_failure(
            child_workflow_id="child-123",
            child_workflow_type="ChildWorkflow",
            child_run_id="run-456",
            cause=child_failure,
        )

        assert failure.failure_type == FailureType.CHILD_WORKFLOW
        assert failure.child_workflow_id == "child-123"
        assert failure.cause is child_failure


class TestFailureFromException:
    """Tests for failure_from_exception function."""

    def test_from_exception(self) -> None:
        """Test converting exception to failure."""
        try:
            raise ValueError("Test error")
        except Exception as e:
            failure = failure_from_exception(e, source="test")

        assert "Test error" in failure.message
        assert failure.source == "test"
        assert failure.details["exception_type"] == "ValueError"

    def test_non_retryable_exception_types(self) -> None:
        """Test that certain exception types are marked non-retryable."""
        failure = failure_from_exception(TypeError("Bad type"))
        assert failure.retryable is False

        failure = failure_from_exception(KeyError("Missing key"))
        assert failure.retryable is False

    def test_already_failure(self) -> None:
        """Test converting a Failure returns it unchanged."""
        original = Failure("Original", failure_type=FailureType.APPLICATION)
        result = failure_from_exception(original)
        assert result is original
