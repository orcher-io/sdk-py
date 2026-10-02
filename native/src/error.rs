//! Python exception types and the mapping from core SDK errors to them.

use pyo3::create_exception;
use pyo3::exceptions::{
    PyConnectionError, PyException, PyRuntimeError, PyTimeoutError, PyValueError,
};
use pyo3::prelude::*;

use orcher_sdk_core::error::Error as CoreError;

// Base class for errors raised by the native module.
create_exception!(_native, NativeError, PyException);

// Kind-specific subclasses of NativeError.
//
// The error kind is carried by the exception type, so callers can tell
// "not found" from "already exists" without parsing the message.
//
// They subclass NativeError so that code catching NativeError also catches
// every specific kind.
create_exception!(_native, WorkflowNotFoundError, NativeError);
create_exception!(_native, WorkflowAlreadyExistsError, NativeError);
create_exception!(_native, WorkflowFailedError, NativeError);
create_exception!(_native, WorkflowCancelledError, NativeError);
create_exception!(_native, WorkflowTerminatedError, NativeError);
create_exception!(_native, TaskFailedError, NativeError);
create_exception!(_native, TaskCancelledError, NativeError);
create_exception!(_native, DeterminismError, NativeError);

/// Convert a core SDK error to the matching Python exception.
pub fn core_error_to_py(error: CoreError) -> PyErr {
    match &error {
        // Connection errors
        CoreError::Connection(msg) => {
            PyConnectionError::new_err(format!("Connection error: {}", msg))
        }
        CoreError::Transport(e) => PyConnectionError::new_err(format!("Transport error: {}", e)),

        // Timeout errors
        CoreError::Timeout { operation, .. } => {
            PyTimeoutError::new_err(format!("Operation timed out: {}", operation))
        }

        // Configuration errors
        CoreError::Configuration(msg) => {
            PyValueError::new_err(format!("Configuration error: {}", msg))
        }

        // Workflow errors
        CoreError::WorkflowNotFound { workflow_id, .. } => {
            WorkflowNotFoundError::new_err(format!("Workflow not found: {}", workflow_id))
        }
        CoreError::WorkflowAlreadyExists {
            workflow_id,
            run_id,
            ..
        } => {
            let err = WorkflowAlreadyExistsError::new_err(format!(
                "Workflow already exists: {}",
                workflow_id
            ));
            // Carried as attributes so the Python layer can name the
            // conflicting run without parsing the message.
            Python::with_gil(|py| {
                let value = err.value(py);
                let _ = value.setattr("workflow_id", workflow_id.clone());
                let _ = value.setattr("run_id", run_id.clone());
            });
            err
        }
        CoreError::WorkflowExecutionFailed { message, .. } => {
            WorkflowFailedError::new_err(format!("Workflow execution failed: {}", message))
        }
        CoreError::WorkflowCancelled { workflow_id, .. } => {
            WorkflowCancelledError::new_err(format!("Workflow was cancelled: {}", workflow_id))
        }
        CoreError::WorkflowTerminated {
            workflow_id,
            reason,
            ..
        } => {
            let msg = match reason {
                Some(r) => format!("Workflow was terminated: {} - {}", workflow_id, r),
                None => format!("Workflow was terminated: {}", workflow_id),
            };
            WorkflowTerminatedError::new_err(msg)
        }
        CoreError::InvalidWorkflowState {
            expected, actual, ..
        } => NativeError::new_err(format!(
            "Invalid workflow state: expected={}, actual={}",
            expected, actual
        )),

        // Task errors
        CoreError::TaskExecutionFailed {
            task_id, reason, ..
        } => TaskFailedError::new_err(format!("Task execution failed: {} - {}", task_id, reason)),
        CoreError::TaskCancelled { task_id, .. } => {
            TaskCancelledError::new_err(format!("Task was cancelled: {}", task_id))
        }

        // Determinism errors
        CoreError::DeterminismViolation { reason, .. } => {
            DeterminismError::new_err(format!("Determinism violation: {}", reason))
        }

        // Serialization errors
        CoreError::Serialization(msg) => {
            PyValueError::new_err(format!("Serialization error: {}", msg))
        }
        CoreError::Deserialization(msg) => {
            PyValueError::new_err(format!("Deserialization error: {}", msg))
        }
        CoreError::InvalidPayload { reason, .. } => {
            PyValueError::new_err(format!("Invalid payload: {}", reason))
        }

        // Authentication/Authorization
        CoreError::Authentication(msg) => {
            NativeError::new_err(format!("Authentication failed: {}", msg))
        }
        CoreError::Authorization(msg) => {
            NativeError::new_err(format!("Authorization failed: {}", msg))
        }

        // Service errors
        CoreError::WorkerError(msg) => NativeError::new_err(format!("Worker error: {}", msg)),
        CoreError::PollerError(msg) => NativeError::new_err(format!("Poller error: {}", msg)),

        // Generic errors
        CoreError::Internal(msg) => PyRuntimeError::new_err(format!("Internal error: {}", msg)),

        // Any other error. There is no GrpcStatus arm: the core SDK maps
        // NOT_FOUND, ALREADY_EXISTS and DEADLINE_EXCEEDED onto the semantic
        // variants handled above. Keeping that mapping in one place stops two
        // copies from drifting apart.
        _ => NativeError::new_err(format!("ORCHER error: {}", error)),
    }
}

/// Result type for operations that fail with a Python exception.
pub type PyNativeResult<T> = Result<T, PyErr>;

/// Converts a core SDK `Result` into a `PyResult`.
pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyNativeResult<T>;
}

impl<T> IntoPyResult<T> for Result<T, CoreError> {
    fn into_py_result(self) -> PyNativeResult<T> {
        self.map_err(core_error_to_py)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_conversion() {
        let error = CoreError::workflow_not_found("wf-123");
        let py_err = core_error_to_py(error);
        assert!(py_err.to_string().contains("Workflow not found"));
    }
}
