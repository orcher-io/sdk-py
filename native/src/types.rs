//! Python wrappers for the core SDK's data types.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::collections::HashMap;

use orcher_sdk_core::types::{Payload, WorkflowExecution, WorkflowStatus};

/// Identifies one run of a workflow: a workflow id plus a run id.
#[pyclass(name = "WorkflowExecution")]
#[derive(Clone, Debug)]
pub struct PyWorkflowExecution {
    inner: WorkflowExecution,
}

#[pymethods]
impl PyWorkflowExecution {
    #[new]
    #[pyo3(signature = (workflow_id, run_id))]
    fn new(workflow_id: String, run_id: String) -> Self {
        Self {
            inner: WorkflowExecution::new(workflow_id, run_id),
        }
    }

    #[getter]
    fn workflow_id(&self) -> &str {
        &self.inner.workflow_id
    }

    #[getter]
    fn run_id(&self) -> &str {
        &self.inner.run_id
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkflowExecution(workflow_id='{}', run_id='{}')",
            self.inner.workflow_id, self.inner.run_id
        )
    }

    fn __str__(&self) -> String {
        format!("{}:{}", self.inner.workflow_id, self.inner.run_id)
    }

    fn __eq__(&self, other: &PyWorkflowExecution) -> bool {
        self.inner.workflow_id == other.inner.workflow_id && self.inner.run_id == other.inner.run_id
    }

    fn __hash__(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.inner.workflow_id.hash(&mut hasher);
        self.inner.run_id.hash(&mut hasher);
        hasher.finish()
    }
}

impl PyWorkflowExecution {
    pub fn from_inner(inner: WorkflowExecution) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> WorkflowExecution {
        self.inner
    }

    pub fn inner(&self) -> &WorkflowExecution {
        &self.inner
    }
}

/// Serialized data plus metadata, as sent to and from the server.
#[pyclass(name = "Payload")]
#[derive(Clone, Debug)]
pub struct PyPayload {
    inner: Payload,
}

#[pymethods]
impl PyPayload {
    #[new]
    #[pyo3(signature = (data=None, metadata=None))]
    fn new(
        data: Option<&Bound<'_, PyBytes>>,
        metadata: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let data_vec = data.map(|b| b.as_bytes().to_vec()).unwrap_or_default();

        let metadata_map: HashMap<String, Vec<u8>> = if let Some(dict) = metadata {
            let mut map = HashMap::new();
            for (key, value) in dict.iter() {
                let key_str: String = key.extract()?;
                let value_bytes: Vec<u8> = if let Ok(bytes) = value.downcast::<PyBytes>() {
                    bytes.as_bytes().to_vec()
                } else if let Ok(s) = value.extract::<String>() {
                    s.into_bytes()
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "Metadata values must be bytes or str",
                    ));
                };
                map.insert(key_str, value_bytes);
            }
            map
        } else {
            HashMap::new()
        };

        Ok(Self {
            inner: Payload {
                data: data_vec,
                metadata: metadata_map,
            },
        })
    }

    /// Create a Payload by serializing a value as JSON.
    #[staticmethod]
    fn from_json(py: Python<'_>, value: PyObject) -> PyResult<Self> {
        let json_module = py.import("json")?;
        let json_str: String = json_module.call_method1("dumps", (value,))?.extract()?;

        match Payload::from_json(
            &serde_json::from_str::<serde_json::Value>(&json_str).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid JSON: {}", e))
            })?,
        ) {
            Ok(payload) => Ok(Self { inner: payload }),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to create payload: {}",
                e
            ))),
        }
    }

    /// Deserialize the payload data as JSON and return the Python value.
    fn to_json(&self, py: Python<'_>) -> PyResult<PyObject> {
        match self.inner.to_json::<serde_json::Value>() {
            Ok(value) => {
                let json_module = py.import("json")?;
                let json_str = serde_json::to_string(&value).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Failed to serialize: {}",
                        e
                    ))
                })?;
                let result = json_module.call_method1("loads", (json_str,))?;
                Ok(result.into_pyobject(py)?.into_any().unbind())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to convert to JSON: {}",
                e
            ))),
        }
    }

    /// Create a Payload from a string
    #[staticmethod]
    fn from_string(value: String) -> Self {
        Self {
            inner: Payload::from_string(value),
        }
    }

    /// Decode the payload data as a string.
    fn to_string(&self) -> PyResult<String> {
        self.inner.as_string().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to convert to string: {}",
                e
            ))
        })
    }

    /// The raw data bytes.
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.data)
    }

    /// The metadata, as a dict of bytes values.
    #[getter]
    fn metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (key, value) in &self.inner.metadata {
            dict.set_item(key, PyBytes::new(py, value))?;
        }
        Ok(dict)
    }

    /// Whether the payload data is empty.
    fn is_empty(&self) -> bool {
        self.inner.data.is_empty()
    }

    /// Length of the payload data in bytes.
    fn __len__(&self) -> usize {
        self.inner.data.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "Payload(len={}, metadata_keys={:?})",
            self.inner.data.len(),
            self.inner.metadata.keys().collect::<Vec<_>>()
        )
    }
}

impl PyPayload {
    pub fn from_inner(inner: Payload) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> Payload {
        self.inner
    }

    pub fn inner(&self) -> &Payload {
        &self.inner
    }
}

/// Status of a workflow execution.
#[pyclass(name = "WorkflowStatus")]
#[derive(Clone, Debug)]
pub struct PyWorkflowStatus {
    inner: WorkflowStatus,
}

#[pymethods]
impl PyWorkflowStatus {
    /// Running status
    #[staticmethod]
    fn running() -> Self {
        Self {
            inner: WorkflowStatus::Running,
        }
    }

    /// Completed status
    #[staticmethod]
    fn completed() -> Self {
        Self {
            inner: WorkflowStatus::Completed,
        }
    }

    /// Failed status
    #[staticmethod]
    fn failed() -> Self {
        Self {
            inner: WorkflowStatus::Failed,
        }
    }

    /// Cancelled status
    #[staticmethod]
    fn cancelled() -> Self {
        Self {
            inner: WorkflowStatus::Cancelled,
        }
    }

    /// Terminated status
    #[staticmethod]
    fn terminated() -> Self {
        Self {
            inner: WorkflowStatus::Terminated,
        }
    }

    /// Timed out status
    #[staticmethod]
    fn timed_out() -> Self {
        Self {
            inner: WorkflowStatus::TimedOut,
        }
    }

    /// Whether the status is terminal (the execution has finished).
    fn is_terminal(&self) -> bool {
        self.inner.is_terminal()
    }

    /// Whether the workflow is running.
    fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    /// The status name.
    #[getter]
    fn name(&self) -> &'static str {
        match self.inner {
            WorkflowStatus::Running => "Running",
            WorkflowStatus::Completed => "Completed",
            WorkflowStatus::Failed => "Failed",
            WorkflowStatus::Cancelled => "Cancelled",
            WorkflowStatus::Terminated => "Terminated",
            WorkflowStatus::TimedOut => "TimedOut",
            WorkflowStatus::RestartedFresh => "RestartedFresh",
        }
    }

    fn __repr__(&self) -> String {
        format!("WorkflowStatus.{}", self.name())
    }

    fn __str__(&self) -> &'static str {
        self.name()
    }

    fn __eq__(&self, other: &PyWorkflowStatus) -> bool {
        std::mem::discriminant(&self.inner) == std::mem::discriminant(&other.inner)
    }
}

impl PyWorkflowStatus {
    pub fn from_inner(inner: WorkflowStatus) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> WorkflowStatus {
        self.inner
    }
}

/// How a failed task or workflow is retried.
#[pyclass(name = "RetryPolicy")]
#[derive(Clone, Debug)]
pub struct PyRetryPolicy {
    /// Initial retry interval in milliseconds
    #[pyo3(get, set)]
    pub initial_interval_ms: u64,
    /// Backoff coefficient
    #[pyo3(get, set)]
    pub backoff_coefficient: f64,
    /// Maximum retry interval in milliseconds
    #[pyo3(get, set)]
    pub max_interval_ms: u64,
    /// Maximum number of attempts (0 = unlimited)
    #[pyo3(get, set)]
    pub max_attempts: u32,
    /// Error types that should not be retried
    #[pyo3(get, set)]
    pub non_retryable_errors: Vec<String>,
}

#[pymethods]
impl PyRetryPolicy {
    #[new]
    #[pyo3(signature = (
        initial_interval_ms = 1000,
        backoff_coefficient = 2.0,
        max_interval_ms = 60000,
        max_attempts = 3,
        non_retryable_errors = vec![]
    ))]
    fn new(
        initial_interval_ms: u64,
        backoff_coefficient: f64,
        max_interval_ms: u64,
        max_attempts: u32,
        non_retryable_errors: Vec<String>,
    ) -> Self {
        Self {
            initial_interval_ms,
            backoff_coefficient,
            max_interval_ms,
            max_attempts,
            non_retryable_errors,
        }
    }

    /// Create a default retry policy
    #[staticmethod]
    fn default_policy() -> Self {
        Self {
            initial_interval_ms: 1000,
            backoff_coefficient: 2.0,
            max_interval_ms: 60000,
            max_attempts: 3,
            non_retryable_errors: vec![],
        }
    }

    /// Create a policy with no retries
    #[staticmethod]
    fn no_retry() -> Self {
        Self {
            initial_interval_ms: 0,
            backoff_coefficient: 1.0,
            max_interval_ms: 0,
            max_attempts: 1,
            non_retryable_errors: vec![],
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "RetryPolicy(initial_interval_ms={}, backoff_coefficient={}, max_interval_ms={}, max_attempts={})",
            self.initial_interval_ms, self.backoff_coefficient, self.max_interval_ms, self.max_attempts
        )
    }
}

/// Description of a failure, with an optional chained cause.
#[pyclass(name = "Failure")]
#[derive(Clone, Debug)]
pub struct PyFailure {
    #[pyo3(get, set)]
    pub message: String,
    #[pyo3(get, set)]
    pub source: String,
    #[pyo3(get, set)]
    pub stack_trace: String,
    #[pyo3(get, set)]
    pub failure_type: String,
    /// Boxed because the type is recursive; exposed through the `cause`
    /// getter and setter.
    cause_inner: Option<Box<PyFailure>>,
}

#[pymethods]
impl PyFailure {
    #[new]
    #[pyo3(signature = (message, source = String::new(), stack_trace = String::new(), failure_type = String::new(), cause = None))]
    fn new(
        message: String,
        source: String,
        stack_trace: String,
        failure_type: String,
        cause: Option<PyFailure>,
    ) -> Self {
        Self {
            message,
            source,
            stack_trace,
            failure_type,
            cause_inner: cause.map(Box::new),
        }
    }

    /// The failure that caused this one, if any.
    #[getter]
    fn cause(&self) -> Option<PyFailure> {
        self.cause_inner.as_ref().map(|b| (**b).clone())
    }

    /// Set the failure that caused this one.
    #[setter]
    fn set_cause(&mut self, cause: Option<PyFailure>) {
        self.cause_inner = cause.map(Box::new);
    }

    /// Create a Failure from a Python exception, capturing its type and traceback.
    #[staticmethod]
    fn from_exception(py: Python<'_>, exc: PyObject) -> PyResult<Self> {
        let exc_ref = exc.bind(py);

        // The first positional arg is the message; fall back to str(exc).
        let message: String = if let Ok(args) = exc_ref.getattr("args") {
            if let Ok(first) = args.get_item(0) {
                first.str()?.to_string()
            } else {
                exc_ref.str()?.to_string()
            }
        } else {
            exc_ref.str()?.to_string()
        };

        let failure_type = exc_ref
            .get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "Exception".to_string());

        let traceback_module = py.import("traceback")?;
        let stack_trace: String = traceback_module
            .call_method1(
                "format_exception",
                (
                    exc_ref.get_type(),
                    exc_ref,
                    exc_ref.getattr("__traceback__").ok(),
                ),
            )?
            .extract::<Vec<String>>()?
            .join("");

        Ok(Self {
            message,
            source: String::new(),
            stack_trace,
            failure_type,
            cause_inner: None,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Failure(message='{}', type='{}')",
            self.message, self.failure_type
        )
    }

    fn __str__(&self) -> &str {
        &self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_execution() {
        let exec = PyWorkflowExecution::new("wf-123".to_string(), "run-456".to_string());
        assert_eq!(exec.workflow_id(), "wf-123");
        assert_eq!(exec.run_id(), "run-456");
    }

    #[test]
    fn test_workflow_status() {
        let running = PyWorkflowStatus::running();
        assert!(running.is_running());
        assert!(!running.is_terminal());

        let completed = PyWorkflowStatus::completed();
        assert!(!completed.is_running());
        assert!(completed.is_terminal());
    }

    #[test]
    fn test_retry_policy() {
        let policy = PyRetryPolicy::default_policy();
        assert_eq!(policy.initial_interval_ms, 1000);
        assert_eq!(policy.max_attempts, 3);

        let no_retry = PyRetryPolicy::no_retry();
        assert_eq!(no_retry.max_attempts, 1);
    }

    #[test]
    fn test_failure() {
        let failure = PyFailure::new(
            "Something went wrong".to_string(),
            "test".to_string(),
            String::new(),
            "ValueError".to_string(),
            None,
        );
        assert_eq!(failure.message, "Something went wrong");
        assert_eq!(failure.failure_type, "ValueError");
    }
}
