//! Client bindings for Python
//!
//! Python wrappers for the core `WorkflowClient` and `WorkflowHandle`.

use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use orcher_sdk_core::client::{StartWorkflowOpts, WorkflowClient, WorkflowHandle};
use orcher_sdk_core::types::{
    ListWorkflowsOptions, ListWorkflowsSortOrder, SearchWorkflowsOptions, WorkflowExecution,
    WorkflowStatus,
};

use crate::convert::{json_value_to_py, py_to_json_value};
use crate::error::IntoPyResult;
use crate::runtime::block_on;
use crate::types::{PyWorkflowExecution, PyWorkflowStatus};

/// Convert a millisecond duration to the proto `Duration` used in retry policies.
fn ms_to_proto_duration(ms: u64) -> orcher_sdk_core::proto::prost_types::Duration {
    orcher_sdk_core::proto::prost_types::Duration {
        seconds: (ms / 1000) as i64,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    }
}

/// Build a proto `RetryPolicy` from the `retry_policy` kwargs dict passed by the
/// Python client. Fields are millisecond-based to match the native retry surface.
/// Returns `None` when no policy dict is present.
fn extract_retry_policy(
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Option<orcher_sdk_core::proto::orcher::v1::RetryPolicy>> {
    let Some(obj) = kwargs.and_then(|k| k.get_item("retry_policy").ok().flatten()) else {
        return Ok(None);
    };
    if obj.is_none() {
        return Ok(None);
    }
    let d = obj
        .downcast::<PyDict>()
        .map_err(|_| pyo3::exceptions::PyTypeError::new_err("retry_policy must be a dict"))?;
    let get_u64 = |key: &str| -> Option<u64> {
        d.get_item(key)
            .ok()
            .flatten()
            .and_then(|v| v.extract().ok())
    };
    let non_retryable: Vec<String> = d
        .get_item("non_retryable_error_types")
        .ok()
        .flatten()
        .and_then(|v| v.extract().ok())
        .unwrap_or_default();
    Ok(Some(orcher_sdk_core::proto::orcher::v1::RetryPolicy {
        initial_interval: get_u64("initial_interval_ms").map(ms_to_proto_duration),
        backoff_coefficient: d
            .get_item("backoff_coefficient")
            .ok()
            .flatten()
            .and_then(|v| v.extract().ok())
            .unwrap_or(2.0),
        maximum_interval: get_u64("max_interval_ms").map(ms_to_proto_duration),
        maximum_attempts: d
            .get_item("max_attempts")
            .ok()
            .flatten()
            .and_then(|v| v.extract().ok())
            .unwrap_or(0),
        non_retryable_error_types: non_retryable,
    }))
}

/// Connection settings for `Client`.
#[pyclass(name = "ClientConfig")]
#[derive(Clone, Debug)]
pub struct PyClientConfig {
    /// Server URL (e.g., "http://localhost:50051")
    #[pyo3(get, set)]
    pub server_url: String,
    /// Namespace (default: "default")
    #[pyo3(get, set)]
    pub namespace: String,
    /// Request timeout in milliseconds (default: 10000)
    #[pyo3(get, set)]
    pub timeout_ms: u64,
    /// Identity for this client
    #[pyo3(get, set)]
    pub identity: Option<String>,
    /// Path to CA certificate PEM file for TLS (optional)
    #[pyo3(get, set)]
    pub tls_ca_cert_path: Option<String>,
    /// Path to client certificate PEM file for mTLS (optional)
    #[pyo3(get, set)]
    pub tls_client_cert_path: Option<String>,
    /// Path to client key PEM file for mTLS (optional)
    #[pyo3(get, set)]
    pub tls_client_key_path: Option<String>,

    /// API key sent as `authorization: Bearer <key>` on every request.
    #[pyo3(get, set)]
    pub api_key: Option<String>,

    /// How long establishing the connection may take, in milliseconds
    /// (default: 10000).
    #[pyo3(get, set)]
    pub connect_timeout_ms: u64,
}

#[pymethods]
impl PyClientConfig {
    #[new]
    #[pyo3(signature = (server_url, namespace = "default".to_string(), timeout_ms = 10000, identity = None, tls_ca_cert_path = None, tls_client_cert_path = None, tls_client_key_path = None, api_key = None, connect_timeout_ms = 10000))]
    fn new(
        server_url: String,
        namespace: String,
        timeout_ms: u64,
        identity: Option<String>,
        tls_ca_cert_path: Option<String>,
        tls_client_cert_path: Option<String>,
        tls_client_key_path: Option<String>,
        api_key: Option<String>,
        connect_timeout_ms: u64,
    ) -> Self {
        Self {
            server_url,
            namespace,
            timeout_ms,
            identity,
            tls_ca_cert_path,
            tls_client_cert_path,
            tls_client_key_path,
            api_key,
            connect_timeout_ms,
        }
    }

    /// Create config from environment variables
    #[staticmethod]
    #[pyo3(signature = (prefix = "ORCHER_".to_string()))]
    fn from_env(py: Python<'_>, prefix: String) -> PyResult<Self> {
        let os_module = py.import("os")?;
        let environ = os_module.getattr("environ")?;

        let server_url: String = environ
            .call_method1(
                "get",
                (format!("{}SERVER_URL", prefix), "http://localhost:50051"),
            )?
            .extract()?;
        let namespace: String = environ
            .call_method1("get", (format!("{}NAMESPACE", prefix), "default"))?
            .extract()?;
        let timeout_str: String = environ
            .call_method1("get", (format!("{}TIMEOUT_MS", prefix), "10000"))?
            .extract()?;
        let timeout_ms: u64 = timeout_str.parse().unwrap_or(10000);
        let identity: Option<String> = environ
            .call_method1("get", (format!("{}IDENTITY", prefix),))?
            .extract()
            .ok();
        // `<prefix>API_KEY` (ORCHER_API_KEY by default). Keys are issued out of
        // band, so the environment is the usual way one reaches a process.
        let api_key: Option<String> = environ
            .call_method1("get", (format!("{}API_KEY", prefix),))?
            .extract()
            .ok();

        Ok(Self {
            server_url,
            namespace,
            timeout_ms,
            identity,
            api_key,
            tls_ca_cert_path: None,
            tls_client_cert_path: None,
            tls_client_key_path: None,
            connect_timeout_ms: 10000,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "ClientConfig(server_url='{}', namespace='{}', timeout_ms={})",
            self.server_url, self.namespace, self.timeout_ms
        )
    }
}

impl PyClientConfig {
    /// The TLS settings for this config's server URL and `tls_*` paths; see
    /// [`crate::tls::tls_from_paths`].
    fn build_tls_config(
        &self,
    ) -> std::result::Result<Option<orcher_sdk_core::poller::TlsConfig>, String> {
        crate::tls::tls_from_paths(
            &self.server_url,
            self.tls_ca_cert_path.as_deref(),
            self.tls_client_cert_path.as_deref(),
            self.tls_client_key_path.as_deref(),
        )
    }
}

/// Internal state for the client
struct ClientInner {
    client: WorkflowClient,
    config: PyClientConfig,
}

/// Client for starting, listing, and searching workflows.
#[pyclass(name = "Client")]
pub struct PyClient {
    inner: Arc<Mutex<Option<ClientInner>>>,
}

#[pymethods]
impl PyClient {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    /// Connect to the Orcher server.
    #[pyo3(signature = (config))]
    fn connect<'py>(&self, py: Python<'py>, config: PyClientConfig) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let server_url = config.server_url.clone();
        let connect_timeout = std::time::Duration::from_millis(config.connect_timeout_ms);

        // Build TLS config before entering async context (file I/O is sync)
        let tls_config = config
            .build_tls_config()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = if let Some(ref tls) = tls_config {
                // No custom CA -> verify against the system trust store, which is
                // what mTLS to a publicly-trusted server needs.
                let mut tls_cfg = match tls.ca_cert {
                    Some(ref ca) => tonic::transport::ClientTlsConfig::new()
                        .ca_certificate(tonic::transport::Certificate::from_pem(ca)),
                    None => tonic::transport::ClientTlsConfig::new().with_native_roots(),
                };

                if let (Some(ref cert), Some(ref key)) = (&tls.client_cert, &tls.client_key) {
                    let identity = tonic::transport::Identity::from_pem(cert, key);
                    tls_cfg = tls_cfg.identity(identity);
                }

                if let Some(ref domain) = tls.domain_name {
                    tls_cfg = tls_cfg.domain_name(domain.clone());
                }

                let endpoint = tonic::transport::Endpoint::from_shared(server_url.clone())
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "Invalid server URL: {}",
                            e
                        ))
                    })?
                    .tls_config(tls_cfg)
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "TLS configuration error: {}",
                            e
                        ))
                    })?
                    .connect_timeout(connect_timeout)
                    .timeout(std::time::Duration::from_secs(180));

                let channel = endpoint.connect().await.map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "TLS connection failed: {}",
                        crate::tls::with_causes(&e)
                    ))
                })?;

                WorkflowClient::from_channel(channel)
            } else {
                // As WorkflowClient::connect builds it, with the configured
                // connect timeout in place of its fixed one.
                let channel = tonic::transport::Endpoint::from_shared(server_url.clone())
                    .map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "Invalid server URL: {}",
                            e
                        ))
                    })?
                    .connect_timeout(connect_timeout)
                    .timeout(std::time::Duration::from_secs(180))
                    .connect_lazy();
                WorkflowClient::from_channel(channel)
            };

            // Attach the API key so every request carries it.
            let client = match config.api_key {
                Some(ref key) => client.with_api_key(key.clone()),
                None => client,
            };

            let mut guard = inner.lock().await;
            *guard = Some(ClientInner { client, config });

            Ok(())
        })
    }

    /// Check if client is connected
    fn is_connected(&self) -> bool {
        let inner = self.inner.clone();
        block_on(async {
            let guard = inner.lock().await;
            guard.is_some()
        })
    }

    /// Start a workflow execution
    #[pyo3(signature = (workflow_id, workflow_type, task_queue, input = None, **kwargs))]
    fn start_workflow<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        workflow_type: String,
        task_queue: String,
        input: Option<PyObject>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        let input_json = if let Some(input_obj) = input {
            py_to_json_value(py, input_obj.bind(py))?
        } else {
            serde_json::Value::Null
        };

        let execution_timeout: Option<Duration> = kwargs
            .and_then(|k| k.get_item("execution_timeout_ms").ok().flatten())
            .and_then(|v| v.extract::<u64>().ok())
            .map(Duration::from_millis);

        let run_timeout: Option<Duration> = kwargs
            .and_then(|k| k.get_item("run_timeout_ms").ok().flatten())
            .and_then(|v| v.extract::<u64>().ok())
            .map(Duration::from_millis);

        let task_timeout: Option<Duration> = kwargs
            .and_then(|k| k.get_item("task_timeout_ms").ok().flatten())
            .and_then(|v| v.extract::<u64>().ok())
            .map(Duration::from_millis);

        let cron_schedule: Option<String> = kwargs
            .and_then(|k| k.get_item("cron_schedule").ok().flatten())
            .and_then(|v| v.extract().ok());

        // Optional workflow-level retry policy. Built before the async block
        // because kwargs is not Send.
        let retry_policy = extract_retry_policy(kwargs)?;

        let id_reuse_policy: Option<String> = kwargs
            .and_then(|k| k.get_item("id_reuse_policy").ok().flatten())
            .map(|v| v.extract())
            .transpose()?;
        let id_reuse_policy = id_reuse_policy
            .map(|name| {
                use orcher_sdk_core::proto::orcher::v1::WorkflowIdReusePolicy as Policy;
                match name.as_str() {
                    "ALLOW_DUPLICATE" => Ok(Policy::AllowDuplicate),
                    "ALLOW_DUPLICATE_FAILED_ONLY" => Ok(Policy::AllowDuplicateFailedOnly),
                    "REJECT_DUPLICATE" => Ok(Policy::RejectDuplicate),
                    "TERMINATE_IF_RUNNING" => Ok(Policy::TerminateIfRunning),
                    other => Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "unknown id_reuse_policy {other:?}"
                    ))),
                }
            })
            .transpose()?;

        let input_bytes = serde_json::to_vec(&input_json).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Failed to serialize input: {}", e))
        })?;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let client_inner = guard.as_ref().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Client not connected")
            })?;

            let client = client_inner
                .client
                .clone()
                .with_namespace(&client_inner.config.namespace);

            let mut opts = StartWorkflowOpts::new(
                workflow_id.clone(),
                workflow_type.clone(),
                task_queue.clone(),
                input_bytes,
            );
            opts.cron_schedule = cron_schedule;
            opts.retry_policy = retry_policy;
            opts.execution_timeout = execution_timeout;
            opts.run_timeout = run_timeout;
            opts.task_timeout = task_timeout;
            opts.id_reuse_policy = id_reuse_policy;

            let handle = client
                .start_workflow_with_options(opts)
                .await
                .into_py_result()?;

            Ok(PyWorkflowHandle::from_handle(handle))
        })
    }

    /// Get a handle to an existing workflow
    #[pyo3(signature = (workflow_id, run_id = None))]
    fn get_workflow_handle<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        run_id: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let client_inner = guard.as_ref().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Client not connected")
            })?;

            let client = client_inner
                .client
                .clone()
                .with_namespace(&client_inner.config.namespace);

            // An empty run id is the wire's "latest run" sentinel: the server
            // resolves by execution_id when one is given and by workflow_id
            // otherwise. A placeholder such as "unknown" would turn every later
            // call into an exact lookup for a run that cannot exist, failing
            // with a not-found error naming an id the caller never supplied.
            let execution = WorkflowExecution::new(workflow_id, run_id.unwrap_or_default());
            let handle = WorkflowHandle::new(client, execution);

            Ok(PyWorkflowHandle::from_handle(handle))
        })
    }

    /// List workflow executions with optional filters
    #[pyo3(signature = (page_size = 100, next_page_token = None, workflow_type = None, task_queue = None, status_filter = None, sort_order = None))]
    fn list_workflows<'py>(
        &self,
        py: Python<'py>,
        page_size: i32,
        next_page_token: Option<Vec<u8>>,
        workflow_type: Option<String>,
        task_queue: Option<String>,
        status_filter: Option<Vec<String>>,
        sort_order: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        // Unknown status names are ignored rather than rejected.
        let status_filter_values: Vec<WorkflowStatus> = status_filter
            .unwrap_or_default()
            .into_iter()
            .filter_map(|s| match s.to_uppercase().as_str() {
                "RUNNING" => Some(WorkflowStatus::Running),
                "COMPLETED" => Some(WorkflowStatus::Completed),
                "FAILED" => Some(WorkflowStatus::Failed),
                "CANCELLED" | "CANCELED" => Some(WorkflowStatus::Cancelled),
                "TERMINATED" => Some(WorkflowStatus::Terminated),
                "TIMED_OUT" => Some(WorkflowStatus::TimedOut),
                "RESTARTED_FRESH" => Some(WorkflowStatus::RestartedFresh),
                _ => None,
            })
            .collect();

        // An unknown sort order is treated as unset.
        let sort_order_value = sort_order.and_then(|s| match s.to_lowercase().as_str() {
            "start_time_asc" => Some(ListWorkflowsSortOrder::StartTimeAsc),
            "start_time_desc" => Some(ListWorkflowsSortOrder::StartTimeDesc),
            "close_time_asc" => Some(ListWorkflowsSortOrder::CloseTimeAsc),
            "close_time_desc" => Some(ListWorkflowsSortOrder::CloseTimeDesc),
            _ => None,
        });

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let client_inner = guard.as_ref().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Client not connected")
            })?;

            let client = client_inner
                .client
                .clone()
                .with_namespace(&client_inner.config.namespace);

            let mut options = ListWorkflowsOptions::default()
                .with_page_size(page_size)
                .with_next_page_token(next_page_token.unwrap_or_default())
                .with_status_filter(status_filter_values);
            options.workflow_type = workflow_type;
            options.task_queue = task_queue;
            options.sort_order = sort_order_value;

            let page = client.list_workflows(options).await.into_py_result()?;

            let json_value = serde_json::to_value(&page).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to serialize list result: {}",
                    e
                ))
            })?;

            Python::with_gil(|py| json_value_to_py(py, &json_value))
        })
    }

    /// Search workflow executions using query syntax
    #[pyo3(signature = (query, page_size = 100, next_page_token = None))]
    fn search_workflows<'py>(
        &self,
        py: Python<'py>,
        query: String,
        page_size: i32,
        next_page_token: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let client_inner = guard.as_ref().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Client not connected")
            })?;

            let client = client_inner
                .client
                .clone()
                .with_namespace(&client_inner.config.namespace);

            let page = client
                .search_workflows(
                    query,
                    SearchWorkflowsOptions::default()
                        .with_page_size(page_size)
                        .with_next_page_token(next_page_token.unwrap_or_default()),
                )
                .await
                .into_py_result()?;

            let json_value = serde_json::to_value(&page).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to serialize search result: {}",
                    e
                ))
            })?;

            Python::with_gil(|py| json_value_to_py(py, &json_value))
        })
    }

    /// Close the client connection
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            *guard = None;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        let connected = self.is_connected();
        format!("Client(connected={})", connected)
    }
}

/// Handle to a single workflow execution.
#[pyclass(name = "WorkflowHandle")]
pub struct PyWorkflowHandle {
    inner: Arc<Mutex<WorkflowHandle>>,
    workflow_id: String,
    run_id: String,
}

impl PyWorkflowHandle {
    fn from_handle(handle: WorkflowHandle) -> Self {
        let workflow_id = handle.workflow_id().to_string();
        let run_id = handle.run_id().to_string();
        Self {
            inner: Arc::new(Mutex::new(handle)),
            workflow_id,
            run_id,
        }
    }
}

#[pymethods]
impl PyWorkflowHandle {
    /// Get the workflow ID
    #[getter]
    fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Get the run ID
    #[getter]
    fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Get the workflow execution info
    #[getter]
    fn execution(&self) -> PyWorkflowExecution {
        PyWorkflowExecution::from_inner(WorkflowExecution::new(
            self.workflow_id.clone(),
            self.run_id.clone(),
        ))
    }

    /// Wait for the workflow result
    #[pyo3(signature = (timeout_ms = None))]
    fn result<'py>(&self, py: Python<'py>, timeout_ms: Option<u64>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.lock().await;

            let result: serde_json::Value = if let Some(timeout) = timeout_ms {
                tokio::time::timeout(
                    Duration::from_millis(timeout),
                    handle.result::<serde_json::Value>(),
                )
                .await
                .map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTimeoutError, _>(
                        "Timeout waiting for workflow result",
                    )
                })?
                .into_py_result()?
            } else {
                handle
                    .result::<serde_json::Value>()
                    .await
                    .into_py_result()?
            };

            Python::with_gil(|py| json_value_to_py(py, &result))
        })
    }

    /// Get the current workflow status
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.lock().await;
            let status = handle.status().await.into_py_result()?;
            Ok(PyWorkflowStatus::from_inner(status))
        })
    }

    /// Send an event to the workflow
    #[pyo3(signature = (event_name, payload = None))]
    fn send_event<'py>(
        &self,
        py: Python<'py>,
        event_name: String,
        payload: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        let payload_json = if let Some(p) = payload {
            py_to_json_value(py, p.bind(py))?
        } else {
            serde_json::Value::Null
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.lock().await;
            handle
                .send_event(&event_name, payload_json)
                .await
                .into_py_result()?;
            Ok(())
        })
    }

    /// Query the workflow
    #[pyo3(signature = (query_type, args = None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        query_type: String,
        args: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        let args_json = if let Some(a) = args {
            py_to_json_value(py, a.bind(py))?
        } else {
            serde_json::Value::Null
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.lock().await;
            let result: serde_json::Value = handle
                .query(&query_type, args_json)
                .await
                .into_py_result()?;

            Python::with_gil(|py| json_value_to_py(py, &result))
        })
    }

    /// Update the workflow
    #[pyo3(signature = (update_name, args = None))]
    fn update<'py>(
        &self,
        py: Python<'py>,
        update_name: String,
        args: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        let args_json = if let Some(a) = args {
            py_to_json_value(py, a.bind(py))?
        } else {
            serde_json::Value::Null
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.lock().await;
            let result: serde_json::Value = handle
                .update(&update_name, args_json)
                .await
                .into_py_result()?;

            Python::with_gil(|py| json_value_to_py(py, &result))
        })
    }

    /// Describe the workflow execution (detailed metadata)
    fn describe<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.lock().await;
            let description = handle.describe().await.into_py_result()?;

            let json_value = serde_json::to_value(&description).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Failed to serialize description: {}",
                    e
                ))
            })?;

            Python::with_gil(|py| json_value_to_py(py, &json_value))
        })
    }

    /// Cancel the workflow
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.lock().await;
            handle.cancel().await.into_py_result()?;
            Ok(())
        })
    }

    /// Terminate the workflow
    #[pyo3(signature = (reason = None))]
    fn terminate<'py>(
        &self,
        py: Python<'py>,
        reason: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let reason_str = reason.unwrap_or_else(|| "terminated by user".to_string());

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.lock().await;
            handle.terminate(reason_str).await.into_py_result()?;
            Ok(())
        })
    }

    /// Reset the workflow to a journal point and re-execute from there.
    ///
    /// Returns the new execution id created by the reset.
    #[pyo3(signature = (target_event_id, reason = None))]
    fn reset_workflow<'py>(
        &self,
        py: Python<'py>,
        target_event_id: i64,
        reason: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let reason_str = reason.unwrap_or_else(|| "reset by user".to_string());

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let handle = inner.lock().await;
            let new_execution_id = handle
                .reset(target_event_id, reason_str)
                .await
                .into_py_result()?;
            Ok(new_execution_id)
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkflowHandle(workflow_id='{}', run_id='{}')",
            self.workflow_id, self.run_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config() {
        let config = PyClientConfig::new(
            "http://localhost:50051".to_string(),
            "default".to_string(),
            10000,
            None,
            None,
            None,
            None,
            None,
            2500,
        );
        assert_eq!(config.server_url, "http://localhost:50051");
        assert_eq!(config.namespace, "default");
        assert_eq!(config.timeout_ms, 10000);
        assert_eq!(config.connect_timeout_ms, 2500);
    }
}
