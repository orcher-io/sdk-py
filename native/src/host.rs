//! The worker, as a binding over the shared worker host (`orcher-sdk-host`).
//!
//! The host runs one worker's plumbing: core's workflow, task and actor
//! drivers built from one configuration, the worker's registration and
//! heartbeats, sessions, and a draining shutdown. Python pulls work from it
//! when it has room to run some, runs it, and hands back the outcome:
//!
//! - `next_activation` / `complete_activation`: a workflow activation as the
//!   kernel protocol's `RunActivation`, answered with a `RunActivationResult`,
//!   both as JSON (see [`crate::kernel`]).
//! - `next_task` / `complete_task` / `fail_task`: a task, with `heartbeat`
//!   and `task_cancelled` while it runs.
//! - `next_actor_operation` / `complete_actor_operation` /
//!   `fail_actor_operation`: an actor operation, with the actor state calls
//!   the operation makes, on the worker's connection.
//!
//! A pull resolves `None` once the host has stopped and handed over
//! everything it took; it never wakes up empty-handed before then.

use std::time::Duration;

use orcher_sdk_core::proto::host::v1::{
    actor_result, task_outcome, ActorMode, ActorOperation, ActorResult, TaskAssignment, TaskOutcome,
};
use orcher_sdk_core::proto::kernel::v1::{Failure, Payload};
use orcher_sdk_host::{ActorHandler, Host, HostConfig, Registrations, StateScope};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde::Deserialize;
use serde_json::json;

use crate::kernel;

/// The worker's configuration, as `WorkerHost.start` reads it from JSON.
/// Every option left out keeps the host's default.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigJson {
    server_url: String,
    task_queue: String,
    namespace: String,
    identity: Option<String>,
    api_key: Option<String>,
    organization_id: Option<String>,
    version_id: Option<String>,
    tls_ca_cert_path: Option<String>,
    tls_client_cert_path: Option<String>,
    tls_client_key_path: Option<String>,
    max_concurrent_workflows: Option<u32>,
    max_concurrent_tasks: Option<u32>,
    max_concurrent_actor_operations: Option<u32>,
    workflow_pollers: Option<usize>,
    task_pollers: Option<usize>,
    actor_pollers: Option<usize>,
    max_sessions: Option<usize>,
    shutdown_grace_ms: Option<u64>,
    sdk_name: Option<String>,
    sdk_version: Option<String>,
}

/// What the worker runs, as `WorkerHost.start` reads it from JSON.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RegistrationsJson {
    workflow_types: Vec<String>,
    task_types: Vec<String>,
    actor_handlers: Vec<ActorHandlerJson>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActorHandlerJson {
    actor_name: String,
    operation: String,
    #[serde(default)]
    shared: bool,
}

/// An API key that is set must be sendable as a header, or it would be
/// dropped and the worker would connect with no credentials at all. A blank
/// one means none.
fn checked_api_key(api_key: Option<String>) -> Result<Option<String>, String> {
    let Some(key) = api_key.filter(|k| !k.trim().is_empty()) else {
        return Ok(None);
    };
    if format!("Bearer {key}")
        .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
        .is_err()
    {
        return Err(
            "api_key contains characters that cannot be sent in the authorization header"
                .to_string(),
        );
    }
    Ok(Some(key))
}

impl ConfigJson {
    fn into_host_config(self) -> Result<HostConfig, String> {
        let defaults = HostConfig::new(&self.server_url, &self.task_queue);
        // TLS follows the URL scheme and the paths, as for the client.
        let tls = crate::tls::tls_from_paths(
            &self.server_url,
            self.tls_ca_cert_path.as_deref(),
            self.tls_client_cert_path.as_deref(),
            self.tls_client_key_path.as_deref(),
        )?;
        let mut config = defaults
            .clone()
            .with_namespace(self.namespace)
            .with_concurrency(
                self.max_concurrent_workflows
                    .unwrap_or(defaults.max_concurrent_workflows),
                self.max_concurrent_tasks
                    .unwrap_or(defaults.max_concurrent_tasks),
                self.max_concurrent_actor_operations
                    .unwrap_or(defaults.max_concurrent_actor_operations),
            )
            .with_pollers(
                self.workflow_pollers.unwrap_or(defaults.workflow_pollers),
                self.task_pollers.unwrap_or(defaults.task_pollers),
                self.actor_pollers.unwrap_or(defaults.actor_pollers),
            )
            .with_sdk(
                self.sdk_name.unwrap_or_default(),
                self.sdk_version.unwrap_or_default(),
            )
            // A workflow failure is marked non-retryable only when the
            // workflow code raised it so (`workflow_failure` in
            // `orcher.worker.activation`).
            .with_protocol_version(orcher_sdk_core::worker_protocol::WORKFLOW_FAILURE_RETRYABILITY);
        if let Some(identity) = self.identity {
            config = config.with_identity(identity);
        }
        if let Some(api_key) = checked_api_key(self.api_key)? {
            config = config.with_api_key(api_key);
        }
        if let Some(organization_id) = self.organization_id {
            config = config.with_organization_id(organization_id);
        }
        if let Some(version_id) = self.version_id {
            config = config.with_version_id(version_id);
        }
        if let Some(tls) = tls {
            config = config.with_tls(tls);
        }
        if let Some(max_sessions) = self.max_sessions {
            config = config.with_max_sessions(max_sessions);
        }
        if let Some(ms) = self.shutdown_grace_ms {
            config = config.with_shutdown_grace(Duration::from_millis(ms));
        }
        Ok(config)
    }
}

impl RegistrationsJson {
    fn into_registrations(self) -> Registrations {
        Registrations::new(self.workflow_types, self.task_types, [] as [&str; 0])
            .with_actor_handlers(self.actor_handlers.into_iter().map(|handler| {
                if handler.shared {
                    ActorHandler::shared(handler.actor_name, handler.operation)
                } else {
                    ActorHandler::exclusive(handler.actor_name, handler.operation)
                }
            }))
    }
}

/// A task as Python reads it, as JSON.
fn task_json(task: &TaskAssignment) -> serde_json::Value {
    let run = task.run.clone().unwrap_or_default();
    let millis = |d: &orcher_sdk_core::proto::prost_types::Duration| {
        d.seconds
            .saturating_mul(1000)
            .saturating_add(i64::from(d.nanos) / 1_000_000)
    };
    json!({
        "token": task.token,
        "task_id": task.task_id,
        "task_type": task.task_type,
        "workflow_id": run.workflow_id,
        "run_id": run.run_id,
        "task_queue": run.task_queue,
        "namespace": run.namespace,
        // The task's arguments as their JSON encoding, one payload.
        "input": task.input.first().map(|payload| &payload.data),
        "attempt": task.attempt,
        // The limits the attempt runs under; null when the engine set none.
        "attempt_timeout_ms": task.attempt_timeout.as_ref().map(millis),
        "heartbeat_timeout_ms": task.heartbeat_timeout.as_ref().map(millis),
    })
}

/// An actor operation as Python reads it, as JSON.
fn actor_operation_json(operation: &ActorOperation) -> serde_json::Value {
    json!({
        "operation_id": operation.operation_id,
        "execution_id": operation.execution_id,
        "actor_name": operation.actor_name,
        "key": operation.key,
        "operation": operation.operation,
        "payload": operation.payload.as_ref().map(|payload| &payload.data),
        "mode": if operation.mode == ActorMode::Shared as i32 { "shared" } else { "exclusive" },
        "metadata": operation.metadata,
    })
}

fn json_payload(data: Vec<u8>) -> Payload {
    Payload {
        data,
        metadata: [("encoding".to_string(), b"json".to_vec())].into(),
        ..Default::default()
    }
}

fn host_error(what: &str, error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(format!("{what}: {error}"))
}

/// A running worker host.
#[pyclass(name = "WorkerHost", frozen)]
pub struct PyWorkerHost {
    host: Host,
}

#[pymethods]
impl PyWorkerHost {
    /// `WorkerHost.start(config_json, registrations_json) -> Awaitable[WorkerHost]`
    ///
    /// Starts a worker: its drivers, its registration with the engine, and its
    /// actor handlers' registration with the actor service. Raises
    /// `ValueError` for a configuration it cannot use (a TLS file it cannot
    /// read, an API key that cannot be a header), and `RuntimeError` when
    /// nothing is registered, the engine cannot be reached, or the actor
    /// service refuses the handlers.
    #[staticmethod]
    fn start<'py>(
        py: Python<'py>,
        config_json: String,
        registrations_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let config: ConfigJson = serde_json::from_str(&config_json)
            .map_err(|e| PyValueError::new_err(format!("invalid worker configuration: {e}")))?;
        let registrations: RegistrationsJson = serde_json::from_str(&registrations_json)
            .map_err(|e| PyValueError::new_err(format!("invalid worker registrations: {e}")))?;
        let config = config.into_host_config().map_err(PyValueError::new_err)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let host = Host::start(config, registrations.into_registrations())
                .await
                .map_err(|e| host_error("the worker could not start", e))?;
            Ok(PyWorkerHost { host })
        })
    }

    /// The worker's identity, as it registered.
    #[getter]
    fn identity(&self) -> String {
        self.host.config().identity.clone()
    }

    /// `wire_metrics() -> str`
    ///
    /// The worker's measurements so far, as JSON: per gRPC method its calls,
    /// status errors by code, transport errors, retries and latency, and the
    /// activations and tasks it ran.
    fn wire_metrics(&self) -> PyResult<String> {
        serde_json::to_string(&self.host.wire_metrics())
            .map_err(|e| PyRuntimeError::new_err(format!("the measurements did not encode: {e}")))
    }

    /// `next_activation() -> Awaitable[str | None]`
    ///
    /// The next workflow activation, as `RunActivation` JSON; `None` once the
    /// worker has stopped and handed over every activation it took.
    fn next_activation<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(host
                .next_activation()
                .await
                .map(|activation| kernel::activation_json(&activation).to_string()))
        })
    }

    /// `complete_activation(result_json: str) -> Awaitable[None]`
    ///
    /// Answers an activation with its `RunActivationResult` JSON. Raises
    /// `ValueError` for JSON that is not one, and `RuntimeError` for an
    /// answer the host does not take (the activation was already answered,
    /// or the worker stopped).
    fn complete_activation<'py>(
        &self,
        py: Python<'py>,
        result_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let answer = kernel::result_from_json(&result_json).map_err(PyValueError::new_err)?;
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            host.complete_activation(answer)
                .await
                .map_err(|e| host_error("the activation's answer was not taken", e))
        })
    }

    /// `next_task() -> Awaitable[str | None]`
    ///
    /// The next task, as JSON; `None` once the worker has stopped and handed
    /// over every task it took. The built-in session tasks are run by the
    /// host and never handed over.
    fn next_task<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(host
                .next_task()
                .await
                .map(|task| task_json(&task).to_string()))
        })
    }

    /// `heartbeat(token: bytes, details: bytes | None = None) -> bool`
    ///
    /// Records a heartbeat from a running task's code, with its details (their
    /// JSON encoding), sent with the next heartbeat due. Never waits: the
    /// host heartbeats every task it handed over on its own. Returns whether
    /// the task has been asked to cancel; `False` for a task the host does not
    /// hold.
    #[pyo3(signature = (token, details = None))]
    fn heartbeat(&self, token: Vec<u8>, details: Option<Vec<u8>>) -> bool {
        self.host
            .heartbeat(&token, details.map(json_payload))
            .cancel_requested
    }

    /// `task_cancelled(token: bytes) -> Awaitable[bool]`
    ///
    /// Resolves `True` once a running task is asked to cancel (its workflow
    /// was cancelled, the attempt is no longer running, or the worker is
    /// stopping past its grace), and `False` once its outcome is reported (at
    /// once for a task the host does not hold).
    fn task_cancelled<'py>(&self, py: Python<'py>, token: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(host.task_cancelled(&token).await)
        })
    }

    /// `complete_task(token: bytes, result: bytes) -> Awaitable[None]`
    ///
    /// Reports a task's result, its JSON encoding.
    fn complete_task<'py>(
        &self,
        py: Python<'py>,
        token: Vec<u8>,
        result: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            host.complete_task(TaskOutcome {
                token,
                result: Some(task_outcome::Result::Value(json_payload(result))),
                ..Default::default()
            })
            .await
            .map_err(|e| host_error("the task's result was not taken", e))
        })
    }

    /// `fail_task(token, message, error_type, non_retryable=False) -> Awaitable[None]`
    ///
    /// Reports a task's failure. The engine decides whether to retry it from
    /// its type, which a retry policy names, and `non_retryable`.
    #[pyo3(signature = (token, message, error_type, non_retryable = false))]
    fn fail_task<'py>(
        &self,
        py: Python<'py>,
        token: Vec<u8>,
        message: String,
        error_type: String,
        non_retryable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            host.complete_task(TaskOutcome {
                token,
                result: Some(task_outcome::Result::Failure(Failure {
                    message,
                    error_type,
                    non_retryable,
                    ..Default::default()
                })),
                ..Default::default()
            })
            .await
            .map_err(|e| host_error("the task's failure was not taken", e))
        })
    }

    /// `next_actor_operation() -> Awaitable[str | None]`
    ///
    /// The next actor operation, as JSON; `None` once the worker has stopped.
    fn next_actor_operation<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(host
                .next_actor_operation()
                .await
                .map(|operation| actor_operation_json(&operation).to_string()))
        })
    }

    /// `complete_actor_operation(operation_id, execution_id, result: bytes) -> Awaitable[None]`
    ///
    /// Reports an actor operation's result, its JSON encoding.
    fn complete_actor_operation<'py>(
        &self,
        py: Python<'py>,
        operation_id: String,
        execution_id: String,
        result: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            host.complete_actor_operation(ActorResult {
                operation_id,
                execution_id,
                result: Some(actor_result::Result::Value(json_payload(result))),
                ..Default::default()
            })
            .await
            .map_err(|e| host_error("the actor operation's result was not taken", e))
        })
    }

    /// `fail_actor_operation(operation_id, execution_id, message, error_type) -> Awaitable[None]`
    ///
    /// Reports an actor operation's failure, with its type.
    fn fail_actor_operation<'py>(
        &self,
        py: Python<'py>,
        operation_id: String,
        execution_id: String,
        message: String,
        error_type: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            host.complete_actor_operation(ActorResult {
                operation_id,
                execution_id,
                result: Some(actor_result::Result::Failure(Failure {
                    message,
                    error_type,
                    ..Default::default()
                })),
                ..Default::default()
            })
            .await
            .map_err(|e| host_error("the actor operation's failure was not taken", e))
        })
    }

    /// `actor_get_state(actor_name, key, state_key, execution_id) -> Awaitable[bytes | None]`
    ///
    /// One key of an actor's state; `None` when it is not set.
    fn actor_get_state<'py>(
        &self,
        py: Python<'py>,
        actor_name: String,
        key: String,
        state_key: String,
        execution_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let scope = StateScope::new(actor_name, key, execution_id);
            let value = host
                .get_actor_state(&scope, &state_key)
                .await
                .map_err(|e| host_error("reading actor state", e))?;
            Ok(value.map(|value| value.value))
        })
    }

    /// `actor_set_state(actor_name, key, state_key, value, execution_id) -> Awaitable[None]`
    ///
    /// Writes one key of an actor's state.
    fn actor_set_state<'py>(
        &self,
        py: Python<'py>,
        actor_name: String,
        key: String,
        state_key: String,
        value: Vec<u8>,
        execution_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let scope = StateScope::new(actor_name, key, execution_id);
            host.set_actor_state(&scope, &state_key, value, None)
                .await
                .map(|_version| ())
                .map_err(|e| host_error("writing actor state", e))
        })
    }

    /// `actor_delete_state(actor_name, key, state_key, execution_id) -> Awaitable[bool]`
    ///
    /// Deletes one key of an actor's state; resolves whether it was set.
    fn actor_delete_state<'py>(
        &self,
        py: Python<'py>,
        actor_name: String,
        key: String,
        state_key: String,
        execution_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let scope = StateScope::new(actor_name, key, execution_id);
            host.delete_actor_state(&scope, &state_key)
                .await
                .map_err(|e| host_error("deleting actor state", e))
        })
    }

    /// `actor_list_state_keys(actor_name, key, execution_id, prefix) -> Awaitable[list[str]]`
    ///
    /// The keys of an actor's state that start with `prefix`.
    fn actor_list_state_keys<'py>(
        &self,
        py: Python<'py>,
        actor_name: String,
        key: String,
        execution_id: String,
        prefix: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let scope = StateScope::new(actor_name, key, execution_id);
            host.actor_state_keys(&scope, &prefix)
                .await
                .map_err(|e| host_error("listing actor state keys", e))
        })
    }

    /// `shutdown() -> Awaitable[None]`
    ///
    /// Stops the worker: it stops claiming work, gives the work running its
    /// shutdown grace to finish, hands back the activations still unanswered,
    /// asks the tasks still running to cancel, closes its sessions and
    /// deregisters. Pulls then resolve `None`. A second call resolves at once.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let host = self.host.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            host.shutdown().await;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        let config = self.host.config();
        format!(
            "WorkerHost(server_url='{}', namespace='{}', task_queue='{}', identity='{}')",
            config.server_url, config.namespace, config.task_queue, config.identity
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(json: &str) -> Result<HostConfig, String> {
        serde_json::from_str::<ConfigJson>(json)
            .map_err(|e| e.to_string())?
            .into_host_config()
    }

    #[test]
    fn a_configuration_maps_every_option_onto_the_host() {
        let config = config(
            r#"{
                "server_url": "http://localhost:50051",
                "task_queue": "orders",
                "namespace": "shop",
                "identity": "worker-1",
                "api_key": "key",
                "organization_id": "org",
                "version_id": "v7",
                "max_concurrent_workflows": 3,
                "max_concurrent_tasks": 4,
                "max_concurrent_actor_operations": 5,
                "workflow_pollers": 6,
                "task_pollers": 7,
                "actor_pollers": 8,
                "max_sessions": 9,
                "shutdown_grace_ms": 2500,
                "sdk_name": "python",
                "sdk_version": "1.2.3"
            }"#,
        )
        .unwrap();
        assert_eq!(config.server_url, "http://localhost:50051");
        assert_eq!(config.task_queue, "orders");
        assert_eq!(config.namespace, "shop");
        assert_eq!(config.identity, "worker-1");
        assert_eq!(config.api_key.as_deref(), Some("key"));
        assert_eq!(config.organization_id.as_deref(), Some("org"));
        assert_eq!(config.version_id.as_deref(), Some("v7"));
        assert!(config.tls.is_none());
        assert_eq!(
            (
                config.max_concurrent_workflows,
                config.max_concurrent_tasks,
                config.max_concurrent_actor_operations
            ),
            (3, 4, 5)
        );
        assert_eq!(
            (
                config.workflow_pollers,
                config.task_pollers,
                config.actor_pollers
            ),
            (6, 7, 8)
        );
        assert_eq!(config.max_sessions, 9);
        assert_eq!(config.shutdown_grace, Duration::from_millis(2500));
        assert_eq!(
            (config.sdk_name.as_str(), config.sdk_version.as_str()),
            ("python", "1.2.3")
        );
    }

    #[test]
    fn unset_options_keep_the_host_defaults() {
        let config = config(
            r#"{ "server_url": "http://x:1", "task_queue": "q", "namespace": "n",
                 "version_id": " ", "api_key": "  " }"#,
        )
        .unwrap();
        let defaults = HostConfig::new("http://x:1", "q");
        assert_eq!(
            config.max_concurrent_workflows,
            defaults.max_concurrent_workflows
        );
        assert_eq!(config.task_pollers, defaults.task_pollers);
        assert_eq!(config.shutdown_grace, defaults.shutdown_grace);
        assert!(config.identity.starts_with("orcher-worker-"));
        // A blank release is no release, and a blank key no key.
        assert!(config.version_id.is_none());
        assert!(config.api_key.is_none());
    }

    /// The worker reports that its workflow failures carry the mark the
    /// workflow code raised.
    #[test]
    fn the_worker_reports_meaningful_workflow_failure_marks() {
        let config =
            config(r#"{ "server_url": "http://x:1", "task_queue": "q", "namespace": "n" }"#)
                .unwrap();
        assert_eq!(
            config.protocol_version,
            orcher_sdk_core::worker_protocol::WORKFLOW_FAILURE_RETRYABILITY
        );
    }

    #[test]
    fn https_means_tls_on_the_system_trust_store() {
        let tls = config(r#"{ "server_url": "https://x:1", "task_queue": "q", "namespace": "n" }"#)
            .unwrap()
            .tls
            .expect("tls");
        assert!(tls.ca_cert.is_none() && tls.client_cert.is_none());
    }

    #[test]
    fn what_cannot_be_used_is_refused_rather_than_dropped() {
        for json in [
            // A key that cannot be a header would be sent as no key.
            r#"{ "server_url": "http://x:1", "task_queue": "q", "namespace": "n", "api_key": "a\nb" }"#,
            r#"{ "server_url": "http://x:1", "task_queue": "q", "namespace": "n",
                 "tls_ca_cert_path": "/no/such/file.pem" }"#,
            r#"{ "server_url": "http://x:1", "task_queue": "q", "namespace": "n",
                 "tls_client_cert_path": "/a.pem" }"#,
            r#"{ "server_url": "http://x:1", "task_queue": "q", "namespace": "n", "poll_ms": 1 }"#,
        ] {
            assert!(config(json).is_err(), "{json}");
        }
    }

    #[test]
    fn registrations_carry_actor_handlers_with_their_modes() {
        let registrations: RegistrationsJson = serde_json::from_str(
            r#"{ "workflow_types": ["wf"], "task_types": ["t"],
                 "actor_handlers": [
                   { "actor_name": "Cart", "operation": "add" },
                   { "actor_name": "Cart", "operation": "total", "shared": true }
                 ] }"#,
        )
        .unwrap();
        let registrations = registrations.into_registrations();
        assert_eq!(registrations.workflow_types, ["wf"]);
        assert_eq!(registrations.task_types, ["t"]);
        assert_eq!(registrations.actor_types, ["Cart"]);
        assert_eq!(
            registrations.actor_handlers,
            [
                ActorHandler::exclusive("Cart", "add"),
                ActorHandler::shared("Cart", "total")
            ]
        );
    }

    #[test]
    fn a_task_crosses_with_its_limits() {
        let task = TaskAssignment {
            token: vec![1],
            task_id: "charge_1".into(),
            task_type: "charge".into(),
            run: Some(orcher_sdk_core::proto::kernel::v1::RunRef {
                workflow_id: "wf".into(),
                run_id: "run".into(),
                task_queue: "q".into(),
                namespace: "ns".into(),
                ..Default::default()
            }),
            input: vec![json_payload(b"{\"n\":1}".to_vec())],
            attempt: 2,
            attempt_timeout: Some(orcher_sdk_core::proto::prost_types::Duration {
                seconds: 30,
                nanos: 500_000_000,
            }),
            ..Default::default()
        };
        let json = task_json(&task);
        assert_eq!(json["token"], json!([1]));
        assert_eq!(json["input"], json!(b"{\"n\":1}"));
        assert_eq!(json["attempt"], 2);
        assert_eq!(json["attempt_timeout_ms"], 30_500);
        assert_eq!(json["heartbeat_timeout_ms"], serde_json::Value::Null);
        assert_eq!(json["namespace"], "ns");
    }
}
