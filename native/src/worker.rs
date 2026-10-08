//! Worker bindings for Python.
//!
//! ## Architecture: core-driven polling with multi-channel fan-out
//!
//! - The Rust SDK core does all gRPC polling, runs the state machines, and
//!   validates replay.
//! - Python only executes handlers, pulling work from this module.
//!
//! ### Multi-channel fan-out
//!
//! Each driver produces work on a single channel. A distributor fans it out to
//! one channel per poll slot, so parallel Python pollers never contend on a
//! shared lock:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                      SDK core (Rust)                                    │
//! │   WorkflowDriver produces work on a single channel                      │
//! └────────────────────────────┬────────────────────────────────────────────┘
//!                              │ Single channel from the SDK core
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                    Fan-Out Distributor (Rust FFI Layer)                 │
//! │   Distributes work round-robin to multiple slot channels                │
//! └────┬──────────┬──────────┬──────────┬──────────┬───────────────────────┘
//!      │          │          │          │          │
//!      ▼          ▼          ▼          ▼          ▼
//!   Slot 0     Slot 1     Slot 2     Slot 3     Slot N   (Independent channels)
//!      │          │          │          │          │
//!      ▼          ▼          ▼          ▼          ▼
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                     Python Polling Loops                                │
//! │   Each loop polls from its own slot - NO MUTEX CONTENTION               │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Properties
//!
//! - **No lock contention**: each Python polling loop has its own channel.
//! - **Parallelism**: polls on different slots run concurrently.
//! - **Fair distribution**: work is assigned to slots round-robin.
//! - **Scalable**: more slots give more parallelism.

use pyo3::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex};

use crate::actor_state;
use crate::journal_times::JournalTimes;
use orcher_sdk_core::bridge::{task_to_execution_request, ExecutionResult};
use orcher_sdk_core::poller::channel::ChannelManager;
use orcher_sdk_core::poller::{
    ActorDriver, ActorDriverConfig, ActorWorkResult, ShutdownHandle, TaskDriver, TaskDriverConfig,
    TaskWorkResult, WorkflowDriver, WorkflowDriverConfig, WorkflowWorkResult,
};

/// Reads an activation's result the way sdk-core does.
///
/// Fails with `ValueError` when sdk-core would reject it, which fails the
/// activation: every command in it is lost.
pub(crate) fn parse_execution_result(result_json: &str) -> PyResult<ExecutionResult> {
    serde_json::from_str(result_json).map_err(|e| {
        tracing::error!(
            error = %e,
            json_len = result_json.len(),
            json_preview = %&result_json[..result_json.len().min(500)],
            "Failed to parse ExecutionResult from Python SDK"
        );
        pyo3::exceptions::PyValueError::new_err(format!("Invalid result JSON: {}", e))
    })
}

// ============================================================================
// Worker configuration
// ============================================================================

/// Configuration for `BridgeWorker`.
#[pyclass(name = "WorkerConfig")]
#[derive(Clone)]
pub struct PyWorkerConfig {
    /// Server URL (e.g., "http://localhost:50051")
    #[pyo3(get, set)]
    pub server_url: String,
    /// Namespace (default: "default")
    #[pyo3(get, set)]
    pub namespace: String,
    /// Task queue to poll
    #[pyo3(get, set)]
    pub task_queue: String,
    /// Maximum concurrent workflow executions
    #[pyo3(get, set)]
    pub max_concurrent_workflows: usize,
    /// Maximum concurrent task executions
    #[pyo3(get, set)]
    pub max_concurrent_tasks: usize,
    /// Worker identity. Generated when not given.
    #[pyo3(get, set)]
    pub identity: Option<String>,
    /// Number of workflow pollers
    #[pyo3(get, set)]
    pub workflow_poller_count: usize,
    /// Number of task pollers
    #[pyo3(get, set)]
    pub task_poller_count: usize,
    /// Number of actor pollers
    #[pyo3(get, set)]
    pub actor_poller_count: usize,
    /// Maximum concurrent actor operations
    #[pyo3(get, set)]
    pub max_concurrent_actors: usize,
    /// Organization ID for multi-tenant servers (optional)
    #[pyo3(get, set)]
    pub organization_id: Option<String>,
    /// API key sent as `authorization: Bearer <key>` on every request the
    /// worker makes, for servers that require one. Never printed: `repr` and
    /// `Debug` redact it.
    #[pyo3(get, set)]
    pub api_key: Option<String>,
    /// Path to CA certificate PEM file for TLS (optional)
    #[pyo3(get, set)]
    pub tls_ca_cert_path: Option<String>,
    /// Path to client certificate PEM file for mTLS (optional)
    #[pyo3(get, set)]
    pub tls_client_cert_path: Option<String>,
    /// Path to client key PEM file for mTLS (optional)
    #[pyo3(get, set)]
    pub tls_client_key_path: Option<String>,
    /// The code release this worker is running (optional).
    ///
    /// Sent on every poll. The server binds an execution to it on first claim,
    /// so replay keeps running against the code the execution started on.
    /// Opaque: never parsed or ordered.
    #[pyo3(get, set)]
    pub version_id: Option<String>,
}

#[pymethods]
impl PyWorkerConfig {
    #[new]
    #[pyo3(signature = (
        server_url,
        task_queue,
        namespace = "default".to_string(),
        max_concurrent_workflows = 100,
        max_concurrent_tasks = 100,
        identity = None,
        workflow_poller_count = 2,
        task_poller_count = 4,
        actor_poller_count = 4,
        max_concurrent_actors = 100,
        organization_id = None,
        tls_ca_cert_path = None,
        tls_client_cert_path = None,
        tls_client_key_path = None,
        version_id = None,
        api_key = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        server_url: String,
        task_queue: String,
        namespace: String,
        max_concurrent_workflows: usize,
        max_concurrent_tasks: usize,
        identity: Option<String>,
        workflow_poller_count: usize,
        task_poller_count: usize,
        actor_poller_count: usize,
        max_concurrent_actors: usize,
        organization_id: Option<String>,
        tls_ca_cert_path: Option<String>,
        tls_client_cert_path: Option<String>,
        tls_client_key_path: Option<String>,
        version_id: Option<String>,
        api_key: Option<String>,
    ) -> PyResult<Self> {
        Ok(Self {
            server_url,
            namespace,
            task_queue,
            max_concurrent_workflows,
            max_concurrent_tasks,
            // Resolve the identity ONCE, here. get_identity() must return a
            // stable value: the actor driver's service_id and identity and
            // handler registration must all agree, or heartbeats never match
            // the registration and it goes stale on the server.
            identity: Some(identity.unwrap_or_else(Self::default_identity)),
            workflow_poller_count,
            task_poller_count,
            actor_poller_count,
            max_concurrent_actors,
            organization_id,
            tls_ca_cert_path,
            tls_client_cert_path,
            tls_client_key_path,
            // Empty or whitespace means "not declared". Otherwise an unset
            // environment variable would bind executions to a release named "",
            // which looks like a real release and hides that versioning is off.
            version_id: version_id.filter(|v| !v.trim().is_empty()),
            api_key: Self::checked_api_key(api_key)?,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkerConfig(server_url='{}', namespace='{}', task_queue='{}', api_key={})",
            self.server_url,
            self.namespace,
            self.task_queue,
            redacted(&self.api_key)
        )
    }
}

/// How a secret appears in `repr` and `Debug`: whether it is set, never what
/// it is.
fn redacted(secret: &Option<String>) -> &'static str {
    match secret {
        Some(_) => "'<redacted>'",
        None => "None",
    }
}

impl std::fmt::Debug for PyWorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConfig")
            .field("server_url", &self.server_url)
            .field("namespace", &self.namespace)
            .field("task_queue", &self.task_queue)
            .field("identity", &self.identity)
            .field("organization_id", &self.organization_id)
            .field("api_key", &format_args!("{}", redacted(&self.api_key)))
            .field("version_id", &self.version_id)
            .finish_non_exhaustive()
    }
}

impl PyWorkerConfig {
    /// An empty key means "none". Any other key must be sendable as a header,
    /// or sdk-core would silently drop it and the worker would connect with
    /// no credentials at all.
    fn checked_api_key(api_key: Option<String>) -> PyResult<Option<String>> {
        let Some(key) = api_key.filter(|k| !k.trim().is_empty()) else {
            return Ok(None);
        };
        // Parsed exactly as sdk-core parses it when it builds the header.
        if format!("Bearer {key}")
            .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
            .is_err()
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "api_key contains characters that cannot be sent in the authorization header",
            ));
        }
        Ok(Some(key))
    }

    /// The TLS settings for this config's server URL and `tls_*` paths; see
    /// [`crate::tls::tls_from_paths`].
    pub fn build_tls_config(
        &self,
    ) -> std::result::Result<Option<orcher_sdk_core::poller::TlsConfig>, String> {
        crate::tls::tls_from_paths(
            &self.server_url,
            self.tls_ca_cert_path.as_deref(),
            self.tls_client_cert_path.as_deref(),
            self.tls_client_key_path.as_deref(),
        )
    }

    /// Return the worker identity.
    ///
    /// The identity is resolved once in `new`, so this is stable. A new one
    /// is generated only if `identity` is set to None through the property
    /// setter after construction.
    pub fn get_identity(&self) -> String {
        self.identity.clone().unwrap_or_else(Self::default_identity)
    }

    /// Generate a default worker identity.
    fn default_identity() -> String {
        format!(
            "python-worker-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )
    }

    /// Build the workflow driver configuration.
    pub fn to_workflow_driver_config(
        &self,
        tls_config: Option<orcher_sdk_core::poller::TlsConfig>,
    ) -> WorkflowDriverConfig {
        let mut config = WorkflowDriverConfig::default();
        config.server_url = self.server_url.clone();
        config.namespace = self.namespace.clone();
        config.task_queue = self.task_queue.clone();
        config.max_concurrent_executions = self.max_concurrent_workflows;
        config.identity = self.get_identity();
        config.poll_timeout = Duration::from_secs(60);
        config.cache_capacity = 1000;
        config.strict_determinism = false;
        config.poller_count = self.workflow_poller_count;
        config.organization_id = self.organization_id.clone();
        config.api_key = self.api_key.clone();
        config.tls_config = tls_config.clone();
        config.version_id = self.version_id.clone();
        config
    }

    /// Build the actor driver configuration.
    pub fn to_actor_driver_config(
        &self,
        tls_config: Option<orcher_sdk_core::poller::TlsConfig>,
    ) -> ActorDriverConfig {
        let mut config = ActorDriverConfig::default();
        config.server_url = self.server_url.clone();
        config.namespace = self.namespace.clone();
        config.service_id = self.get_identity();
        config.max_concurrent_executions = self.max_concurrent_actors;
        config.identity = self.get_identity();
        config.poll_timeout = Duration::from_secs(30);
        config.poller_count = self.actor_poller_count;
        config.organization_id = self.organization_id.clone();
        config.api_key = self.api_key.clone();
        config.enable_heartbeat = true;
        config.heartbeat_interval = Duration::from_secs(10);
        config.registration_id = None;
        config.tls_config = tls_config.clone();
        config
    }

    /// Build the task driver configuration.
    pub fn to_task_driver_config(
        &self,
        tls_config: Option<orcher_sdk_core::poller::TlsConfig>,
    ) -> TaskDriverConfig {
        let mut config = TaskDriverConfig::default();
        config.server_url = self.server_url.clone();
        config.namespace = self.namespace.clone();
        config.task_queue = self.task_queue.clone();
        config.max_concurrent_executions = self.max_concurrent_tasks;
        config.identity = self.get_identity();
        config.poll_timeout = Duration::from_secs(60);
        config.enable_heartbeat = true;
        config.heartbeat_interval = Duration::from_secs(30);
        config.poller_count = self.task_poller_count;
        config.organization_id = self.organization_id.clone();
        config.api_key = self.api_key.clone();
        config.tls_config = tls_config;
        config.version_id = self.version_id.clone();
        // Every task is heartbeated while it runs, whatever its code does, so
        // a task whose worker dies is retried rather than left started.
        config.auto_heartbeat = true;
        config
    }
}

/// A task handed to Python whose outcome has not been reported yet.
///
/// `heartbeat` is where the task's code records heartbeats; heartbeating
/// stops once it is dropped. `reported` is cancelled when the outcome is
/// reported.
struct RunningTask {
    heartbeat: orcher_sdk_core::poller::TaskHeartbeat,
    reported: orcher_sdk_core::poller::CancellationToken,
}

/// The tasks handed to Python and not yet reported, by task token.
type RunningTasks = Arc<std::sync::Mutex<HashMap<Vec<u8>, RunningTask>>>;

/// Forget a task whose outcome Python has reported. Its heartbeat stops once
/// the driver has sent the outcome, and anything waiting on its cancellation
/// is released.
fn task_reported(running: &RunningTasks, token: &[u8]) {
    let task = running.lock().unwrap_or_else(|p| p.into_inner()).remove(token);
    if let Some(task) = task {
        task.reported.cancel();
    }
}

fn decode_task_token(task_token: &str) -> PyResult<Vec<u8>> {
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, task_token).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid task token: {}", e))
    })
}

// ============================================================================
// WorkerShutdownEvent: raised when a poll ends because the worker is stopping
// ============================================================================

pyo3::create_exception!(orcher, WorkerShutdownEvent, pyo3::exceptions::PyException);

// ============================================================================
// Multi-Channel Fan-Out Types
// ============================================================================

/// Workflow work serialized once, ready to hand to Python.
#[derive(Debug, Clone)]
struct SerializedWorkflowWork {
    json_bytes: Vec<u8>,
}

/// Task work serialized once, ready to hand to Python.
#[derive(Debug, Clone)]
struct SerializedTaskWork {
    json_bytes: Vec<u8>,
}

/// Fan-out state for workflow polling.
struct WorkflowFanOut {
    /// One receiver per slot, each locked independently.
    slot_receivers: Vec<Arc<Mutex<mpsc::Receiver<SerializedWorkflowWork>>>>,
    /// Shutdown receiver, read by `is_shutdown_requested`
    shutdown_rx: watch::Receiver<bool>,
}

/// Fan-out state for task polling.
struct TaskFanOut {
    /// One receiver per slot, each locked independently.
    slot_receivers: Vec<Arc<Mutex<mpsc::Receiver<SerializedTaskWork>>>>,
}

/// Actor work serialized once, ready to hand to Python.
#[derive(Debug, Clone)]
struct SerializedActorWork {
    json_bytes: Vec<u8>,
}

/// Fan-out state for actor polling.
struct ActorFanOut {
    /// One receiver per slot, each locked independently.
    slot_receivers: Vec<Arc<Mutex<mpsc::Receiver<SerializedActorWork>>>>,
    /// Shutdown receiver (cloned per slot when polling)
    shutdown_rx: watch::Receiver<bool>,
}

/// How long `stop_drivers` waits for each of the workflow and task drivers to
/// stop.
///
/// A driver finishes its whole shutdown, draining included, within a grace
/// period (five seconds by default) that starts when it is told to stop,
/// normally at `request_shutdown`. This timeout must stay above that grace
/// period, or it would cut the drain short. It exists for a driver that is
/// stuck elsewhere: shutdown must return even then.
const DRIVER_STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// A running driver: the handle that asks it to stop, and the task running it.
type RunningDriver = (ShutdownHandle, tokio::task::JoinHandle<()>);

/// The workflow and task drivers, as `stop_drivers` takes them.
struct DriverStops {
    workflow: RunningDriver,
    task: RunningDriver,
}

impl DriverStops {
    /// Ask both drivers to stop; they then finish within their shutdown grace.
    fn signal(&self) {
        self.workflow.0.shutdown();
        self.task.0.shutdown();
    }
}

/// Wait up to `DRIVER_STOP_TIMEOUT` for a driver already asked to stop,
/// abandoning it if it does not.
async fn await_driver(driver: &str, mut run: tokio::task::JoinHandle<()>) {
    if tokio::time::timeout(DRIVER_STOP_TIMEOUT, &mut run)
        .await
        .is_err()
    {
        tracing::warn!(
            timeout_ms = DRIVER_STOP_TIMEOUT.as_millis() as u64,
            "{driver} driver did not stop in time; abandoning it"
        );
        run.abort();
    }
}

// ============================================================================
// Worker senders: cloneable, for sending results concurrently
// ============================================================================

/// Senders that return results to the core drivers. Cloneable, so results
/// can be sent concurrently.
#[derive(Clone)]
struct WorkerSenders {
    /// Workflow results, to the workflow driver.
    workflow_result_tx: mpsc::Sender<WorkflowWorkResult>,
    /// Task results, to the task driver.
    task_result_tx: mpsc::Sender<TaskWorkResult>,
    /// Actor results, to the actor driver. None when actors are disabled.
    actor_result_tx: Option<mpsc::Sender<ActorWorkResult>>,
}

// ============================================================================
// BridgeWorker
// ============================================================================

/// Connects Python handlers to the core SDK's workflow, task, and actor drivers.
///
/// - The drivers (WorkflowDriver, TaskDriver, ActorDriver) do all gRPC and run
///   the state machines.
/// - Fan-out distributors route work to independent slot channels.
/// - Each Python polling loop has its own slot, so polls do not contend.
#[pyclass(name = "BridgeWorker")]
pub struct PyBridgeWorker {
    /// Workflow polling fan-out (multiple independent channels)
    workflow_fan_out: Arc<Mutex<Option<WorkflowFanOut>>>,
    /// Task polling fan-out (multiple independent channels)
    task_fan_out: Arc<Mutex<Option<TaskFanOut>>>,
    /// Actor polling fan-out (multiple independent channels)
    actor_fan_out: Arc<Mutex<Option<ActorFanOut>>>,
    /// Result senders. Cloned out under a short read lock, never held across
    /// an await.
    senders: Arc<std::sync::RwLock<Option<WorkerSenders>>>,
    /// Signals shutdown to the actor distributor and polls.
    shutdown_tx: Arc<std::sync::Mutex<Option<watch::Sender<bool>>>>,
    /// Stops the workflow and task drivers, and the tasks running them. Taken
    /// once, by `stop_drivers`.
    driver_stops: Arc<std::sync::Mutex<Option<DriverStops>>>,
    /// Number of workflow poll slots
    workflow_slot_count: AtomicUsize,
    /// Number of task poll slots
    task_slot_count: AtomicUsize,
    /// Number of actor poll slots
    actor_slot_count: AtomicUsize,
    /// Shared gRPC connection for actor RPCs (state, registration, invoke).
    state_channel_manager: actor_state::StateChannels,
    /// Server URL.
    #[allow(dead_code)]
    server_url: String,
    /// Stable worker identity, shared by the actor driver (service_id,
    /// heartbeats) and handler registration so the server can correlate them.
    service_id: String,
    /// Lets Python add and remove session queues at run time.
    session_queue_tx: Arc<std::sync::Mutex<Option<mpsc::UnboundedSender<orcher_sdk_core::poller::SessionQueueChange>>>>,
    /// The tasks handed to Python and not yet reported.
    running_tasks: RunningTasks,
}

#[pymethods]
impl PyBridgeWorker {
    /// Create a bridge worker and start its drivers.
    ///
    /// Creates the workflow and task drivers (and the actor driver when
    /// `actor_poller_count > 0`), and starts distributors that fan their work
    /// out to independent slot channels.
    #[new]
    fn new(_py: Python<'_>, config: PyWorkerConfig) -> PyResult<Self> {
        let workflow_poller_count = config.workflow_poller_count;
        let task_poller_count = config.task_poller_count;
        let actor_poller_count = config.actor_poller_count;
        let namespace = config.namespace.clone();
        let server_url = config.server_url.clone();
        let service_id = config.get_identity();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let workflow_slot_count = AtomicUsize::new(workflow_poller_count);
        let task_slot_count = AtomicUsize::new(task_poller_count);
        let actor_slot_count = AtomicUsize::new(actor_poller_count);

        tracing::info!(
            "Creating BridgeWorker with {} workflow slots, {} task slots, {} actor slots (multi-channel fan-out)",
            workflow_poller_count,
            task_poller_count,
            actor_poller_count
        );

        // Read the TLS files before entering the async context.
        let credentials = actor_state::Credentials {
            api_key: config.api_key.clone(),
            organization_id: config.organization_id.clone(),
        };

        let tls_config = config
            .build_tls_config()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?;

        let workflow_driver_config = config.to_workflow_driver_config(tls_config.clone());
        let task_driver_config = config.to_task_driver_config(tls_config.clone());
        let actor_driver_config = config.to_actor_driver_config(tls_config.clone());

        let runtime = pyo3_async_runtimes::tokio::get_runtime();
        let running_tasks: RunningTasks = Arc::default();
        let distributed_tasks = Arc::clone(&running_tasks);

        let (
            workflow_fan_out,
            task_fan_out,
            actor_fan_out,
            worker_senders,
            session_queue_tx,
            driver_stops,
        ) = runtime.block_on(async {
                let rt = tokio::runtime::Handle::current();

                let (workflow_driver, workflow_work_rx, workflow_result_tx) =
                    WorkflowDriver::new(workflow_driver_config)
                        .await
                        .map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "Failed to create workflow driver: {}",
                                e
                            ))
                        })?;

                tracing::info!("Workflow driver created successfully");

                let (mut task_driver, task_work_rx, task_result_tx, session_queue_tx) =
                    TaskDriver::new(task_driver_config).await.map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "Failed to create task driver: {}",
                            e
                        ))
                    })?;

                tracing::info!("Task driver created successfully");

                // ============================================================
                // Workflow fan-out
                // ============================================================
                let mut workflow_slot_receivers = Vec::with_capacity(workflow_poller_count);
                let mut workflow_senders = Vec::with_capacity(workflow_poller_count);

                for _ in 0..workflow_poller_count {
                    let (tx, rx) = mpsc::channel::<SerializedWorkflowWork>(10);
                    workflow_slot_receivers.push(Arc::new(Mutex::new(rx)));
                    workflow_senders.push(tx);
                }

                let workflow_fan_out = WorkflowFanOut {
                    slot_receivers: workflow_slot_receivers,
                    shutdown_rx: shutdown_rx.clone(),
                };

                // ============================================================
                // Task fan-out
                // ============================================================
                let mut task_slot_receivers = Vec::with_capacity(task_poller_count);
                let mut task_senders = Vec::with_capacity(task_poller_count);

                for _ in 0..task_poller_count {
                    let (tx, rx) = mpsc::channel::<SerializedTaskWork>(10);
                    task_slot_receivers.push(Arc::new(Mutex::new(rx)));
                    task_senders.push(tx);
                }

                let task_fan_out = TaskFanOut {
                    slot_receivers: task_slot_receivers,
                };

                // ============================================================
                // Actor driver and fan-out (only when actor pollers are configured)
                // ============================================================
                let (actor_fan_out, actor_result_tx_opt) = if actor_poller_count > 0 {
                    let (mut actor_driver, actor_work_rx, actor_result_tx, _actor_event_rx) =
                        ActorDriver::new(actor_driver_config).await.map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "Failed to create actor driver: {}",
                                e
                            ))
                        })?;

                    tracing::info!("Actor driver created successfully");

                    let mut actor_slot_receivers_vec = Vec::with_capacity(actor_poller_count);
                    let mut actor_senders_vec = Vec::with_capacity(actor_poller_count);

                    for _ in 0..actor_poller_count {
                        let (tx, rx) = mpsc::channel::<SerializedActorWork>(10);
                        actor_slot_receivers_vec.push(Arc::new(Mutex::new(rx)));
                        actor_senders_vec.push(tx);
                    }

                    let fan_out = ActorFanOut {
                        slot_receivers: actor_slot_receivers_vec,
                        shutdown_rx: shutdown_rx.clone(),
                    };

                    // Unlike the workflow and task distributors, the actor
                    // distributor stops on the shutdown signal.
                    let actor_shutdown_rx = shutdown_rx.clone();
                    let mut actor_work_rx = actor_work_rx;

                    rt.spawn(async move {
                        tracing::info!(
                            "Actor distributor started with {} slots",
                            actor_senders_vec.len()
                        );
                        let mut slot_idx = 0usize;
                        let mut shutdown_watch = actor_shutdown_rx;
                        let _ = *shutdown_watch.borrow_and_update();

                        loop {
                            tokio::select! {
                                biased;

                                result = shutdown_watch.changed() => {
                                    if result.is_err() || *shutdown_watch.borrow_and_update() {
                                        tracing::info!("Actor distributor shutdown");
                                        break;
                                    }
                                }

                                work = actor_work_rx.recv() => {
                                    match work {
                                        Some(actor_work) => {
                                            let op = &actor_work.operation;
                                            let actor_request = serde_json::json!({
                                                "operation_id": op.operation_id,
                                                "actor_name": op.actor_name,
                                                "key": op.key,
                                                "operation": op.operation,
                                                "payload": base64::Engine::encode(
                                                    &base64::engine::general_purpose::STANDARD,
                                                    &op.payload
                                                ),
                                                "mode": op.mode,
                                                "execution_id": op.execution_id,
                                                "metadata": op.metadata,
                                            });

                                            let json_bytes = match serde_json::to_vec(&actor_request) {
                                                Ok(b) => b,
                                                Err(e) => {
                                                    tracing::error!("Failed to serialize actor work: {}", e);
                                                    continue;
                                                }
                                            };

                                            let serialized = SerializedActorWork { json_bytes };
                                            let target_slot = slot_idx % actor_senders_vec.len();
                                            slot_idx = slot_idx.wrapping_add(1);

                                            if let Err(e) = actor_senders_vec[target_slot].send(serialized).await {
                                                tracing::error!("Failed to send to actor slot {}: {}", target_slot, e);
                                            }
                                        }
                                        None => {
                                            tracing::info!("Actor work channel closed");
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    });

                    rt.spawn(async move {
                        if let Err(e) = actor_driver.run().await {
                            tracing::error!(error = %e, "Actor driver error");
                        }
                    });

                    (Some(fan_out), Some(actor_result_tx))
                } else {
                    (None, None)
                };

                let worker_senders = WorkerSenders {
                    workflow_result_tx,
                    task_result_tx,
                    actor_result_tx: actor_result_tx_opt,
                };

                // ============================================================
                // Workflow distributor
                // ============================================================
                let mut workflow_work_rx = workflow_work_rx;

                // Unlike the actor distributor, this one does not stop on the
                // shutdown signal. During its shutdown grace period the
                // workflow driver keeps handing over what its polls bring
                // back, and each of those activations is already claimed on
                // the engine: dropped here, it would wait out the claim
                // timeout. So it forwards until the driver has stopped and
                // dropped its sender, and only then drops the slot senders,
                // which is what ends the Python workflow pollers.
                rt.spawn(async move {
                    tracing::info!(
                        "Workflow distributor started with {} slots",
                        workflow_senders.len()
                    );
                    let mut slot_idx = 0usize;

                    while let Some(workflow_work) = workflow_work_rx.recv().await {
                        // Serialize the work once
                        let exec_request = match task_to_execution_request(workflow_work.task.clone()) {
                            Ok(req) => req,
                            Err(e) => {
                                // Nothing will answer this activation, so it
                                // stays claimed until the engine's claim
                                // timeout. Log which one.
                                tracing::error!(
                                    workflow_id = %workflow_work.task.execution.workflow_id,
                                    run_id = %workflow_work.task.execution.run_id,
                                    "Failed to convert workflow task; activation dropped: {}",
                                    e
                                );
                                continue;
                            }
                        };

                        // The jobs carry no times; the workflow's clock is
                        // read from the journal they came from.
                        let mut execution_request = match serde_json::to_value(&exec_request) {
                            Ok(value) => value,
                            Err(e) => {
                                tracing::error!(
                                    workflow_id = %workflow_work.task.execution.workflow_id,
                                    run_id = %workflow_work.task.execution.run_id,
                                    "Failed to serialize workflow task; activation dropped: {}",
                                    e
                                );
                                continue;
                            }
                        };
                        if let Some(request) = execution_request.as_object_mut() {
                            request.insert(
                                "journal_times".to_string(),
                                serde_json::json!(JournalTimes::read(&workflow_work.task.journal)),
                            );
                        }

                        let response = serde_json::json!({
                            "execution_request": execution_request,
                            "workflow_id": workflow_work.task.execution.workflow_id,
                            "run_id": workflow_work.task.execution.run_id,
                            "task_token": base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                &workflow_work.task.task_token
                            ),
                            "stream_entry_id": workflow_work.stream_entry_id,
                        });

                        let json_bytes = match serde_json::to_vec(&response) {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::error!(
                                    workflow_id = %workflow_work.task.execution.workflow_id,
                                    run_id = %workflow_work.task.execution.run_id,
                                    "Failed to serialize workflow work; activation dropped: {}",
                                    e
                                );
                                continue;
                            }
                        };

                        let serialized = SerializedWorkflowWork { json_bytes };

                        // Round-robin across slots.
                        let target_slot = slot_idx % workflow_senders.len();
                        slot_idx = slot_idx.wrapping_add(1);

                        tracing::debug!(
                            "Distributing workflow {} to slot {}",
                            workflow_work.task.execution.workflow_id,
                            target_slot
                        );

                        if let Err(e) = workflow_senders[target_slot].send(serialized).await {
                            tracing::error!(
                                workflow_id = %workflow_work.task.execution.workflow_id,
                                run_id = %workflow_work.task.execution.run_id,
                                "Failed to send to workflow slot {}; activation dropped: {}",
                                target_slot,
                                e
                            );
                        }
                    }
                    tracing::info!("Workflow work channel closed");
                });

                // ============================================================
                // Task distributor
                // ============================================================
                // Like the workflow distributor, this one does not stop on the
                // shutdown signal. During its shutdown grace period the task
                // driver keeps handing over what its polls bring back, and
                // each of those tasks is already claimed on the engine:
                // dropped here, it would wait out its start-to-close timeout.
                // So it forwards until the driver has stopped and dropped its
                // sender, and only then drops the slot senders, which is what
                // ends the Python task pollers.
                let mut task_work_rx = task_work_rx;
                let task_namespace = namespace.clone();
                let running = distributed_tasks;

                rt.spawn(async move {
                    tracing::info!(
                        "🟢 Task distributor started with {} slots",
                        task_senders.len()
                    );
                    let mut slot_idx = 0usize;
                    loop {
                        match task_work_rx.recv().await {
                            Some(task_work) => {
                                let task = &task_work.task;

                                // The input is JSON-encoded bytes from the
                                // workflow. Empty or invalid input becomes {}.
                                let task_input: serde_json::Value = if task.input.is_empty() {
                                    serde_json::Value::Object(serde_json::Map::new())
                                } else {
                                    match serde_json::from_slice(&task.input) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            tracing::error!("Failed to deserialize task input: {}", e);
                                            serde_json::Value::Object(serde_json::Map::new())
                                        }
                                    }
                                };

                                let task_request = serde_json::json!({
                                    "task_id": task.task_id,
                                    "task_type": task.task_type,
                                    "task_token": base64::Engine::encode(
                                        &base64::engine::general_purpose::STANDARD,
                                        &task.task_token
                                    ),
                                    "workflow_id": task.execution.workflow_id,
                                    "execution_id": task.execution.run_id,
                                    "task_queue": task.task_queue,
                                    "namespace": task_namespace,
                                    "input": task_input,
                                    "attempt": task.attempt,
                                    // The limits this task runs under, so a handler can
                                    // pace its heartbeats against the interval it is
                                    // judged by. Milliseconds, or null when the engine
                                    // set no limit.
                                    "heartbeat_timeout_ms": task
                                        .heartbeat_timeout
                                        .map(|d| d.as_millis() as u64),
                                    "start_to_close_timeout_ms": task
                                        .start_to_close_timeout
                                        .map(|d| d.as_millis() as u64),
                                });

                                let json_bytes = match serde_json::to_vec(&task_request) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        tracing::error!("Failed to serialize task work: {}", e);
                                        continue;
                                    }
                                };

                                let serialized = SerializedTaskWork { json_bytes };
                                // Tracked until Python reports the task, so it
                                // is heartbeated while it runs.
                                running.lock().unwrap_or_else(|p| p.into_inner()).insert(
                                    task.task_token.clone(),
                                    RunningTask {
                                        heartbeat: task_work.heartbeat.clone(),
                                        reported: Default::default(),
                                    },
                                );

                                // Round-robin across slots.
                                let target_slot = slot_idx % task_senders.len();
                                slot_idx = slot_idx.wrapping_add(1);

                                tracing::debug!(
                                    "🟢 Distributing task {} to slot {}",
                                    task.task_id,
                                    target_slot
                                );

                                if let Err(e) = task_senders[target_slot].send(serialized).await {
                                    tracing::error!("Failed to send to task slot {}: {}", target_slot, e);
                                    task_reported(&running, &task_work.task.task_token);
                                }
                            }
                            None => {
                                tracing::info!("🟢 Task work channel closed");
                                break;
                            }
                        }
                    }
                });

                // ============================================================
                // Drivers
                // ============================================================

                // Eager task injection: tasks scheduled by a workflow completion go
                // straight into the task driver's work channel, skipping a poll
                // round-trip.
                let mut workflow_driver = workflow_driver
                    .with_eager_task_injector(task_driver.eager_task_injector());

                // Take the stop handle before the driver moves into its task:
                // `run` borrows the driver while it runs, and stopping it
                // through the handle is what lets it send the results it holds
                // instead of dropping them.
                let workflow_stop = workflow_driver.shutdown_handle();
                let workflow_run = rt.spawn(async move {
                    if let Err(e) = workflow_driver.run().await {
                        tracing::error!(error = %e, "Workflow driver error");
                    }
                });

                let task_stop = task_driver.shutdown_handle();
                let task_run = rt.spawn(async move {
                    if let Err(e) = task_driver.run().await {
                        tracing::error!(error = %e, "Task driver error");
                    }
                });

                Ok::<_, PyErr>((
                    workflow_fan_out,
                    task_fan_out,
                    actor_fan_out,
                    worker_senders,
                    session_queue_tx,
                    DriverStops {
                        workflow: (workflow_stop, workflow_run),
                        task: (task_stop, task_run),
                    },
                ))
            })?;

        // Connection for actor RPCs, with the same TLS settings as the drivers.
        let state_cm = match tls_config {
            Some(ref tls) => ChannelManager::with_tls(server_url.clone(), tls.clone()),
            None => ChannelManager::new(server_url.clone()),
        };

        Ok(Self {
            workflow_fan_out: Arc::new(Mutex::new(Some(workflow_fan_out))),
            task_fan_out: Arc::new(Mutex::new(Some(task_fan_out))),
            actor_fan_out: Arc::new(Mutex::new(actor_fan_out)),
            senders: Arc::new(std::sync::RwLock::new(Some(worker_senders))),
            shutdown_tx: Arc::new(std::sync::Mutex::new(Some(shutdown_tx))),
            driver_stops: Arc::new(std::sync::Mutex::new(Some(driver_stops))),
            workflow_slot_count,
            task_slot_count,
            actor_slot_count,
            state_channel_manager: actor_state::StateChannels::new(state_cm, credentials),
            server_url,
            service_id,
            session_queue_tx: Arc::new(std::sync::Mutex::new(Some(session_queue_tx))),
            running_tasks,
        })
    }

    /// Record a heartbeat from a running task's code.
    ///
    /// A heartbeat says the task is still making progress; `details`, when
    /// given, say how far it has got. Never waits: the worker heartbeats every
    /// task on its own, and this is sent with the next heartbeat due. Returns
    /// whether the task has been asked to stop, or `False` for a task this
    /// worker is not running.
    #[pyo3(signature = (task_token, details = None))]
    fn heartbeat_task(&self, task_token: String, details: Option<Vec<u8>>) -> PyResult<bool> {
        let token = decode_task_token(&task_token)?;
        let running = self.running_tasks.lock().unwrap_or_else(|p| p.into_inner());
        Ok(match running.get(&token) {
            Some(task) => {
                task.heartbeat.record(details);
                task.heartbeat.is_cancelled()
            }
            None => false,
        })
    }

    /// Wait until the task is asked to stop or its outcome is reported.
    ///
    /// Resolves `True` once the engine asks the task to stop (its workflow was
    /// cancelled, or the attempt is no longer running), and `False` once the
    /// task's outcome is reported. Resolves `False` at once for a task this
    /// worker is not running.
    fn wait_task_cancelled<'py>(
        &self,
        py: Python<'py>,
        task_token: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let token = decode_task_token(&task_token)?;
        let watched = self
            .running_tasks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&token)
            .map(|task| (task.heartbeat.cancellation_token(), task.reported.clone()));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(match watched {
                Some((cancelled, reported)) => tokio::select! {
                    _ = cancelled.cancelled() => true,
                    _ = reported.cancelled() => false,
                },
                None => false,
            })
        })
    }

    /// Register actor handlers (with per-operation modes) with the server.
    ///
    /// `handlers_json` is a JSON array of `{"actor_name", "operation", "mode"}`
    /// entries where `mode` is `"exclusive"` or `"shared"`; `metadata_json` is
    /// a JSON object of string pairs. Registration is what lets the server
    /// resolve an operation's concurrency mode; without it, every operation
    /// runs exclusive, the conservative default.
    ///
    /// Returns the server-issued `registration_id`.
    fn register_actor_handlers<'py>(
        &self,
        py: Python<'py>,
        handlers_json: String,
        metadata_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let channel_manager = self.state_channel_manager.clone();
        let service_id = self.service_id.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            use orcher_sdk_core::proto::orcher::v1::{
                ActorHandler, OperationMode,
                RegisterHandlersRequest,
            };

            let entries: Vec<serde_json::Value> =
                serde_json::from_str(&handlers_json).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "Invalid handlers JSON: {}",
                        e
                    ))
                })?;

            let handlers: Vec<ActorHandler> = entries
                .iter()
                .map(|h| {
                    let mode = match h.get("mode").and_then(|m| m.as_str()) {
                        Some("shared") => OperationMode::Shared as i32,
                        _ => OperationMode::Exclusive as i32,
                    };
                    ActorHandler {
                        actor_name: h
                            .get("actor_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        operation: h
                            .get("operation")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        mode,
                        metadata: std::collections::HashMap::new(),
                        ..Default::default()
                    }
                })
                .collect();

            let metadata: std::collections::HashMap<String, String> =
                serde_json::from_str(&metadata_json).unwrap_or_default();

            // The client is taken off the shared channel without holding its
            // lock across the RPC, so other bridge RPCs never queue behind it.
            let mut client = actor_state::client(&channel_manager).await?;
            let request = tonic::Request::new(RegisterHandlersRequest {
                service_id,
                handlers,
                metadata,
                ..Default::default()
            });

            let response = client.register_handlers(request).await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "RegisterHandlers RPC error: {}",
                    e
                ))
            })?;

            let resp = response.into_inner();
            if !resp.success {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Handler registration failed: {}",
                    resp.error_message
                )));
            }

            Ok(resp.registration_id)
        })
    }

    /// Invoke an actor operation and wait for its result.
    ///
    /// This is the client-side entry point (InvokeOperation RPC): the server queues
    /// the operation under its concurrency rules, a worker executes it, and
    /// the serialized result bytes come back. Raises on execution failure.
    #[pyo3(signature = (actor_name, key, operation, payload, timeout_ms=None))]
    fn invoke_actor_operation<'py>(
        &self,
        py: Python<'py>,
        actor_name: String,
        key: String,
        operation: String,
        payload: Vec<u8>,
        timeout_ms: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let channel_manager = self.state_channel_manager.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            use orcher_sdk_core::proto::orcher::v1::{
                ExecutionStatus,
                InvokeOperationRequest,
            };

            // This await can last the whole operation (seconds), and the
            // executing operation's own state RPCs need the same channel. The
            // client is taken off it without holding its lock across the RPC,
            // so they are not deadlocked behind it.
            let mut client = actor_state::client(&channel_manager).await?;
            let request = tonic::Request::new(InvokeOperationRequest {
                actor_name,
                key,
                operation,
                payload,
                timeout_ms: timeout_ms.unwrap_or(30_000),
                idempotency_key: String::new(),
                metadata: std::collections::HashMap::new(),
                ..Default::default()
            });

            let response = client.invoke_operation(request).await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "InvokeOperation RPC error: {}",
                    e
                ))
            })?;

            let resp = response.into_inner();
            if resp.status != ExecutionStatus::Success as i32 {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Actor operation failed: {} ({})",
                    resp.error_message, resp.error_code
                )));
            }

            Ok(resp.result)
        })
    }

    /// Poll for a workflow activation from a specific slot.
    ///
    /// Each Python poller should use a different slot_index to avoid contention.
    /// Returns bytes (a JSON-encoded ExecutionRequest), or None when no work
    /// arrives within 30 seconds. Raises WorkerShutdownEvent once the slot
    /// channel closes.
    #[pyo3(signature = (slot_index = 0))]
    fn poll_workflow_task<'py>(
        &self,
        py: Python<'py>,
        slot_index: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let workflow_fan_out = self.workflow_fan_out.clone();
        let slot_count = self.workflow_slot_count.load(Ordering::SeqCst);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if slot_index >= slot_count {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid slot index {}, max is {}",
                    slot_index,
                    slot_count.saturating_sub(1)
                )));
            }

            let fan_out_guard = workflow_fan_out.lock().await;
            let fan_out = fan_out_guard
                .as_ref()
                .ok_or_else(|| WorkerShutdownEvent::new_err("Worker not started"))?;

            // No shutdown check here, unlike the actor poll. During shutdown
            // the workflow driver still hands over what its polls bring back,
            // already claimed on the engine, and this is the only way it
            // reaches Python. So a workflow poll ends only when its slot
            // channel closes, once the driver has stopped.
            let slot_receiver = fan_out.slot_receivers[slot_index].clone();

            // Release the fan-out lock before awaiting, so slots do not block
            // each other.
            drop(fan_out_guard);

            let mut receiver = slot_receiver.lock().await;

            tokio::select! {
                work = receiver.recv() => {
                    match work {
                        Some(serialized_work) => {
                            tracing::debug!(
                                "poll_workflow_task slot {}: Received work",
                                slot_index
                            );
                            Ok(Some(serialized_work.json_bytes))
                        }
                        None => {
                            Err(WorkerShutdownEvent::new_err("Workflow slot channel closed"))
                        }
                    }
                }

                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    // No work within the poll window.
                    Ok(None)
                }
            }
        })
    }

    /// Poll for a task from a specific slot.
    ///
    /// Each Python poller should use a different slot_index to avoid contention.
    /// Returns bytes (a JSON-encoded task request), or None when no work
    /// arrives within 30 seconds. Raises WorkerShutdownEvent once the slot
    /// channel closes.
    #[pyo3(signature = (slot_index = 0))]
    fn poll_task<'py>(&self, py: Python<'py>, slot_index: usize) -> PyResult<Bound<'py, PyAny>> {
        let task_fan_out = self.task_fan_out.clone();
        let slot_count = self.task_slot_count.load(Ordering::SeqCst);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if slot_index >= slot_count {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid slot index {}, max is {}",
                    slot_index,
                    slot_count.saturating_sub(1)
                )));
            }

            let fan_out_guard = task_fan_out.lock().await;
            let fan_out = fan_out_guard
                .as_ref()
                .ok_or_else(|| WorkerShutdownEvent::new_err("Worker not started"))?;

            // No shutdown check here, for the same reason as the workflow
            // poll: during shutdown the task driver still hands over what its
            // polls bring back, already claimed on the engine, so a task poll
            // ends only when its slot channel closes, once the driver has
            // stopped.
            let slot_receiver = fan_out.slot_receivers[slot_index].clone();

            // Release the fan-out lock before awaiting.
            drop(fan_out_guard);

            let mut receiver = slot_receiver.lock().await;

            tokio::select! {
                work = receiver.recv() => {
                    match work {
                        Some(serialized_work) => {
                            tracing::debug!(
                                "🟢 POLL_TASK slot {}: Received work",
                                slot_index
                            );
                            Ok(Some(serialized_work.json_bytes))
                        }
                        None => {
                            Err(WorkerShutdownEvent::new_err("Task slot channel closed"))
                        }
                    }
                }

                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    // No work within the poll window.
                    Ok(None)
                }
            }
        })
    }

    /// Number of workflow polling slots.
    fn get_workflow_slot_count(&self) -> usize {
        self.workflow_slot_count.load(Ordering::SeqCst)
    }

    /// Number of task polling slots.
    fn get_task_slot_count(&self) -> usize {
        self.task_slot_count.load(Ordering::SeqCst)
    }

    /// Report the result of a workflow activation.
    ///
    /// The result goes to the workflow driver, which sends the completion to
    /// the server.
    #[pyo3(signature = (workflow_id, execution_id, result_json, task_token, stream_entry_id=None))]
    fn complete_workflow_task<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        execution_id: String,
        result_json: String,
        task_token: String,
        stream_entry_id: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let senders = self.senders.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Clone the sender out so the lock is never held across an await.
            let tx = {
                let guard = senders.read().unwrap();
                let worker_senders = guard.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Worker is shutdown")
                })?;
                worker_senders.workflow_result_tx.clone()
            };

            let exec_result = parse_execution_result(&result_json)?;

            let task_token_bytes =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &task_token)
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "Invalid task token: {}",
                            e
                        ))
                    })?;

            let work_result = WorkflowWorkResult {
                workflow_id,
                run_id: execution_id,
                task_token: task_token_bytes,
                stream_entry_id,
                result: Ok(exec_result),
            };

            tx.send(work_result).await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to send result to sdk-core: {}",
                    e
                ))
            })?;

            Ok(())
        })
    }

    /// Report that a workflow activation failed.
    ///
    /// `_error_type` is accepted but not forwarded.
    fn fail_workflow_task<'py>(
        &self,
        py: Python<'py>,
        workflow_id: String,
        execution_id: String,
        task_token: String,
        error_message: String,
        error_type: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Accepted under the name the worker passes it by; sdk-core reports
        // an activation failed this way as an internal error whatever its type.
        let _ = error_type;
        let senders = self.senders.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Clone the sender out so the lock is never held across an await.
            let tx = {
                let guard = senders.read().unwrap();
                let worker_senders = guard.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Worker is shutdown")
                })?;
                worker_senders.workflow_result_tx.clone()
            };

            let task_token_bytes =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &task_token)
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "Invalid task token: {}",
                            e
                        ))
                    })?;

            let work_result = WorkflowWorkResult {
                workflow_id,
                run_id: execution_id,
                task_token: task_token_bytes,
                stream_entry_id: None,
                result: Err(orcher_sdk_core::Error::internal(error_message)),
            };

            tx.send(work_result).await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to send error to sdk-core: {}",
                    e
                ))
            })?;

            Ok(())
        })
    }

    /// Report a task's successful result to the task driver.
    fn complete_task<'py>(
        &self,
        py: Python<'py>,
        task_token: String,
        result_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let senders = self.senders.clone();
        let running = Arc::clone(&self.running_tasks);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Clone the sender out so the lock is never held across an await.
            let tx = {
                let guard = senders.read().unwrap();
                let worker_senders = guard.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Worker is shutdown")
                })?;
                worker_senders.task_result_tx.clone()
            };

            let task_token_bytes =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &task_token)
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "Invalid task token: {}",
                            e
                        ))
                    })?;

            let work_result = TaskWorkResult {
                task_token: task_token_bytes,
                result: Ok(result_json.into_bytes()),
            };

            let token = work_result.task_token.clone();
            let sent = tx.send(work_result).await;
            task_reported(&running, &token);
            sent.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to send result to sdk-core: {}",
                    e
                ))
            })?;

            Ok(())
        })
    }

    /// Report a task failure to the task driver.
    #[pyo3(signature = (task_token, error_message, error_type, non_retryable = false))]
    fn fail_task<'py>(
        &self,
        py: Python<'py>,
        task_token: String,
        error_message: String,
        error_type: String,
        non_retryable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let senders = self.senders.clone();
        let running = Arc::clone(&self.running_tasks);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Clone the sender out so the lock is never held across an await.
            let tx = {
                let guard = senders.read().unwrap();
                let worker_senders = guard.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Worker is shutdown")
                })?;
                worker_senders.task_result_tx.clone()
            };

            let task_token_bytes =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &task_token)
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "Invalid task token: {}",
                            e
                        ))
                    })?;

            // The error type and non-retryable flag go to the engine with the
            // failure; the engine uses them to decide whether to retry.
            let work_result = TaskWorkResult {
                task_token: task_token_bytes,
                result: Err(orcher_sdk_core::TaskFailure::new(error_message)
                    .with_type(error_type)
                    .with_non_retryable(non_retryable)
                    .into()),
            };

            let token = work_result.task_token.clone();
            let sent = tx.send(work_result).await;
            task_reported(&running, &token);
            sent.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to send error to sdk-core: {}",
                    e
                ))
            })?;

            Ok(())
        })
    }

    // ========================================================================
    // Actor Operations
    // ========================================================================

    /// Poll for an actor operation from a specific slot.
    ///
    /// Returns bytes (a JSON-encoded operation), or None when no work arrives
    /// within 30 seconds. Raises WorkerShutdownEvent once shutdown is
    /// requested.
    #[pyo3(signature = (slot_index = 0))]
    fn poll_actor_operation<'py>(
        &self,
        py: Python<'py>,
        slot_index: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let actor_fan_out = self.actor_fan_out.clone();
        let slot_count = self.actor_slot_count.load(Ordering::SeqCst);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if slot_index >= slot_count {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid actor slot index {}, max is {}",
                    slot_index,
                    slot_count.saturating_sub(1)
                )));
            }

            let fan_out_guard = actor_fan_out.lock().await;
            let fan_out = fan_out_guard
                .as_ref()
                .ok_or_else(|| WorkerShutdownEvent::new_err("Actor driver not started"))?;

            if *fan_out.shutdown_rx.borrow() {
                return Err(WorkerShutdownEvent::new_err("Worker is shutting down"));
            }

            let slot_receiver = fan_out.slot_receivers[slot_index].clone();
            let mut shutdown_rx = fan_out.shutdown_rx.clone();
            let _ = *shutdown_rx.borrow_and_update();

            drop(fan_out_guard);

            let mut receiver = slot_receiver.lock().await;

            tokio::select! {
                biased;

                result = shutdown_rx.changed() => {
                    if result.is_err() || *shutdown_rx.borrow_and_update() {
                        return Err(WorkerShutdownEvent::new_err("Worker is shutting down"));
                    }
                    Ok(None)
                }

                work = receiver.recv() => {
                    match work {
                        Some(serialized_work) => {
                            Ok(Some(serialized_work.json_bytes))
                        }
                        None => {
                            Err(WorkerShutdownEvent::new_err("Actor slot channel closed"))
                        }
                    }
                }

                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    Ok(None)
                }
            }
        })
    }

    /// Report an actor operation's result to the actor driver.
    fn complete_actor_operation<'py>(
        &self,
        py: Python<'py>,
        operation_id: String,
        execution_id: String,
        result_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let senders = self.senders.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tx = {
                let guard = senders.read().unwrap();
                let worker_senders = guard.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Worker is shutdown")
                })?;
                worker_senders.actor_result_tx.clone().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Actor driver not initialized")
                })?
            };

            let result_bytes = result_json.into_bytes();

            let work_result = ActorWorkResult {
                operation_id,
                execution_id,
                result: Ok(result_bytes),
            };

            tx.send(work_result).await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to send actor result to sdk-core: {}",
                    e
                ))
            })?;

            Ok(())
        })
    }

    /// Report an actor operation failure to the actor driver.
    // `error_type` is accepted but not forwarded. PyO3 exposes parameters
    // under their exact Rust names, so it must NOT be spelled `_error_type`,
    // or the keyword argument would never match.
    #[pyo3(signature = (operation_id, execution_id, error_message, error_type=None))]
    fn fail_actor_operation<'py>(
        &self,
        py: Python<'py>,
        operation_id: String,
        execution_id: String,
        error_message: String,
        #[allow(unused_variables)] error_type: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let senders = self.senders.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tx = {
                let guard = senders.read().unwrap();
                let worker_senders = guard.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Worker is shutdown")
                })?;
                worker_senders.actor_result_tx.clone().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Actor driver not initialized")
                })?
            };

            let work_result = ActorWorkResult {
                operation_id,
                execution_id,
                result: Err(orcher_sdk_core::Error::internal(error_message)),
            };

            tx.send(work_result).await.map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to send actor error to sdk-core: {}",
                    e
                ))
            })?;

            Ok(())
        })
    }

    // ========================================================================
    // Actor State RPCs
    // ========================================================================

    /// Read one actor state value. Returns None when the key does not exist.
    fn actor_get_state<'py>(
        &self,
        py: Python<'py>,
        actor_name: String,
        key: String,
        state_key: String,
        execution_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let channels = self.state_channel_manager.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            actor_state::get_state(&channels, actor_name, key, state_key, execution_id).await
        })
    }

    /// Write one actor state value.
    fn actor_set_state<'py>(
        &self,
        py: Python<'py>,
        actor_name: String,
        key: String,
        state_key: String,
        value: Vec<u8>,
        execution_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let channels = self.state_channel_manager.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            actor_state::set_state(&channels, actor_name, key, state_key, value, execution_id)
                .await
        })
    }

    /// Delete one actor state value. Returns whether it existed.
    fn actor_delete_state<'py>(
        &self,
        py: Python<'py>,
        actor_name: String,
        key: String,
        state_key: String,
        execution_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let channels = self.state_channel_manager.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            actor_state::delete_state(&channels, actor_name, key, state_key, execution_id).await
        })
    }

    /// List an actor's state keys that start with `prefix`.
    fn actor_list_state_keys<'py>(
        &self,
        py: Python<'py>,
        actor_name: String,
        key: String,
        execution_id: String,
        prefix: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let channels = self.state_channel_manager.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            actor_state::list_state_keys(&channels, actor_name, key, execution_id, prefix).await
        })
    }

    /// Number of actor polling slots.
    fn get_actor_slot_count(&self) -> usize {
        self.actor_slot_count.load(Ordering::SeqCst)
    }

    /// Request shutdown of the worker.
    ///
    /// Stops the actor distributor and polls, and tells the workflow and task
    /// drivers to stop. The drivers do not stop at once: during their shutdown
    /// grace period they still hand over what in-flight polls bring back, so
    /// the workflow and task pollers keep running until `stop_drivers` has
    /// returned and their slot channels close.
    fn request_shutdown(&self) -> PyResult<()> {
        {
            let guard = self.shutdown_tx.lock().unwrap();
            if let Some(tx) = guard.as_ref() {
                let _ = tx.send(true);
            }
        }
        // Signal the drivers now rather than in `stop_drivers`, so they start
        // no new polls while in-flight executions finish, and their grace
        // periods run side by side. `stop_drivers` still takes the handles to
        // wait on them.
        let stops = self.driver_stops.lock().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Lock poisoned: {}", e))
        })?;
        if let Some(stops) = stops.as_ref() {
            stops.signal();
        }
        Ok(())
    }

    /// Stop the workflow and task drivers and wait for both to finish.
    ///
    /// Call once the results still being worked on have been handed back,
    /// and keep the workflow and task pollers running until this returns:
    /// each driver hands over what its last polls bring back and waits for
    /// those results too, all within its shutdown grace period, which started at
    /// `request_shutdown`. A result handed back after its driver has stopped
    /// has nowhere to go, and what it answers stays claimed on the engine
    /// until it times out.
    ///
    /// Waits at most `DRIVER_STOP_TIMEOUT`; calling it again does nothing.
    fn stop_drivers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stops = self
            .driver_stops
            .lock()
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Lock poisoned: {}", e))
            })?
            .take();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(stops) = stops {
                stops.signal();
                let DriverStops { workflow, task } = stops;
                tokio::join!(
                    await_driver("Workflow", workflow.1),
                    await_driver("Task", task.1)
                );
            }
            Ok(())
        })
    }

    /// Whether shutdown has been requested. True once the worker is stopped.
    fn is_shutdown_requested(&self) -> bool {
        let workflow_fan_out = self.workflow_fan_out.clone();
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let guard = workflow_fan_out.lock().await;
            guard
                .as_ref()
                .map(|f| *f.shutdown_rx.borrow())
                .unwrap_or(true)
        })
    }

    /// Always 100. Workflow concurrency is enforced by the core driver, not
    /// tracked here.
    fn available_workflow_slots(&self) -> usize {
        100
    }

    /// Always 100. Task concurrency is enforced by the core driver, not
    /// tracked here.
    fn available_task_slots(&self) -> usize {
        100
    }

    /// Add a session queue; the task driver starts polling it immediately.
    ///
    /// Called by Python's SessionManager when a worker accepts a session.
    fn add_session_queue(&self, queue_name: String) -> PyResult<()> {
        let guard = self
            .session_queue_tx
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Lock poisoned: {}", e)))?;
        match guard.as_ref() {
            Some(tx) => tx
                .send(orcher_sdk_core::poller::SessionQueueChange::Add(queue_name))
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to send session queue add: {}",
                        e
                    ))
                }),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Session queue channel not initialized",
            )),
        }
    }

    /// Remove a session queue; the task driver stops polling it.
    ///
    /// Called by Python's SessionManager when a session completes or fails.
    fn remove_session_queue(&self, queue_name: String) -> PyResult<()> {
        let guard = self
            .session_queue_tx
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Lock poisoned: {}", e)))?;
        match guard.as_ref() {
            Some(tx) => tx
                .send(orcher_sdk_core::poller::SessionQueueChange::Remove(
                    queue_name,
                ))
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to send session queue remove: {}",
                        e
                    ))
                }),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Session queue channel not initialized",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_config() {
        let config = PyWorkerConfig::new(
            "http://localhost:50051".to_string(),
            "test-queue".to_string(),
            "default".to_string(),
            100,
            100,
            None,
            2,
            4,
            4,
            100,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(config.server_url, "http://localhost:50051");
        assert_eq!(config.task_queue, "test-queue");
    }

    #[test]
    fn test_workflow_driver_config_conversion() {
        let config = PyWorkerConfig::new(
            "http://localhost:50051".to_string(),
            "test-queue".to_string(),
            "default".to_string(),
            100,
            100,
            Some("test-identity".to_string()),
            2,
            4,
            4,
            100,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let driver_config = config.to_workflow_driver_config(None);
        assert_eq!(driver_config.server_url, "http://localhost:50051");
        assert_eq!(driver_config.task_queue, "test-queue");
        assert_eq!(driver_config.namespace, "default");
        assert_eq!(driver_config.max_concurrent_executions, 100);
        assert_eq!(driver_config.poller_count, 2);
    }

    #[test]
    fn test_task_driver_config_conversion() {
        let config = PyWorkerConfig::new(
            "http://localhost:50051".to_string(),
            "test-queue".to_string(),
            "default".to_string(),
            100,
            200,
            Some("test-identity".to_string()),
            2,
            4,
            4,
            100,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let driver_config = config.to_task_driver_config(None);
        assert_eq!(driver_config.server_url, "http://localhost:50051");
        assert_eq!(driver_config.task_queue, "test-queue");
        assert_eq!(driver_config.max_concurrent_executions, 200);
        assert_eq!(driver_config.poller_count, 4);
    }

    fn config_with_credentials(api_key: Option<&str>) -> PyResult<PyWorkerConfig> {
        PyWorkerConfig::new(
            "http://localhost:50051".to_string(),
            "test-queue".to_string(),
            "default".to_string(),
            100,
            100,
            Some("test-identity".to_string()),
            2,
            4,
            4,
            100,
            Some("org_123".to_string()),
            None,
            None,
            None,
            None,
            api_key.map(str::to_string),
        )
    }

    #[test]
    fn the_api_key_reaches_every_driver() {
        let config = config_with_credentials(Some("orch_secret")).unwrap();
        let key = Some("orch_secret".to_string());
        let org = Some("org_123".to_string());

        // sdk-core hands each driver's key and organization on to its pollers,
        // its completion and heartbeat reports, and its worker registration.
        let workflow = config.to_workflow_driver_config(None);
        assert_eq!(workflow.api_key, key, "workflow driver");
        assert_eq!(workflow.organization_id, org, "workflow driver");
        let task = config.to_task_driver_config(None);
        assert_eq!(task.api_key, key, "task driver");
        assert_eq!(task.organization_id, org, "task driver");
        let actor = config.to_actor_driver_config(None);
        assert_eq!(actor.api_key, key, "actor driver");
        assert_eq!(actor.organization_id, org, "actor driver");
    }

    #[test]
    fn a_worker_without_a_key_sends_none() {
        for api_key in [None, Some(""), Some("  ")] {
            let config = config_with_credentials(api_key).unwrap();
            assert_eq!(config.api_key, None);
            assert_eq!(config.to_workflow_driver_config(None).api_key, None);
            assert_eq!(config.to_task_driver_config(None).api_key, None);
            assert_eq!(config.to_actor_driver_config(None).api_key, None);
        }
    }

    #[test]
    fn a_key_that_cannot_be_a_header_is_refused_rather_than_dropped() {
        assert!(config_with_credentials(Some("orch_\nsecret")).is_err());
        assert!(config_with_credentials(Some("orch_\u{7f}")).is_err());
    }

    #[test]
    fn repr_and_debug_never_show_the_api_key() {
        let config = config_with_credentials(Some("orch_secret")).unwrap();
        let repr = config.__repr__();
        let debug = format!("{config:?}");
        for shown in [&repr, &debug] {
            assert!(!shown.contains("orch_secret"), "key leaked: {shown}");
            assert!(shown.contains("redacted"), "key not marked as set: {shown}");
        }
        assert!(debug.contains("org_123"));

        let without = config_with_credentials(None).unwrap();
        assert!(!without.__repr__().contains("redacted"));
    }
}
