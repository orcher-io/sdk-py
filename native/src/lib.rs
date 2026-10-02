//! Native module for the Orcher Python SDK.
//!
//! Python bindings for the Rust Orcher SDK core, built with PyO3.
//!
//! # Architecture
//!
//! ```text
//! Python (orcher package)
//!     ↓
//! PyO3 (this module)
//!     ↓
//! Orcher SDK core (Rust)
//! ```
//!
//! The Python package calls into this module, which delegates to the Rust
//! SDK core for networking, polling, and serialization.

use once_cell::sync::Lazy;
use pyo3::prelude::*;
use std::sync::Mutex;
use tracing_subscriber::{fmt, EnvFilter};

mod actor_state;
mod client;
mod codec;
mod convert;
mod error;
mod journal_times;
mod runtime;
mod worker;
mod types;

// Re-exported for use within the crate.
pub use convert::*;
pub use error::*;
pub use runtime::*;
pub use types::*;

/// Set once `initialize` has run, so it runs at most once per process.
static INITIALIZED: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));

/// Initialize the native module's global state.
///
/// Installs the tracing subscriber. Safe to call more than once.
fn initialize() {
    let mut initialized = INITIALIZED.lock().unwrap();
    if *initialized {
        return;
    }

    // RUST_LOG overrides the default filter. The default includes
    // orcher_sdk_py (this crate's name with underscores) so its logs show.
    let _ = fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("orcher=info,orcher_sdk_core=info,orcher_sdk_py=info")
        }))
        .try_init();

    tracing::info!("ORCHER Python SDK native module initialized");
    *initialized = true;
}

/// Return the version of the native module.
#[pyfunction]
fn _get_native_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Return the version of the Rust SDK core.
#[pyfunction]
fn _get_core_version() -> &'static str {
    orcher_sdk_core::VERSION
}

/// Always returns true; a successful call shows the native module is loaded.
#[pyfunction]
fn _health_check() -> bool {
    true
}

/// Reads an activation's result the way the worker hands it to sdk-core, and
/// returns it as sdk-core serializes it back. Raises `ValueError` for a result
/// sdk-core would reject. For tests: it checks the commands the Python side
/// builds against the types sdk-core deserializes.
#[pyfunction]
fn _parse_execution_result(result_json: &str) -> PyResult<String> {
    let result = worker::parse_execution_result(result_json)?;
    serde_json::to_string(&result)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// Python module definition.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    initialize();

    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("__core_version__", orcher_sdk_core::VERSION)?;

    // Version and health functions
    m.add_function(wrap_pyfunction!(_get_native_version, m)?)?;
    m.add_function(wrap_pyfunction!(_get_core_version, m)?)?;
    m.add_function(wrap_pyfunction!(_health_check, m)?)?;
    m.add_function(wrap_pyfunction!(_parse_execution_result, m)?)?;

    // Types
    m.add_class::<types::PyWorkflowExecution>()?;
    m.add_class::<types::PyPayload>()?;
    m.add_class::<types::PyWorkflowStatus>()?;
    m.add_class::<types::PyRetryPolicy>()?;
    m.add_class::<types::PyFailure>()?;

    // Client
    m.add_class::<client::PyClientConfig>()?;
    m.add_class::<client::PyClient>()?;
    m.add_class::<client::PyWorkflowHandle>()?;

    // Worker
    m.add_class::<worker::PyWorkerConfig>()?;
    m.add_class::<worker::PyBridgeWorker>()?;

    // Codecs
    m.add_class::<codec::PyGzipCodec>()?;
    m.add_class::<codec::PyEncryptionCodec>()?;
    m.add_class::<codec::PyCodecChain>()?;

    // Exceptions
    m.add("NativeError", m.py().get_type::<error::NativeError>())?;
    // Kind-specific subclasses, so callers can catch precisely instead of
    // parsing messages. All subclass NativeError, so catching NativeError
    // still catches them.
    m.add(
        "WorkflowNotFoundError",
        m.py().get_type::<error::WorkflowNotFoundError>(),
    )?;
    m.add(
        "WorkflowAlreadyExistsError",
        m.py().get_type::<error::WorkflowAlreadyExistsError>(),
    )?;
    m.add(
        "WorkflowFailedError",
        m.py().get_type::<error::WorkflowFailedError>(),
    )?;
    m.add(
        "WorkflowCancelledError",
        m.py().get_type::<error::WorkflowCancelledError>(),
    )?;
    m.add(
        "WorkflowTerminatedError",
        m.py().get_type::<error::WorkflowTerminatedError>(),
    )?;
    m.add("TaskFailedError", m.py().get_type::<error::TaskFailedError>())?;
    m.add(
        "TaskCancelledError",
        m.py().get_type::<error::TaskCancelledError>(),
    )?;
    m.add(
        "DeterminismError",
        m.py().get_type::<error::DeterminismError>(),
    )?;
    m.add(
        "WorkerShutdownEvent",
        m.py().get_type::<worker::WorkerShutdownEvent>(),
    )?;

    tracing::info!("ORCHER Python SDK native module loaded successfully");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initialization() {
        initialize();
        let initialized = INITIALIZED.lock().unwrap();
        assert!(*initialized);
    }

    #[test]
    fn test_version() {
        let version = _get_native_version();
        assert!(!version.is_empty());
    }

    #[test]
    fn test_core_version() {
        let version = _get_core_version();
        assert!(!version.is_empty());
    }

    #[test]
    fn test_health_check() {
        assert!(_health_check());
    }
}
