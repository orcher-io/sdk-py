//! The client, as a binding over sdk-core's client protocol.
//!
//! Python connects once and then makes every client call (starting
//! workflows, waiting for their results, queries, updates, events, listing,
//! actors, namespaces) by handing sdk-core an encoded
//! `orcher.client.v1.ClientCall` and reading back the encoded `ClientAnswer`.
//! Python builds and reads the messages with the code generated from the
//! same protocol (`orcher._proto.client_pb2`), so a new client call needs no
//! change here.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;

use orcher_sdk_core::payload::PayloadSeal;
use orcher_sdk_core::{ConnectOptions, Connection, ProtocolClient};

use crate::seal::PyPayloadSeal;

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

    /// Applied to every payload sent and removed from every payload
    /// received; see [`crate::seal`]. Not readable from Python: it holds keys.
    payload_seal: Option<PayloadSeal>,
}

#[pymethods]
impl PyClientConfig {
    #[new]
    #[pyo3(signature = (server_url, namespace = "default".to_string(), timeout_ms = 10000, identity = None, tls_ca_cert_path = None, tls_client_cert_path = None, tls_client_key_path = None, api_key = None, connect_timeout_ms = 10000, payload_seal = None))]
    #[allow(clippy::too_many_arguments)]
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
        payload_seal: Option<PyRef<'_, PyPayloadSeal>>,
    ) -> Self {
        Self {
            payload_seal: payload_seal.and_then(|seal| seal.seal()),
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
            payload_seal: None,
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
    /// What the connection is made with: the address, the namespace, the
    /// API key, and the payload seal when one is configured. TLS is added by
    /// the caller, which reads its files.
    fn connect_options(&self) -> ConnectOptions {
        let mut options = ConnectOptions::new(&self.server_url)
            .with_namespace(&self.namespace)
            .with_connect_timeout(Duration::from_millis(self.connect_timeout_ms));
        if let Some(ref key) = self.api_key {
            options = options.with_api_key(key);
        }
        if let Some(ref seal) = self.payload_seal {
            options = options.with_payload_seal(seal.clone());
        }
        options
    }

    fn build_tls_config(&self) -> std::result::Result<Option<orcher_sdk_core::TlsConfig>, String> {
        crate::tls::tls_from_paths(
            &self.server_url,
            self.tls_ca_cert_path.as_deref(),
            self.tls_client_cert_path.as_deref(),
            self.tls_client_key_path.as_deref(),
        )
    }
}

/// A connection to the engine that answers client protocol calls.
#[pyclass(name = "ClientConnection", frozen)]
pub struct PyClientConnection {
    client: Arc<ProtocolClient>,
}

#[pymethods]
impl PyClientConnection {
    /// `ClientConnection.connect(config) -> Awaitable[ClientConnection]`
    ///
    /// With TLS settings the connection is made now, so a certificate the
    /// server refuses fails here; without, on the first call. Raises
    /// `ValueError` for TLS files that cannot be read and `RuntimeError` for
    /// a connection that cannot be made.
    #[staticmethod]
    fn connect<'py>(py: Python<'py>, config: PyClientConfig) -> PyResult<Bound<'py, PyAny>> {
        let tls = config
            .build_tls_config()
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        let mut options = config.connect_options();
        let dial_now = tls.is_some();
        if let Some(tls) = tls {
            options = options.with_tls(tls);
        }
        let timeout = Duration::from_millis(config.timeout_ms);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let connection = if dial_now {
                Connection::connect(options).await
            } else {
                Connection::lazy(options)
            }
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Client connection failed: {e}"))
            })?;
            // Every call carries the configured timeout unless it sets its own.
            let client = connection.protocol_client().with_timeout(timeout);
            Ok(PyClientConnection {
                client: Arc::new(client),
            })
        })
    }

    /// `call(call: bytes) -> Awaitable[bytes]`
    ///
    /// Answers an encoded `ClientCall` with an encoded `ClientAnswer`; a
    /// refused call is answered with a `ClientError`, never raised.
    fn call<'py>(&self, py: Python<'py>, call: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let client = Arc::clone(&self.client);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(Cow::<'static, [u8]>::Owned(client.call(&call).await))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(payload_seal: Option<PayloadSeal>) -> PyClientConfig {
        PyClientConfig {
            server_url: "http://localhost:50051".to_string(),
            namespace: "default".to_string(),
            timeout_ms: 10000,
            identity: None,
            tls_ca_cert_path: None,
            tls_client_cert_path: None,
            tls_client_key_path: None,
            api_key: Some("key".to_string()),
            connect_timeout_ms: 2500,
            payload_seal,
        }
    }

    #[test]
    fn the_connection_is_made_with_the_payload_seal() {
        let seal = crate::seal::build_seal(Some(&[7u8; 32]), None, &[], None).unwrap();
        assert!(seal.is_some());
        let options = config(seal).connect_options();
        assert!(options.payload_seal.is_some());
        assert_eq!(options.api_key.as_deref(), Some("key"));
        assert_eq!(options.namespace, "default");
    }

    #[test]
    fn without_a_seal_payloads_are_sent_as_they_are() {
        assert!(config(None).connect_options().payload_seal.is_none());
    }

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
            None,
        );
        assert_eq!(config.server_url, "http://localhost:50051");
        assert_eq!(config.namespace, "default");
        assert_eq!(config.timeout_ms, 10000);
        assert_eq!(config.connect_timeout_ms, 2500);
    }
}
