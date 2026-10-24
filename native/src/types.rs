//! Python wrappers for the core SDK's data types the codecs take.

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::collections::HashMap;

use orcher_sdk_core::types::Payload;

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
