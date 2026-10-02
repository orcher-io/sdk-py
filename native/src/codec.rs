//! Payload codec bindings.
//!
//! Exposes the core gzip and encryption payload codecs, and a chain that
//! combines them, to Python.

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use orcher_sdk_core::codec::{EncryptionPayloadCodec, GzipPayloadCodec, PayloadCodec};

use crate::types::PyPayload;

/// Payload codec that compresses data with gzip.
#[pyclass(name = "GzipCodec")]
#[derive(Clone, Debug)]
pub struct PyGzipCodec {
    inner: GzipPayloadCodec,
}

#[pymethods]
impl PyGzipCodec {
    /// Create a new GzipCodec with a compression level (0-9, default 6).
    #[new]
    #[pyo3(signature = (level=6))]
    fn new(level: u32) -> PyResult<Self> {
        if level > 9 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Compression level must be 0-9",
            ));
        }
        Ok(Self {
            inner: GzipPayloadCodec::new(flate2::Compression::new(level)),
        })
    }

    /// Create a GzipCodec with fast compression (level 1).
    #[staticmethod]
    fn fast() -> Self {
        Self {
            inner: GzipPayloadCodec::fast(),
        }
    }

    /// Create a GzipCodec with best compression (level 9).
    #[staticmethod]
    fn best() -> Self {
        Self {
            inner: GzipPayloadCodec::best(),
        }
    }

    /// Compress a Payload, returning a new Payload with compressed data.
    fn encode(&self, payload: &PyPayload) -> PyResult<PyPayload> {
        self.inner
            .encode(payload.inner())
            .map(PyPayload::from_inner)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Gzip compression failed: {}",
                    e
                ))
            })
    }

    /// Decompress a Payload, returning a new Payload with decompressed data.
    fn decode(&self, payload: &PyPayload) -> PyResult<PyPayload> {
        self.inner
            .decode(payload.inner())
            .map(PyPayload::from_inner)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Gzip decompression failed: {}",
                    e
                ))
            })
    }

    /// The compression level.
    #[getter]
    fn compression_level(&self) -> u32 {
        self.inner.compression_level()
    }

    fn __repr__(&self) -> String {
        format!("GzipCodec(level={})", self.inner.compression_level())
    }
}

/// Payload codec that encrypts data with AES-256-GCM.
#[pyclass(name = "EncryptionCodec")]
#[derive(Clone, Debug)]
pub struct PyEncryptionCodec {
    inner: EncryptionPayloadCodec,
}

#[pymethods]
impl PyEncryptionCodec {
    /// Create a new EncryptionCodec with a 32-byte key.
    ///
    /// Args:
    ///     key: 32-byte encryption key (bytes)
    ///
    /// Raises:
    ///     ValueError: If key is not exactly 32 bytes
    #[new]
    fn new(key: &Bound<'_, PyBytes>) -> PyResult<Self> {
        let key_bytes = key.as_bytes();
        let codec = EncryptionPayloadCodec::from_slice(key_bytes).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid key: {}", e))
        })?;
        Ok(Self { inner: codec })
    }

    /// Generate a cryptographically secure random 32-byte key.
    ///
    /// Returns:
    ///     bytes: 32-byte random key
    #[staticmethod]
    fn generate_key<'py>(py: Python<'py>) -> Bound<'py, PyBytes> {
        let key = EncryptionPayloadCodec::generate_key();
        PyBytes::new(py, &key)
    }

    /// Encrypt a Payload, returning a new Payload with encrypted data.
    fn encode(&self, payload: &PyPayload) -> PyResult<PyPayload> {
        self.inner
            .encode(payload.inner())
            .map(PyPayload::from_inner)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Encryption failed: {}",
                    e
                ))
            })
    }

    /// Decrypt a Payload, returning a new Payload with decrypted data.
    fn decode(&self, payload: &PyPayload) -> PyResult<PyPayload> {
        self.inner
            .decode(payload.inner())
            .map(PyPayload::from_inner)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Decryption failed: {}",
                    e
                ))
            })
    }

    fn __repr__(&self) -> String {
        "EncryptionCodec(<key redacted>)".to_string()
    }
}

/// Description of one codec in a chain. Storing specs rather than codec
/// instances lets the chain be cloned and each codec rebuilt on demand.
#[derive(Clone, Debug)]
enum CodecSpec {
    Gzip(u32),
    Encryption([u8; 32]),
}

impl CodecSpec {
    fn build(&self) -> Box<dyn PayloadCodec> {
        match self {
            CodecSpec::Gzip(level) => {
                Box::new(GzipPayloadCodec::new(flate2::Compression::new(*level)))
            }
            CodecSpec::Encryption(key) => Box::new(EncryptionPayloadCodec::new(key)),
        }
    }
}

/// Ordered chain of payload codecs.
///
/// Codecs are applied in the order added when encoding and in reverse order
/// when decoding.
///
/// Example:
///     chain = CodecChain().with_encryption(key).with_gzip()
///     encoded = chain.encode(payload)   # encrypt then compress
///     decoded = chain.decode(encoded)   # decompress then decrypt
#[pyclass(name = "CodecChain")]
#[derive(Clone, Debug)]
pub struct PyCodecChain {
    specs: Vec<CodecSpec>,
}

impl PyCodecChain {
    fn encode_inner(
        &self,
        payload: &orcher_sdk_core::types::Payload,
    ) -> Result<orcher_sdk_core::types::Payload, orcher_sdk_core::error::Error> {
        let mut current = payload.clone();
        for spec in &self.specs {
            let codec = spec.build();
            current = codec.encode(&current)?;
        }
        Ok(current)
    }

    fn decode_inner(
        &self,
        payload: &orcher_sdk_core::types::Payload,
    ) -> Result<orcher_sdk_core::types::Payload, orcher_sdk_core::error::Error> {
        let mut current = payload.clone();
        for spec in self.specs.iter().rev() {
            let codec = spec.build();
            current = codec.decode(&current)?;
        }
        Ok(current)
    }
}

#[pymethods]
impl PyCodecChain {
    /// Create an empty CodecChain.
    #[new]
    fn new() -> Self {
        Self { specs: Vec::new() }
    }

    /// Add gzip compression to the chain, returning a new CodecChain.
    ///
    /// Args:
    ///     level: Compression level 0-9 (default 6)
    ///
    /// Returns:
    ///     New CodecChain with gzip added
    #[pyo3(signature = (level=None))]
    fn with_gzip(&self, level: Option<u32>) -> PyResult<PyCodecChain> {
        let l = level.unwrap_or(6);
        if l > 9 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Compression level must be 0-9",
            ));
        }
        let mut specs = self.specs.clone();
        specs.push(CodecSpec::Gzip(l));
        Ok(PyCodecChain { specs })
    }

    /// Add AES-256-GCM encryption to the chain, returning a new CodecChain.
    ///
    /// Args:
    ///     key: 32-byte encryption key (bytes)
    ///
    /// Returns:
    ///     New CodecChain with encryption added
    fn with_encryption(&self, key: &Bound<'_, PyBytes>) -> PyResult<PyCodecChain> {
        let key_bytes = key.as_bytes();
        if key_bytes.len() != 32 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid key length: expected 32 bytes, got {}",
                key_bytes.len()
            )));
        }
        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(key_bytes);
        let mut specs = self.specs.clone();
        specs.push(CodecSpec::Encryption(key_array));
        Ok(PyCodecChain { specs })
    }

    /// Encode a Payload through the full codec chain.
    fn encode(&self, payload: &PyPayload) -> PyResult<PyPayload> {
        self.encode_inner(payload.inner())
            .map(PyPayload::from_inner)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Codec chain encode failed: {}",
                    e
                ))
            })
    }

    /// Decode a Payload through the full codec chain (reverse order).
    fn decode(&self, payload: &PyPayload) -> PyResult<PyPayload> {
        self.decode_inner(payload.inner())
            .map(PyPayload::from_inner)
            .map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Codec chain decode failed: {}",
                    e
                ))
            })
    }

    fn __len__(&self) -> usize {
        self.specs.len()
    }

    fn __repr__(&self) -> String {
        let codec_names: Vec<&str> = self
            .specs
            .iter()
            .map(|s| match s {
                CodecSpec::Gzip(_) => "Gzip",
                CodecSpec::Encryption(_) => "Encryption",
            })
            .collect();
        format!("CodecChain(codecs={:?})", codec_names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_sdk_core::payload::Payload;

    #[test]
    fn test_gzip_roundtrip() {
        let codec = PyGzipCodec::new(6).unwrap();
        let original = PyPayload::from_inner(Payload::json(b"Hello, World!".repeat(100)));

        let encoded = codec.encode(&original).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded.inner().data, original.inner().data);
    }

    #[test]
    fn test_gzip_invalid_level() {
        let result = PyGzipCodec::new(10);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_roundtrip() {
        let key = EncryptionPayloadCodec::generate_key();
        let codec = EncryptionPayloadCodec::new(&key);
        let py_codec = PyEncryptionCodec {
            inner: codec.clone(),
        };

        let original = PyPayload::from_inner(Payload::json(b"sensitive data".to_vec()));
        let encoded = py_codec.encode(&original).unwrap();
        assert_ne!(encoded.inner().data, original.inner().data);

        let decoded = py_codec.decode(&encoded).unwrap();
        assert_eq!(decoded.inner().data, original.inner().data);
    }

    #[test]
    fn test_codec_chain_roundtrip() {
        let key = EncryptionPayloadCodec::generate_key();
        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(&key);

        // Build: encrypt then gzip
        let chain = PyCodecChain {
            specs: vec![CodecSpec::Encryption(key_array), CodecSpec::Gzip(6)],
        };

        let original = PyPayload::from_inner(Payload::json(b"chain test data".repeat(50)));
        let encoded = chain.encode(&original).unwrap();
        let decoded = chain.decode(&encoded).unwrap();

        assert_eq!(decoded.inner().data, original.inner().data);
    }

    #[test]
    fn test_codec_chain_empty() {
        let chain = PyCodecChain::new();
        assert_eq!(chain.__len__(), 0);

        let original = PyPayload::from_inner(Payload::json(b"pass through".to_vec()));
        let encoded = chain.encode(&original).unwrap();
        assert_eq!(encoded.inner().data, original.inner().data);
    }
}
