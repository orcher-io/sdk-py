//! Payload encryption and compression, as the client and the worker are
//! configured with them.
//!
//! Python hands over a key and what to do with it once, as a `PayloadSeal`.
//! The client's connection and the worker host's are both built with it, and
//! sdk-core then encodes every payload just before it is sent to the engine
//! and decodes every payload just after it arrives. Nothing else in this
//! crate or in the Python package handles an encoded payload.
//!
//! Key material is never formatted: not in `repr`, not in `Debug`, not in an
//! error message.

use std::collections::HashMap;

use orcher_sdk_core::payload::codec::{CodecChain, EncryptionPayloadCodec, GzipPayloadCodec};
use orcher_sdk_core::payload::PayloadSeal;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// The only compression there is.
const GZIP: &str = "gzip";

/// The length of an AES-256 key, in bytes.
const KEY_LEN: usize = 32;

fn checked_key(what: &str, key: &[u8]) -> Result<[u8; KEY_LEN], String> {
    // The length is the only thing said about the key.
    <[u8; KEY_LEN]>::try_from(key).map_err(|_| {
        format!(
            "{what} must be exactly {KEY_LEN} bytes (AES-256-GCM), got {}",
            key.len()
        )
    })
}

fn checked_key_id(what: &str, key_id: &str) -> Result<(), String> {
    if key_id.trim().is_empty() {
        return Err(format!("{what} must not be blank"));
    }
    Ok(())
}

/// The seal a configuration asks for, or `None` when it asks for neither
/// encryption nor compression.
///
/// With both, a payload is compressed first and then encrypted: ciphertext
/// does not compress.
///
/// # Errors
///
/// Returns a message for a key that is not 32 bytes, a blank key id, a
/// decryption key given without an encryption key or under the encryption
/// key's own id, and a compression other than `gzip`. A message never
/// contains key material.
pub(crate) fn build_seal(
    key: Option<&[u8]>,
    key_id: Option<&str>,
    decryption_keys: &[(String, Vec<u8>)],
    compression: Option<&str>,
) -> Result<Option<PayloadSeal>, String> {
    let mut chain = CodecChain::new();
    match compression {
        None => {}
        Some(GZIP) => chain = chain.with_codec(GzipPayloadCodec::default()),
        Some(other) => {
            return Err(format!(
                "compression must be \"{GZIP}\" or unset, got {other:?}"
            ))
        }
    }
    match key {
        Some(key) => {
            let mut codec = EncryptionPayloadCodec::new(&checked_key("encryption key", key)?);
            if let Some(key_id) = key_id {
                checked_key_id("encryption key_id", key_id)?;
                codec = codec.with_key_id(key_id);
            }
            for (id, retired) in decryption_keys {
                checked_key_id("a decryption key's id", id)?;
                if key_id == Some(id.as_str()) {
                    return Err(format!(
                        "decryption_keys holds a key under `{id}`, the id of the encryption key"
                    ));
                }
                let retired = checked_key(&format!("decryption key `{id}`"), retired)?;
                codec = codec.with_decryption_key(id, &retired);
            }
            chain = chain.with_codec(codec);
        }
        None if key_id.is_some() || !decryption_keys.is_empty() => {
            return Err("a key id or decryption keys were given without an encryption key".into());
        }
        None => {}
    }
    Ok((!chain.is_empty()).then(|| PayloadSeal::new(chain)))
}

/// What a client or a worker does to every payload it exchanges with the
/// engine: gzip compression, AES-256-GCM encryption, or both.
///
/// Built by `orcher.encryption`; pass it to `ClientConfig` and to
/// `WorkerHost.start`.
#[pyclass(name = "PayloadSeal", frozen)]
pub struct PyPayloadSeal {
    seal: Option<PayloadSeal>,
    encrypted: bool,
    key_id: Option<String>,
    /// The ids of the keys kept for reading, sorted. Never the keys.
    decryption_key_ids: Vec<String>,
    compression: Option<String>,
}

impl PyPayloadSeal {
    /// The seal to build a connection with; `None` when nothing was asked for.
    pub(crate) fn seal(&self) -> Option<PayloadSeal> {
        self.seal.clone()
    }
}

#[pymethods]
impl PyPayloadSeal {
    /// `PayloadSeal(key=None, key_id=None, decryption_keys=None, compression=None)`
    ///
    /// Raises `ValueError` for a configuration that cannot be used; see
    /// `build_seal`.
    #[new]
    #[pyo3(signature = (key = None, key_id = None, decryption_keys = None, compression = None))]
    fn new(
        key: Option<Vec<u8>>,
        key_id: Option<String>,
        decryption_keys: Option<HashMap<String, Vec<u8>>>,
        compression: Option<String>,
    ) -> PyResult<Self> {
        let mut decryption_keys: Vec<(String, Vec<u8>)> =
            decryption_keys.unwrap_or_default().into_iter().collect();
        decryption_keys.sort_by(|a, b| a.0.cmp(&b.0));
        let seal = build_seal(
            key.as_deref(),
            key_id.as_deref(),
            &decryption_keys,
            compression.as_deref(),
        )
        .map_err(PyValueError::new_err)?;
        Ok(Self {
            seal,
            encrypted: key.is_some(),
            key_id,
            decryption_key_ids: decryption_keys.into_iter().map(|(id, _)| id).collect(),
            compression,
        })
    }

    /// Whether payloads are encrypted.
    #[getter]
    fn encrypted(&self) -> bool {
        self.encrypted
    }

    /// The compression applied, if any.
    #[getter]
    fn compression(&self) -> Option<String> {
        self.compression.clone()
    }

    fn __repr__(&self) -> String {
        // Python's spelling, and never a key.
        let text = |value: &Option<String>| match value {
            Some(value) => format!("{value:?}"),
            None => "None".to_string(),
        };
        format!(
            "PayloadSeal(encrypted={}, key_id={}, decryption_key_ids={:?}, compression={})",
            if self.encrypted { "True" } else { "False" },
            text(&self.key_id),
            self.decryption_key_ids,
            text(&self.compression),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = *b"0123456789abcdef0123456789abcdef";
    const OLD: [u8; 32] = *b"fedcba9876543210fedcba9876543210";
    const PLAIN: &[u8] = br#"{"card":"SEAL-TEST-MARKER"}"#;

    fn seal(
        key: Option<&[u8]>,
        key_id: Option<&str>,
        decryption_keys: &[(&str, &[u8])],
        compression: Option<&str>,
    ) -> Result<Option<PayloadSeal>, String> {
        let decryption_keys: Vec<(String, Vec<u8>)> = decryption_keys
            .iter()
            .map(|(id, key)| (id.to_string(), key.to_vec()))
            .collect();
        build_seal(key, key_id, &decryption_keys, compression)
    }

    fn sealed_text(seal: &PayloadSeal) -> String {
        String::from_utf8(seal.seal(PLAIN).unwrap()).unwrap()
    }

    #[test]
    fn nothing_asked_for_is_no_seal() {
        assert!(seal(None, None, &[], None).unwrap().is_none());
    }

    #[test]
    fn an_encrypted_payload_is_unreadable_and_opens_with_the_same_key() {
        let writer = seal(Some(&KEY), None, &[], None).unwrap().unwrap();
        let stored = sealed_text(&writer);
        assert!(stored.contains("binary/encrypted"), "{stored}");
        assert!(!stored.contains("SEAL-TEST-MARKER"), "{stored}");

        let reader = seal(Some(&KEY), None, &[], None).unwrap().unwrap();
        assert_eq!(
            reader.open(stored.as_bytes()).unwrap().as_deref(),
            Some(PLAIN)
        );
    }

    #[test]
    fn another_key_does_not_open_it() {
        let stored = sealed_text(&seal(Some(&KEY), None, &[], None).unwrap().unwrap());
        let reader = seal(Some(&OLD), None, &[], None).unwrap().unwrap();
        assert!(reader.open(stored.as_bytes()).is_err());
    }

    #[test]
    fn compression_alone_is_gzip() {
        let writer = seal(None, None, &[], Some("gzip")).unwrap().unwrap();
        let stored = sealed_text(&writer);
        assert!(stored.contains("binary/gzip"), "{stored}");
        assert_eq!(
            writer.open(stored.as_bytes()).unwrap().as_deref(),
            Some(PLAIN)
        );
    }

    /// Compressed first, then encrypted: what is stored says only that it is
    /// encrypted, and it opens back to the plain value.
    #[test]
    fn both_compress_first_and_encrypt_last() {
        let writer = seal(Some(&KEY), None, &[], Some("gzip")).unwrap().unwrap();
        let stored = sealed_text(&writer);
        // The encodings in the order they were applied.
        assert!(stored.contains("binary/gzip+encrypted"), "{stored}");
        assert!(!stored.contains("SEAL-TEST-MARKER"), "{stored}");
        assert_eq!(
            writer.open(stored.as_bytes()).unwrap().as_deref(),
            Some(PLAIN)
        );
        // A reader that only decrypts is left holding gzip, not the value.
        let decrypt_only = seal(Some(&KEY), None, &[], None).unwrap().unwrap();
        let half_open = decrypt_only.open(stored.as_bytes());
        assert!(!matches!(half_open, Ok(Some(ref plain)) if plain == PLAIN));
    }

    #[test]
    fn a_rotated_out_key_still_reads_what_it_wrote() {
        let old_writer = seal(Some(&OLD), Some("k1"), &[], None).unwrap().unwrap();
        let stored = sealed_text(&old_writer);
        assert!(stored.contains("k1"), "{stored}");

        let rotated = seal(Some(&KEY), Some("k2"), &[("k1", &OLD)], None)
            .unwrap()
            .unwrap();
        assert_eq!(
            rotated.open(stored.as_bytes()).unwrap().as_deref(),
            Some(PLAIN)
        );
        // It writes with the new key, which the old writer cannot read.
        let restored = sealed_text(&rotated);
        assert!(restored.contains("k2"), "{restored}");
        assert!(old_writer.open(restored.as_bytes()).is_err());

        // Without the old key the payload names a key the reader lacks.
        let forgetful = seal(Some(&KEY), Some("k2"), &[], None).unwrap().unwrap();
        assert!(forgetful.open(stored.as_bytes()).is_err());
    }

    #[test]
    fn what_cannot_be_used_is_refused() {
        let short = [7u8; 31];
        let long = [7u8; 33];
        for (what, result) in [
            ("short key", seal(Some(&short), None, &[], None)),
            ("long key", seal(Some(&long), None, &[], None)),
            ("empty key", seal(Some(&[]), None, &[], None)),
            ("blank key id", seal(Some(&KEY), Some(" "), &[], None)),
            (
                "short decryption key",
                seal(Some(&KEY), Some("k2"), &[("k1", &short)], None),
            ),
            (
                "blank decryption key id",
                seal(Some(&KEY), Some("k2"), &[("", &OLD)], None),
            ),
            (
                "decryption key under the encryption key's id",
                seal(Some(&KEY), Some("k1"), &[("k1", &OLD)], None),
            ),
            (
                "decryption keys without a key",
                seal(None, None, &[("k1", &OLD)], None),
            ),
            ("key id without a key", seal(None, Some("k1"), &[], None)),
            ("unknown compression", seal(None, None, &[], Some("zstd"))),
        ] {
            assert!(result.is_err(), "{what} was accepted");
        }
    }

    #[test]
    fn no_error_or_debug_output_shows_a_key() {
        let secret = [b'S'; 31];
        let error = seal(Some(&secret), None, &[], None).unwrap_err();
        assert!(!error.contains("SSSS"), "{error}");
        let error = seal(Some(&KEY), Some("k2"), &[("k1", &secret)], None).unwrap_err();
        assert!(!error.contains("SSSS"), "{error}");

        let built = seal(Some(&KEY), Some("k2"), &[("k1", &OLD)], Some("gzip"))
            .unwrap()
            .unwrap();
        let debug = format!("{built:?}");
        for key in [&KEY, &OLD] {
            assert!(
                !debug.contains(std::str::from_utf8(key).unwrap()),
                "{debug}"
            );
        }
    }
}
