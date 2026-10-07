//! The TLS settings a server URL and the `tls_*` paths resolve to.
//!
//! The client and the worker both decide TLS here, so they cannot disagree.

use orcher_sdk_core::poller::TlsConfig;

/// Whether `url` uses the `https` scheme.
pub(crate) fn is_https(url: &str) -> bool {
    url.trim()
        .get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
}

/// `error` followed by each cause in its source chain.
///
/// tonic reports a failed TLS handshake as a bare "transport error"; why it
/// failed (an untrusted certificate, a client certificate the server
/// required) is only in the chain.
pub(crate) fn with_causes(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !cause_text.is_empty() && !text.contains(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

/// Reads the PEM files named by the `tls_*` paths into a [`TlsConfig`].
///
/// TLS is on if any path is set, or if the URL is `https://`: nobody writing
/// `https://` means plaintext, and sdk-core dials plaintext when handed no
/// `TlsConfig`. A missing CA means "verify against the system trust store", not
/// "no TLS", so a client certificate without a CA never connects in plaintext.
/// An `http://` URL with no paths gets no TLS.
///
/// # Errors
///
/// Returns a message naming the file if one cannot be read, or if only one of
/// the client certificate and key is given.
pub(crate) fn tls_from_paths(
    server_url: &str,
    ca_cert_path: Option<&str>,
    client_cert_path: Option<&str>,
    client_key_path: Option<&str>,
) -> Result<Option<TlsConfig>, String> {
    if ca_cert_path.is_none() && client_cert_path.is_none() && client_key_path.is_none() {
        return Ok(is_https(server_url).then(TlsConfig::new));
    }
    if client_cert_path.is_some() != client_key_path.is_some() {
        return Err(
            "tls_client_cert_path and tls_client_key_path must be set together".to_string(),
        );
    }

    let read = |what: &str, path: Option<&str>| {
        path.map(|p| std::fs::read(p).map_err(|e| format!("Failed to read {what} '{p}': {e}")))
            .transpose()
    };
    let mut tls = TlsConfig::new();
    tls.ca_cert = read("CA cert", ca_cert_path)?;
    tls.client_cert = read("client cert", client_cert_path)?;
    tls.client_key = read("client key", client_key_path)?;
    Ok(Some(tls))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn https_without_paths_uses_the_system_trust_store() {
        let tls = tls_from_paths("https://orcher.example:443", None, None, None)
            .unwrap()
            .expect("TLS on");
        assert!(tls.ca_cert.is_none() && tls.client_cert.is_none());
        assert!(tls_from_paths(" HTTPS://orcher.example", None, None, None)
            .unwrap()
            .is_some());
    }

    #[test]
    fn http_without_paths_is_plaintext() {
        assert!(tls_from_paths("http://localhost:50051", None, None, None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn paths_are_read_into_the_config() {
        let dir = std::env::temp_dir().join(format!("orcher-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (ca, cert, key) = (dir.join("ca.pem"), dir.join("c.pem"), dir.join("k.pem"));
        std::fs::write(&ca, b"ca").unwrap();
        std::fs::write(&cert, b"cert").unwrap();
        std::fs::write(&key, b"key").unwrap();

        let tls = tls_from_paths(
            "https://orcher.example",
            ca.to_str(),
            cert.to_str(),
            key.to_str(),
        )
        .unwrap()
        .unwrap();
        std::fs::remove_dir_all(&dir).unwrap();

        assert_eq!(tls.ca_cert.as_deref(), Some(&b"ca"[..]));
        assert_eq!(tls.client_cert.as_deref(), Some(&b"cert"[..]));
        assert_eq!(tls.client_key.as_deref(), Some(&b"key"[..]));
    }

    #[test]
    fn causes_follow_the_error() {
        let inner = std::io::Error::other("received fatal alert: CertificateRequired");
        let outer = std::io::Error::other(Wrapped(inner));
        assert_eq!(
            with_causes(&outer),
            "transport error: received fatal alert: CertificateRequired"
        );
    }

    #[derive(Debug)]
    struct Wrapped(std::io::Error);

    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("transport error")
        }
    }

    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn a_certificate_without_a_key_is_rejected() {
        let err = tls_from_paths("https://x", None, Some("/c.pem"), None).unwrap_err();
        assert!(err.contains("must be set together"), "{err}");
    }

    #[test]
    fn an_unreadable_file_is_named() {
        let err = tls_from_paths("https://x", Some("/nonexistent/ca.pem"), None, None).unwrap_err();
        assert!(err.contains("/nonexistent/ca.pem"), "{err}");
    }
}
