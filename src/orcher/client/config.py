"""Client configuration for ORCHER Python SDK."""

import os
from dataclasses import dataclass, field
from datetime import timedelta

from orcher.errors import ConfigurationError

__all__ = [
    "ClientConfig",
]


@dataclass
class ClientConfig:
    """Configuration for ORCHER client connection.

    Attributes:
        server_url: ORCHER server address (e.g., "http://localhost:50051").
        namespace: Namespace for workflow operations. Defaults to "default".
        identity: Client identifier. If not set, generated automatically.
        tls_ca_cert_path: Path to a CA certificate PEM file, for a private or
            self-signed authority. Without one the server is verified against
            the system trust store.
        tls_client_cert_path: Path to client certificate PEM file for mTLS.
            Requires tls_client_key_path.
        tls_client_key_path: Path to client key PEM file for mTLS. Requires
            tls_client_cert_path.
        api_key: API key sent with every request, for servers that require one.
            Defaults to the ORCHER_API_KEY environment variable; pass None to
            send none. Never shown by repr.

    TLS follows the URL scheme: an ``https://`` server_url connects over TLS
    verified against the system trust store, ``http://`` in plaintext. The
    tls_* paths supply a private CA or a client certificate (mTLS).
        connection_timeout: Timeout for establishing a connection (a timedelta).
        request_timeout: Default timeout for requests (a timedelta).
    """

    server_url: str
    namespace: str = "default"
    identity: str | None = None
    tls_ca_cert_path: str | None = None
    tls_client_cert_path: str | None = None
    tls_client_key_path: str | None = None
    # Keys are issued out of band, so the environment is the usual way one
    # reaches a process; the worker builder reads the same variable.
    api_key: str | None = field(
        default_factory=lambda: os.environ.get("ORCHER_API_KEY"), repr=False
    )
    connection_timeout: timedelta = timedelta(seconds=10)
    request_timeout: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        """Validate configuration."""
        if not self.server_url:
            raise ConfigurationError.missing_required("server_url")
        if not self.server_url.startswith(("http://", "https://", "grpc://")):
            raise ConfigurationError.invalid_value(
                "server_url",
                self.server_url,
                "must start with http://, https://, or grpc://",
            )
        if (self.tls_client_cert_path is None) != (self.tls_client_key_path is None):
            raise ConfigurationError.invalid_value(
                "tls_client_cert_path",
                self.tls_client_cert_path,
                "tls_client_cert_path and tls_client_key_path must be set together",
            )
        # Blank is "no key", so an exported-but-empty ORCHER_API_KEY does not
        # send an empty bearer token.
        if self.api_key is not None and not self.api_key.strip():
            self.api_key = None
