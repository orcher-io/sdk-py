"""Client configuration for ORCHER Python SDK."""

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
        tls_ca_cert_path: Path to CA certificate PEM file for TLS.
        tls_client_cert_path: Path to client certificate PEM file for mTLS.
        tls_client_key_path: Path to client key PEM file for mTLS.
        api_key: API key sent with every request, for servers that require one.
            Never shown by repr.
        connection_timeout: Timeout for establishing a connection (a timedelta).
        request_timeout: Default timeout for requests (a timedelta).
    """

    server_url: str
    namespace: str = "default"
    identity: str | None = None
    tls_ca_cert_path: str | None = None
    tls_client_cert_path: str | None = None
    tls_client_key_path: str | None = None
    api_key: str | None = field(default=None, repr=False)
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
        # Blank is "no key", so an exported-but-empty ORCHER_API_KEY does not
        # send an empty bearer token.
        if self.api_key is not None and not self.api_key.strip():
            self.api_key = None
