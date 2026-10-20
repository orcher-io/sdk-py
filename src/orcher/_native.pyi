"""Type stubs for the ORCHER native module.

The native module exposes the shared Rust SDK core to Python through PyO3.
"""

from typing import Any

# =============================================================================
# Module Information
# =============================================================================

def _get_native_version() -> str:
    """Get the version of the native module."""
    ...

def _get_core_version() -> str:
    """Get the version of the SDK core."""
    ...

def _health_check() -> bool:
    """Return whether the SDK core is reachable from Python."""
    ...

def _kernel_resolve(poll_response: bytes, namespace: str) -> bytes:
    """A recorded activation resolved by the kernel, an encoded ``RunActivation``."""
    ...

def _kernel_execution_result(run_id: str, result: bytes) -> str:
    """An encoded ``RunActivationResult`` as the kernel turns it into what is sent,
    as JSON.

    Raises ValueError when the kernel refuses it.
    """
    ...

# =============================================================================
# Core Types
# =============================================================================

class WorkflowExecution:
    """Workflow execution identifier."""

    def __init__(self, workflow_id: str, run_id: str) -> None: ...
    @property
    def workflow_id(self) -> str: ...
    @property
    def run_id(self) -> str: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...

class Payload:
    """Serialized data with metadata."""

    def __init__(
        self,
        data: bytes | None = None,
        metadata: dict[str, bytes] | None = None,
    ) -> None: ...
    @staticmethod
    def from_json(value: Any) -> Payload: ...
    def to_json(self) -> Any: ...
    @staticmethod
    def from_string(value: str) -> Payload: ...
    def to_string(self) -> str: ...
    @property
    def data(self) -> bytes: ...
    @property
    def metadata(self) -> dict[str, bytes]: ...
    def is_empty(self) -> bool: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class WorkflowStatus:
    """Workflow execution status."""

    @staticmethod
    def running() -> WorkflowStatus: ...
    @staticmethod
    def completed() -> WorkflowStatus: ...
    @staticmethod
    def failed() -> WorkflowStatus: ...
    @staticmethod
    def cancelled() -> WorkflowStatus: ...
    @staticmethod
    def terminated() -> WorkflowStatus: ...
    @staticmethod
    def timed_out() -> WorkflowStatus: ...
    def is_terminal(self) -> bool: ...
    def is_running(self) -> bool: ...
    @property
    def name(self) -> str: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
    def __eq__(self, other: object) -> bool: ...

class RetryPolicy:
    """Retry policy configuration."""

    def __init__(
        self,
        initial_interval_ms: int = 1000,
        backoff_coefficient: float = 2.0,
        max_interval_ms: int = 60000,
        max_attempts: int = 3,
        non_retryable_errors: list[str] | None = None,
    ) -> None: ...
    @staticmethod
    def default_policy() -> RetryPolicy: ...
    @staticmethod
    def no_retry() -> RetryPolicy: ...

    initial_interval_ms: int
    backoff_coefficient: float
    max_interval_ms: int
    max_attempts: int
    non_retryable_errors: list[str]

    def __repr__(self) -> str: ...

class Failure:
    """Error/failure information."""

    def __init__(
        self,
        message: str,
        source: str = "",
        stack_trace: str = "",
        failure_type: str = "",
        cause: Failure | None = None,
    ) -> None: ...
    @staticmethod
    def from_exception(exc: BaseException) -> Failure: ...

    message: str
    source: str
    stack_trace: str
    failure_type: str
    cause: Failure | None

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

# =============================================================================
# Client API
# =============================================================================

class ClientConfig:
    """Configuration for ORCHER client connection."""

    def __init__(
        self,
        server_url: str,
        namespace: str = "default",
        timeout_ms: int = 10000,
        identity: str | None = None,
        tls_ca_cert_path: str | None = None,
        tls_client_cert_path: str | None = None,
        tls_client_key_path: str | None = None,
        api_key: str | None = None,
        connect_timeout_ms: int = 10000,
    ) -> None: ...
    @staticmethod
    def from_env(prefix: str = "ORCHER_") -> ClientConfig: ...

    server_url: str
    namespace: str
    timeout_ms: int
    identity: str | None
    tls_ca_cert_path: str | None
    tls_client_cert_path: str | None
    tls_client_key_path: str | None
    api_key: str | None
    connect_timeout_ms: int

    def __repr__(self) -> str: ...

class Client:
    """Client for interacting with ORCHER server."""

    def __init__(self) -> None: ...
    async def connect(self, config: ClientConfig) -> None: ...
    def is_connected(self) -> bool: ...
    async def start_workflow(
        self,
        workflow_id: str,
        workflow_type: str,
        task_queue: str,
        input: Any | None = None,
        **kwargs: Any,
    ) -> WorkflowHandle: ...
    async def get_workflow_handle(
        self,
        workflow_id: str,
        run_id: str | None = None,
    ) -> WorkflowHandle: ...
    async def list_workflows(
        self,
        page_size: int = 100,
        next_page_token: bytes | None = None,
        workflow_type: str | None = None,
        task_queue: str | None = None,
        status_filter: list[str] | None = None,
        sort_order: str | None = None,
    ) -> dict[str, Any]: ...
    async def search_workflows(
        self,
        query: str,
        page_size: int = 100,
        next_page_token: bytes | None = None,
    ) -> dict[str, Any]: ...
    async def invoke_actor_operation(
        self,
        actor_name: str,
        key: str,
        operation: str,
        payload: bytes,
        timeout_ms: int | None = None,
    ) -> bytes: ...
    async def close(self) -> None: ...
    def __repr__(self) -> str: ...

class WorkflowHandle:
    """Handle to a running or completed workflow."""

    @property
    def workflow_id(self) -> str: ...
    @property
    def run_id(self) -> str: ...
    @property
    def execution(self) -> WorkflowExecution: ...
    async def result(self, timeout_ms: int | None = None) -> Any: ...
    async def status(self) -> WorkflowStatus: ...
    async def send_event(
        self,
        event_name: str,
        payload: Any | None = None,
    ) -> None: ...
    async def query(
        self,
        query_type: str,
        args: Any | None = None,
    ) -> Any: ...
    async def update(
        self,
        update_name: str,
        args: Any | None = None,
    ) -> Any: ...
    async def describe(self) -> dict[str, Any]: ...
    async def cancel(self, cleanup_timeout_ms: int | None = None) -> None: ...
    async def terminate(self, reason: str | None = None) -> None: ...
    async def reset_workflow(
        self,
        target_event_id: int,
        reason: str | None = None,
    ) -> str: ...
    def __repr__(self) -> str: ...

# =============================================================================
# Worker API
# =============================================================================

class WorkerHost:
    """The worker host shared with the Rust and TypeScript SDKs, running.

    It polls the engine, keeps the worker registered, runs sessions and
    drains on shutdown; ``orcher.Worker`` pulls work from it. Messages cross
    as JSON: activations and their answers in the kernel protocol's shapes
    (see ``orcher.worker.activation``). A pull returns None once the host has
    stopped and handed over everything it took.
    """

    @staticmethod
    async def start(config_json: str, registrations_json: str) -> WorkerHost:
        """Start a worker. Raises ValueError for a configuration it cannot use
        and RuntimeError when the worker cannot start."""
        ...
    @property
    def identity(self) -> str: ...
    def wire_metrics(self) -> str: ...
    async def next_activation(self) -> bytes | None: ...
    async def complete_activation(self, result: bytes) -> None: ...
    async def next_task(self) -> bytes | None: ...
    def heartbeat(self, token: bytes, details: bytes | None = None) -> bool:
        """Record a heartbeat; return whether the task was asked to cancel."""
        ...
    async def task_cancelled(self, token: bytes) -> bool:
        """True once the task is asked to cancel, False once it is reported."""
        ...
    async def complete_task(self, token: bytes, result: bytes) -> None: ...
    async def fail_task(
        self,
        token: bytes,
        message: str,
        error_type: str,
        non_retryable: bool = False,
    ) -> None: ...
    async def next_actor_operation(self) -> bytes | None: ...
    async def complete_actor_operation(
        self, operation_id: str, execution_id: str, result: bytes
    ) -> None: ...
    async def fail_actor_operation(
        self, operation_id: str, execution_id: str, message: str, error_type: str
    ) -> None: ...
    async def actor_get_state(
        self, actor_name: str, key: str, state_key: str, execution_id: str
    ) -> bytes | None: ...
    async def actor_set_state(
        self, actor_name: str, key: str, state_key: str, value: bytes, execution_id: str
    ) -> None: ...
    async def actor_delete_state(
        self, actor_name: str, key: str, state_key: str, execution_id: str
    ) -> bool: ...
    async def actor_list_state_keys(
        self, actor_name: str, key: str, execution_id: str, prefix: str
    ) -> list[str]: ...
    async def shutdown(self) -> None:
        """Stop the worker, within its shutdown grace; pulls then return None."""
        ...
    def __repr__(self) -> str: ...

# =============================================================================
# Codecs
# =============================================================================

class GzipCodec:
    """Gzip compression codec for Payloads.

    Compresses Payload data in gzip (RFC 1952) format. Text and JSON data
    typically shrink by 60-90%.
    """

    def __init__(self, level: int = 6) -> None:
        """Create a GzipCodec with compression level 0-9 (default 6)."""
        ...
    @staticmethod
    def fast() -> GzipCodec:
        """Create a GzipCodec with fast compression (level 1)."""
        ...
    @staticmethod
    def best() -> GzipCodec:
        """Create a GzipCodec with best compression (level 9)."""
        ...
    def encode(self, payload: Payload) -> Payload:
        """Compress a Payload."""
        ...
    def decode(self, payload: Payload) -> Payload:
        """Decompress a Payload."""
        ...
    @property
    def compression_level(self) -> int: ...
    def __repr__(self) -> str: ...

class EncryptionCodec:
    """AES-256-GCM encryption codec for Payloads.

    Provides confidentiality, authenticity, and integrity.
    Each encryption uses a unique random nonce.
    """

    def __init__(self, key: bytes) -> None:
        """Create an EncryptionCodec with a 32-byte key.

        Raises:
            ValueError: If key is not exactly 32 bytes.
        """
        ...
    @staticmethod
    def generate_key() -> bytes:
        """Generate a cryptographically secure random 32-byte key."""
        ...
    def encode(self, payload: Payload) -> Payload:
        """Encrypt a Payload."""
        ...
    def decode(self, payload: Payload) -> Payload:
        """Decrypt a Payload."""
        ...
    def __repr__(self) -> str: ...

class CodecChain:
    """Composable chain of codecs applied in sequence.

    Codecs are applied in order during encoding and in reverse during decoding.

    Example:
        chain = CodecChain().with_encryption(key).with_gzip()
        encoded = chain.encode(payload)   # encrypt then compress
        decoded = chain.decode(encoded)   # decompress then decrypt
    """

    def __init__(self) -> None:
        """Create an empty CodecChain."""
        ...
    def with_gzip(self, level: int | None = None) -> CodecChain:
        """Add gzip compression to the chain, returning a new CodecChain."""
        ...
    def with_encryption(self, key: bytes) -> CodecChain:
        """Add AES-256-GCM encryption to the chain, returning a new CodecChain."""
        ...
    def encode(self, payload: Payload) -> Payload:
        """Encode a Payload through the full codec chain."""
        ...
    def decode(self, payload: Payload) -> Payload:
        """Decode a Payload through the full codec chain (reverse order)."""
        ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

# =============================================================================
# Errors
# =============================================================================

class NativeError(Exception):
    """Base exception for native module errors."""

    ...
