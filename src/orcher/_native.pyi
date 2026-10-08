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

def _parse_execution_result(result_json: str) -> str:
    """Read an activation's result as sdk-core does; raise ValueError if it would reject it."""
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
# Service API
# =============================================================================

class WorkerConfig:
    """Configuration for ``BridgeWorker``.

    ``api_key`` is sent as ``authorization: Bearer <key>`` on every request the
    worker makes; ``repr`` never shows it.
    """

    def __init__(
        self,
        server_url: str,
        task_queue: str,
        namespace: str = "default",
        max_concurrent_workflows: int = 100,
        max_concurrent_tasks: int = 100,
        identity: str | None = None,
        workflow_poller_count: int = 2,
        task_poller_count: int = 4,
        actor_poller_count: int = 4,
        max_concurrent_actors: int = 100,
        organization_id: str | None = None,
        tls_ca_cert_path: str | None = None,
        tls_client_cert_path: str | None = None,
        tls_client_key_path: str | None = None,
        version_id: str | None = None,
        api_key: str | None = None,
    ) -> None: ...

    server_url: str
    namespace: str
    task_queue: str
    max_concurrent_workflows: int
    max_concurrent_tasks: int
    identity: str | None
    workflow_poller_count: int
    task_poller_count: int
    actor_poller_count: int
    max_concurrent_actors: int
    organization_id: str | None
    api_key: str | None
    tls_ca_cert_path: str | None
    tls_client_cert_path: str | None
    tls_client_key_path: str | None
    version_id: str | None

    def __repr__(self) -> str: ...

class BridgeWorker:
    """
    Worker bridge between Python handlers and the Rust SDK core.

    The Rust core owns all gRPC traffic and execution state. Python receives
    work from it, runs the user's handlers, and hands the results back.
    """

    def __init__(self, config: WorkerConfig) -> None: ...
    async def register_actor_handlers(
        self,
        handlers_json: str,
        metadata_json: str,
    ) -> str:
        """
        Register actor handlers (with per-operation modes) with the server.

        handlers_json is a JSON array of {"actor_name", "operation", "mode"}
        entries where mode is "exclusive" or "shared". Registration is what
        lets the server resolve an operation's concurrency mode — without it,
        every operation conservatively runs exclusive.

        Returns the server-issued registration_id. Raises on RPC failure.
        """
        ...

    async def invoke_actor_operation(
        self,
        actor_name: str,
        key: str,
        operation: str,
        payload: bytes,
        timeout_ms: int | None = None,
    ) -> bytes:
        """
        Invoke an actor operation (client-side InvokeOperation RPC) and wait
        for its result bytes. Raises on execution failure.
        """
        ...

    async def poll_workflow_task(self) -> bytes | None:
        """
        Wait for the next workflow execution the Rust core has received.

        Returns JSON-encoded bytes with execution_request, workflow_id, run_id,
        task_token and stream_entry_id, or None if no work is available.
        Raises ServiceShutdownEvent when the worker is shutting down.
        """
        ...

    async def poll_task(self) -> bytes | None:
        """
        Wait for the next task execution the Rust core has received.

        Returns the JSON-encoded task request, or None if no work is available.
        Raises ServiceShutdownEvent when the worker is shutting down.
        """
        ...

    async def complete_workflow_task(
        self,
        workflow_id: str,
        execution_id: str,
        result_json: str,
        task_token: str,
        stream_entry_id: str | None = None,
    ) -> None:
        """
        Complete a workflow execution by handing its result to the Rust core.

        The core reports the completion to the server.
        """
        ...

    async def fail_workflow_task(
        self,
        workflow_id: str,
        execution_id: str,
        task_token: str,
        error_message: str,
        error_type: str,
    ) -> None:
        """
        Fail a workflow execution by handing its error to the Rust core.

        The core reports the failure to the server.
        """
        ...

    async def complete_task(
        self,
        task_token: str,
        result_json: str,
    ) -> None:
        """Complete a task execution."""
        ...

    def heartbeat_task(self, task_token: str, details: bytes | None = None) -> bool:
        """Record a heartbeat from a running task's code.

        The worker heartbeats every task on its own; this is sent with the
        next heartbeat due, never waiting. Returns whether the task has been
        asked to stop.
        """
        ...

    async def wait_task_cancelled(self, task_token: str) -> bool:
        """Resolve ``True`` once the engine asks the task to stop, and
        ``False`` once its outcome is reported."""
        ...

    async def fail_task(
        self,
        task_token: str,
        error_message: str,
        error_type: str,
        non_retryable: bool = False,
    ) -> None:
        """Fail a task execution.

        ``error_type`` is what a retry policy's non-retryable list is matched
        against; ``non_retryable`` stops retries whatever the policy allows.
        """
        ...

    def request_shutdown(self) -> None:
        """Request shutdown of the worker."""
        ...

    async def stop_drivers(self) -> None:
        """Stop the workflow and task drivers once they have sent the results they hold."""
        ...

    def is_shutdown_requested(self) -> bool:
        """Check if shutdown has been requested."""
        ...

    def available_workflow_slots(self) -> int:
        """Get available workflow execution slots."""
        ...

    def available_task_slots(self) -> int:
        """Get available task execution slots."""
        ...

class Service:
    """Native worker runtime that runs registered workflow and task handlers.

    ``orcher.Worker`` does not use it; it drives ``BridgeWorker`` instead.
    """

    def __init__(self, config: WorkerConfig) -> None: ...
    def register_workflow(
        self,
        workflow_type: str,
        handler: Any,
    ) -> None: ...
    def register_task(
        self,
        task_type: str,
        handler: Any,
    ) -> None: ...
    @property
    def state(self) -> str: ...
    def is_running(self) -> bool: ...
    async def run_with_handlers(
        self,
        workflow_executor: Any,
        task_executor: Any,
    ) -> None: ...
    def get_bridge_worker(self) -> BridgeWorker | None: ...
    async def shutdown(
        self,
        force: bool = False,
        timeout_ms: int | None = None,
    ) -> None: ...
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

class ServiceShutdownEvent(Exception):  # noqa: N818
    """
    Raised when a poll returns because the worker is shutting down.

    Polling loops catch it to exit cleanly.
    """

    ...
