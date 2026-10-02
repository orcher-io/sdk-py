"""Tests for ORCHER Client API."""

import pytest


def _engine_reachable(host: str = "localhost", port: int = 50051) -> bool:
    """Return True when something is listening on the gRPC port.

    Several tests below start real workflows against localhost. Without an
    engine they fail with Unavailable, which is noise rather than a signal.
    They are skipped instead, so a local run without an engine is still a
    usable gate.
    """
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.25)
        return sock.connect_ex((host, port)) == 0


requires_engine = pytest.mark.skipif(
    not _engine_reachable(),
    reason="no engine listening on localhost:50051 (start the orchestrator to run this)",
)


class TestClientConfig:
    """Tests for ClientConfig dataclass."""

    def test_valid_config(self) -> None:
        """Test creating a valid configuration."""
        from orcher.client import ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        assert config.server_url == "http://localhost:50051"
        assert config.namespace == "default"
        assert config.identity is None
        # TLS is expressed as three optional PEM paths, not a boolean flag.
        assert config.tls_ca_cert_path is None
        assert config.tls_client_cert_path is None
        assert config.tls_client_key_path is None

    def test_custom_namespace(self) -> None:
        """Test config with custom namespace."""
        from orcher.client import ClientConfig

        config = ClientConfig(
            server_url="http://localhost:50051",
            namespace="production",
        )
        assert config.namespace == "production"

    def test_missing_server_url(self) -> None:
        """Test that missing server_url raises error."""
        from orcher.client import ClientConfig
        from orcher.errors import ConfigurationError

        with pytest.raises(ConfigurationError) as exc_info:
            ClientConfig(server_url="")
        assert "server_url" in str(exc_info.value)

    def test_invalid_server_url(self) -> None:
        """Test that invalid server_url raises error."""
        from orcher.client import ClientConfig
        from orcher.errors import ConfigurationError

        with pytest.raises(ConfigurationError) as exc_info:
            ClientConfig(server_url="invalid-url")
        assert "must start with" in str(exc_info.value)

    def test_https_url(self) -> None:
        """Test config with HTTPS URL."""
        from orcher.client import ClientConfig

        config = ClientConfig(server_url="https://orcher.example.com:50051")
        assert config.server_url == "https://orcher.example.com:50051"

    def test_timeout_settings(self) -> None:
        """Test custom timeout settings."""
        from orcher.client import ClientConfig

        config = ClientConfig(
            server_url="http://localhost:50051",
            connection_timeout=5.0,
            request_timeout=60.0,
        )
        assert config.connection_timeout == 5.0
        assert config.request_timeout == 60.0


class TestClient:
    """Tests for Client class."""

    def test_client_creation(self) -> None:
        """Test creating a client."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        client = Client(config)
        assert client.config == config
        assert not client.is_connected

    def test_client_repr(self) -> None:
        """Test client string representation."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        client = Client(config)
        repr_str = repr(client)
        assert "localhost:50051" in repr_str
        assert "disconnected" in repr_str

    @pytest.mark.asyncio
    async def test_client_connect(self) -> None:
        """Test client connect method."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        client = Client(config)

        await client.connect()
        assert client.is_connected

        await client.close()
        assert not client.is_connected

    @pytest.mark.asyncio
    async def test_client_context_manager(self) -> None:
        """Test client as async context manager."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        async with Client(config) as client:
            assert client.is_connected
        assert not client.is_connected

    @pytest.mark.asyncio
    async def test_double_connect(self) -> None:
        """Test that connecting twice is safe."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        client = Client(config)

        await client.connect()
        await client.connect()  # Idempotent.
        assert client.is_connected

        await client.close()

    @pytest.mark.asyncio
    async def test_double_close(self) -> None:
        """Test that closing twice is safe."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        client = Client(config)

        await client.connect()
        await client.close()
        await client.close()  # Idempotent.
        assert not client.is_connected

    @requires_engine
    @pytest.mark.asyncio
    async def test_start_workflow(self) -> None:
        """Test starting a workflow."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        async with Client(config) as client:
            handle = await client.start_workflow(
                workflow_type="test-workflow",
                workflow_id="test-123",
                task_queue="test-queue",
            )
            assert handle.workflow_id == "test-123"
            assert handle.run_id is not None

    @requires_engine
    @pytest.mark.asyncio
    async def test_start_workflow_with_args(self) -> None:
        """Test starting a workflow with arguments."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        async with Client(config) as client:
            handle = await client.start_workflow(
                workflow_type="greeting-workflow",
                workflow_id="greeting-1",
                task_queue="greetings",
                args=("World", 42),
            )
            assert handle.workflow_id == "greeting-1"

    @pytest.mark.asyncio
    async def test_get_workflow(self) -> None:
        """Test getting a workflow handle."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        async with Client(config) as client:
            handle = await client.get_workflow("existing-workflow")
            assert handle.workflow_id == "existing-workflow"

    @pytest.mark.asyncio
    async def test_get_workflow_with_run_id(self) -> None:
        """Test getting a workflow handle with specific run ID."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        async with Client(config) as client:
            handle = await client.get_workflow(
                "existing-workflow",
                run_id="specific-run",
            )
            assert handle.workflow_id == "existing-workflow"
            assert handle.run_id == "specific-run"

    @pytest.mark.asyncio
    async def test_start_workflow_not_connected(self) -> None:
        """Test that operations fail when not connected."""
        from orcher.client import Client, ClientConfig
        from orcher.errors import ClientError

        config = ClientConfig(server_url="http://localhost:50051")
        client = Client(config)

        with pytest.raises(ClientError):
            await client.start_workflow(
                workflow_type="test",
                workflow_id="test",
                task_queue="test",
            )


class TestWorkflowHandle:
    """Tests for WorkflowHandle class."""

    @requires_engine
    @pytest.mark.asyncio
    async def test_handle_properties(self) -> None:
        """Test workflow handle properties."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        async with Client(config) as client:
            handle = await client.start_workflow(
                workflow_type="test-workflow",
                workflow_id="test-123",
                task_queue="test-queue",
            )

            assert handle.workflow_id == "test-123"
            assert handle.run_id is not None
            assert handle.execution.workflow_id == "test-123"

    @requires_engine
    @pytest.mark.asyncio
    async def test_handle_repr(self) -> None:
        """Test workflow handle string representation."""
        from orcher.client import Client, ClientConfig

        config = ClientConfig(server_url="http://localhost:50051")
        async with Client(config) as client:
            handle = await client.start_workflow(
                workflow_type="test-workflow",
                workflow_id="test-123",
                task_queue="test-queue",
            )

            repr_str = repr(handle)
            assert "WorkflowHandle" in repr_str
            assert "test-123" in repr_str

    # result/status/send_event/query/cancel/terminate are round-trips against
    # a live engine, so they are not unit-tested here. The contract harness
    # (contract/run.py) covers them: see the `echo`, `durable-timer-roundtrip`
    # and `reset-replays-journal-prefix` scenarios.
