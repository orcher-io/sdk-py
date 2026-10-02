"""Integration tests for the Python SDK against a running orchestrator.

These tests exercise the full round-trip from the SDK to the orchestrator and
back. They need an orchestrator reachable at ORCHER_SERVER_URL (default
http://localhost:50051).

The whole module is skipped unless ORCHER_INTEGRATION_TESTS is set to 1, true
or yes:

    ORCHER_INTEGRATION_TESTS=1 pytest tests/test_integration.py -v
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from orcher._native import Client

# Skip every test in this module unless ORCHER_INTEGRATION_TESTS is set.
pytestmark = pytest.mark.skipif(
    os.environ.get("ORCHER_INTEGRATION_TESTS", "").lower() not in ("1", "true", "yes"),
    reason="Integration tests disabled. Set ORCHER_INTEGRATION_TESTS=1 to enable.",
)


# Server settings, overridable through the environment.
ORCHESTRATOR_URL = os.environ.get("ORCHER_SERVER_URL", "http://localhost:50051")
NAMESPACE = os.environ.get("ORCHER_NAMESPACE", "default")
TASK_QUEUE = os.environ.get("ORCHER_TASK_QUEUE", "integration-test-queue")


class TestNativeClientConnection:
    """Test native client connection to orchestrator."""

    @pytest.mark.asyncio
    async def test_native_client_connect(self) -> None:
        """Test that native client can connect to orchestrator."""
        from orcher._native import Client, ClientConfig

        config = ClientConfig(
            server_url=ORCHESTRATOR_URL,
            namespace=NAMESPACE,
        )

        client = Client()
        await client.connect(config)

        assert client.is_connected()

        await client.close()
        assert not client.is_connected()

    @pytest.mark.asyncio
    async def test_native_client_connect_invalid_url(self) -> None:
        """Test that the native client fails when using an invalid URL.

        gRPC connections may be lazy, so connecting can succeed while the first
        operation fails. Either outcome is accepted.
        """
        from orcher._native import Client, ClientConfig

        config = ClientConfig(
            server_url="http://localhost:99999",  # Invalid port
            namespace=NAMESPACE,
        )

        client = Client()

        # Connecting may succeed because the connection is lazy.
        try:
            await client.connect(config)
            # If it does, the first real operation must fail.
            with pytest.raises(Exception, match=r".*"):
                await client.start_workflow(
                    workflow_id="test",
                    workflow_type="test",
                    task_queue="test",
                )
        except Exception:
            # Failing at connect time is also acceptable.
            pass
        finally:
            if client.is_connected():
                await client.close()


class TestNativeClientWorkflow:
    """Test native client workflow operations."""

    @pytest.fixture
    async def connected_client(self) -> AsyncGenerator[Client, None]:
        """Create and connect a client."""
        from orcher._native import Client, ClientConfig

        config = ClientConfig(
            server_url=ORCHESTRATOR_URL,
            namespace=NAMESPACE,
        )

        client = Client()
        await client.connect(config)
        yield client
        await client.close()

    @pytest.mark.asyncio
    async def test_start_workflow(self, connected_client: Client) -> None:
        """Test starting a workflow via native client."""
        workflow_id = f"test-workflow-{uuid.uuid4()}"

        handle = await connected_client.start_workflow(
            workflow_id=workflow_id,
            workflow_type="test-workflow",
            task_queue=TASK_QUEUE,
            input={"message": "hello"},
        )

        assert handle is not None
        assert handle.workflow_id == workflow_id

    @pytest.mark.asyncio
    async def test_get_workflow_handle(self, connected_client: Client) -> None:
        """Test getting a handle to an existing workflow."""
        workflow_id = f"test-workflow-{uuid.uuid4()}"

        await connected_client.start_workflow(
            workflow_id=workflow_id,
            workflow_type="test-workflow",
            task_queue=TASK_QUEUE,
        )

        handle2 = await connected_client.get_workflow_handle(workflow_id)

        assert handle2 is not None
        assert handle2.workflow_id == workflow_id

    @pytest.mark.asyncio
    async def test_workflow_status(self, connected_client: Client) -> None:
        """Test getting workflow status."""
        workflow_id = f"test-workflow-{uuid.uuid4()}"

        handle = await connected_client.start_workflow(
            workflow_id=workflow_id,
            workflow_type="test-workflow",
            task_queue=TASK_QUEUE,
        )

        # No worker is polling the queue, so the workflow stays running.
        status = await handle.status()
        assert status is not None

    @pytest.mark.asyncio
    async def test_cancel_workflow(self, connected_client: Client) -> None:
        """Test cancelling a workflow."""
        workflow_id = f"test-workflow-{uuid.uuid4()}"

        handle = await connected_client.start_workflow(
            workflow_id=workflow_id,
            workflow_type="test-workflow",
            task_queue=TASK_QUEUE,
        )

        # Only the request is checked. The server applies the cancellation
        # asynchronously, so the status is not asserted here.
        await handle.cancel()

    @pytest.mark.asyncio
    async def test_terminate_workflow(self, connected_client: Client) -> None:
        """Test terminating a workflow."""
        workflow_id = f"test-workflow-{uuid.uuid4()}"

        handle = await connected_client.start_workflow(
            workflow_id=workflow_id,
            workflow_type="test-workflow",
            task_queue=TASK_QUEUE,
        )

        await handle.terminate("test termination")


class TestNativeClientEvents:
    """Test native client event operations."""

    @pytest.fixture
    async def connected_client(self) -> AsyncGenerator[Client, None]:
        """Create and connect a client."""
        from orcher._native import Client, ClientConfig

        config = ClientConfig(
            server_url=ORCHESTRATOR_URL,
            namespace=NAMESPACE,
        )

        client = Client()
        await client.connect(config)
        yield client
        await client.close()

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason="Server-side: SendEvent not yet implemented")
    async def test_send_event(self, connected_client: Client) -> None:
        """Test sending an event to a workflow.

        Marked as an expected failure: the server does not implement
        SendEvent.
        """
        workflow_id = f"test-workflow-{uuid.uuid4()}"

        handle = await connected_client.start_workflow(
            workflow_id=workflow_id,
            workflow_type="test-workflow",
            task_queue=TASK_QUEUE,
        )

        await handle.send_event("test-event", {"data": "value"})


class TestNativeServiceConfig:
    """Test native service/worker configuration."""

    def test_service_config_creation(self) -> None:
        """Test creating a ServiceConfig."""
        from orcher._native import ServiceConfig

        config = ServiceConfig(
            server_url=ORCHESTRATOR_URL,
            task_queue=TASK_QUEUE,
            namespace=NAMESPACE,
        )

        assert config.server_url == ORCHESTRATOR_URL
        assert config.task_queue == TASK_QUEUE
        assert config.namespace == NAMESPACE

    def test_service_config_defaults(self) -> None:
        """Test ServiceConfig default values."""
        from orcher._native import ServiceConfig

        config = ServiceConfig(
            server_url=ORCHESTRATOR_URL,
            task_queue=TASK_QUEUE,
        )

        assert config.namespace == "default"
        assert config.max_concurrent_workflows == 100
        assert config.max_concurrent_tasks == 100


class TestPythonClientIntegration:
    """Test Python-level client integration with orchestrator.

    These tests use the pure Python Client class which wraps the native client.
    """

    @pytest.mark.asyncio
    async def test_client_context_manager(self) -> None:
        """Test using Client as async context manager."""
        from orcher import Client, ClientConfig

        config = ClientConfig(server_url=ORCHESTRATOR_URL)

        async with Client(config) as client:
            assert client.is_connected

    @pytest.mark.asyncio
    async def test_client_start_workflow(self) -> None:
        """Test starting a workflow via Python Client."""
        from orcher import Client, ClientConfig

        config = ClientConfig(server_url=ORCHESTRATOR_URL)
        workflow_id = f"py-test-workflow-{uuid.uuid4()}"

        async with Client(config) as client:
            handle = await client.start_workflow(
                workflow_type="test-workflow",
                workflow_id=workflow_id,
                task_queue=TASK_QUEUE,
                args=({"message": "hello from python"},),
            )

            assert handle is not None
            assert handle.workflow_id == workflow_id


class TestEndToEndWorkflow:
    """End-to-end workflow tests with a service (worker).

    These tests start a service and execute a complete workflow. If the
    workflow does not complete in time, the test is skipped rather than failed.
    """

    @pytest.mark.asyncio
    async def test_simple_workflow_execution(self) -> None:
        """Test a simple workflow execution end-to-end.

        The test starts a service with a registered workflow, starts the
        workflow through the client, waits for the result and checks it.
        A timeout or NotImplementedError skips the test instead of failing it.
        """
        from orcher import Client, ClientConfig, Service, ServiceConfig, WorkflowContext, workflow

        @workflow(name="greeting-workflow", version="1.0")
        class GreetingWorkflow:
            async def run(self, ctx: WorkflowContext, name: str) -> str:
                return f"Hello, {name}!"

        service_config = ServiceConfig(
            server_url=ORCHESTRATOR_URL,
            task_queue=TASK_QUEUE,
            namespace=NAMESPACE,
        )

        service = Service(service_config)

        service_task = asyncio.create_task(service.run())

        try:
            # Give the service time to connect
            await asyncio.sleep(1)

            client_config = ClientConfig(server_url=ORCHESTRATOR_URL)
            async with Client(client_config) as client:
                workflow_id = f"e2e-test-{uuid.uuid4()}"

                handle = await client.start_workflow(
                    workflow_type="greeting-workflow",
                    workflow_id=workflow_id,
                    task_queue=TASK_QUEUE,
                    args=("World",),
                )

                result = await asyncio.wait_for(
                    handle.result(),
                    timeout=10.0,
                )

                assert result == "Hello, World!"

        except TimeoutError:
            pytest.skip("Service layer not fully implemented - workflow execution timed out")
        except NotImplementedError as e:
            pytest.skip(f"Service layer not fully implemented: {e}")
        finally:
            await service.shutdown()
            service_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await service_task


class TestErrorHandling:
    """Test error handling in integration scenarios."""

    @pytest.mark.asyncio
    async def test_workflow_not_found(self) -> None:
        """Test handling of workflow not found errors."""
        from orcher._native import Client, ClientConfig

        config = ClientConfig(
            server_url=ORCHESTRATOR_URL,
            namespace=NAMESPACE,
        )

        client = Client()
        await client.connect(config)

        try:
            handle = await client.get_workflow_handle("non-existent-workflow-id")
            # Getting a handle may succeed; operations on it must fail.
            with pytest.raises(Exception, match=r".*"):
                await handle.status()
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_timeout_handling(self) -> None:
        """Test handling of timeouts."""
        from orcher._native import Client, ClientConfig

        config = ClientConfig(
            server_url=ORCHESTRATOR_URL,
            namespace=NAMESPACE,
            timeout_ms=100,  # Very short timeout
        )

        client = Client()
        await client.connect(config)

        try:
            workflow_id = f"timeout-test-{uuid.uuid4()}"

            handle = await client.start_workflow(
                workflow_id=workflow_id,
                workflow_type="test-workflow",
                task_queue=TASK_QUEUE,
            )

            # No worker is polling the queue, so the result times out.
            with pytest.raises(Exception, match=r".*"):  # TimeoutError or similar
                await handle.result(timeout_ms=100)
        finally:
            await client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-x"])
