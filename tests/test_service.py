"""
Unit tests for the Worker module.

Tests cover:
- WorkerConfig creation and validation
- WorkerBuilder fluent API
- Worker lifecycle (start/stop)
- Handler registration from GlobalRegistry
"""

import asyncio
import os
from datetime import datetime
from unittest.mock import patch

import pytest

from orcher import (
    GlobalRegistry,
    TaskContext,
    Worker,
    WorkerBuilder,
    WorkerConfig,
    WorkerState,
    WorkflowContext,
    task,
    tasks,
    workflow,
)


class TestWorkerConfig:
    """Tests for WorkerConfig dataclass."""

    def test_valid_config(self) -> None:
        """Test creating a valid WorkerConfig."""
        config = WorkerConfig(
            server_url="http://localhost:50051",
            task_queue="test-queue",
        )
        assert config.server_url == "http://localhost:50051"
        assert config.task_queue == "test-queue"
        assert config.namespace == "default"
        assert config.max_concurrent_workflow_executions == 100
        assert config.max_concurrent_task_executions == 100

    def test_custom_config(self) -> None:
        """Test WorkerConfig with custom values."""
        config = WorkerConfig(
            server_url="http://localhost:50051",
            task_queue="my-queue",
            namespace="production",
            max_concurrent_workflow_executions=50,
            max_concurrent_task_executions=200,
            identity="my-service",
            workflow_poller_count=8,
            task_poller_count=16,
        )
        assert config.namespace == "production"
        assert config.max_concurrent_workflow_executions == 50
        assert config.max_concurrent_task_executions == 200
        assert config.identity == "my-service"
        assert config.workflow_poller_count == 8
        assert config.task_poller_count == 16

    def test_missing_server_url(self) -> None:
        """Test that missing server_url raises ValueError."""
        with pytest.raises(ValueError, match="server_url is required"):
            WorkerConfig(
                server_url="",
                task_queue="test-queue",
            )

    def test_missing_task_queue(self) -> None:
        """Test that missing task_queue raises ValueError."""
        with pytest.raises(ValueError, match="task_queue is required"):
            WorkerConfig(
                server_url="http://localhost:50051",
                task_queue="",
            )

    def test_invalid_concurrent_workflows(self) -> None:
        """Test that invalid max_concurrent_workflow_executions raises ValueError."""
        with pytest.raises(ValueError, match="max_concurrent_workflow_executions must be >= 1"):
            WorkerConfig(
                server_url="http://localhost:50051",
                task_queue="test-queue",
                max_concurrent_workflow_executions=0,
            )

    def test_invalid_concurrent_tasks(self) -> None:
        """Test that invalid max_concurrent_task_executions raises ValueError."""
        with pytest.raises(ValueError, match="max_concurrent_task_executions must be >= 1"):
            WorkerConfig(
                server_url="http://localhost:50051",
                task_queue="test-queue",
                max_concurrent_task_executions=-1,
            )

    def test_invalid_poller_count(self) -> None:
        """Test that invalid poller count raises ValueError."""
        with pytest.raises(ValueError, match="workflow_poller_count must be >= 1"):
            WorkerConfig(
                server_url="http://localhost:50051",
                task_queue="test-queue",
                workflow_poller_count=0,
            )

    def test_auto_generated_identity(self) -> None:
        """Test that identity is auto-generated if not provided."""
        config = WorkerConfig(
            server_url="http://localhost:50051",
            task_queue="test-queue",
        )
        assert config.identity is not None
        assert len(config.identity) > 0
        assert "service-" in config.identity

    def test_from_env(self) -> None:
        """Test creating WorkerConfig from environment variables."""
        with patch.dict(
            os.environ,
            {
                "ORCHER_SERVER_URL": "http://env-server:50051",
                "ORCHER_TASK_QUEUE": "env-queue",
                "ORCHER_NAMESPACE": "env-namespace",
                "ORCHER_MAX_CONCURRENT_WORKFLOWS": "25",
            },
        ):
            config = WorkerConfig.from_env()
            assert config.server_url == "http://env-server:50051"
            assert config.task_queue == "env-queue"
            assert config.namespace == "env-namespace"
            assert config.max_concurrent_workflow_executions == 25

    def test_from_env_missing_required(self) -> None:
        """Test that from_env raises error for missing required env vars."""
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(ValueError, match="SERVER_URL environment variable is required"),
        ):
            WorkerConfig.from_env()

    def test_from_env_custom_prefix(self) -> None:
        """Test from_env with custom prefix."""
        with patch.dict(
            os.environ,
            {
                "MYAPP_SERVER_URL": "http://custom:50051",
                "MYAPP_TASK_QUEUE": "custom-queue",
            },
        ):
            config = WorkerConfig.from_env(prefix="MYAPP_")
            assert config.server_url == "http://custom:50051"
            assert config.task_queue == "custom-queue"


class TestWorkerBuilder:
    """Tests for WorkerBuilder fluent API."""

    def test_basic_build(self) -> None:
        """Test building a Worker with required options."""
        service = (
            WorkerBuilder.create()
            .server_url("http://localhost:50051")
            .namespace("default")
            .task_queue("test-queue")
            .build()
        )
        assert service is not None
        assert service.config.server_url == "http://localhost:50051"
        assert service.config.task_queue == "test-queue"

    def test_builder_chaining(self) -> None:
        """Test that all builder methods return self for chaining."""
        builder = WorkerBuilder.create()

        result = builder.server_url("http://localhost:50051")
        assert result is builder

        result = builder.namespace("default")
        assert result is builder

        result = builder.task_queue("test-queue")
        assert result is builder

        result = builder.max_concurrent_workflows(50)
        assert result is builder

        result = builder.max_concurrent_tasks(100)
        assert result is builder

    def test_all_options(self) -> None:
        """Test building with all configuration options."""
        service = (
            WorkerBuilder.create()
            .server_url("http://localhost:50051")
            .namespace("production")
            .task_queue("order-queue")
            .max_concurrent_workflows(50)
            .max_concurrent_tasks(200)
            .identity("my-worker")
            .workflow_poll_interval(200)
            .task_poll_interval(150)
            .workflow_poller_count(8)
            .task_poller_count(16)
            .shutdown_grace_time(60000)
            .force_shutdown_timeout(120000)
            .version_id("v1.0.0")
            .binary_checksum("abc123")
            .build()
        )

        config = service.config
        assert config.server_url == "http://localhost:50051"
        assert config.namespace == "production"
        assert config.task_queue == "order-queue"
        assert config.max_concurrent_workflow_executions == 50
        assert config.max_concurrent_task_executions == 200
        assert config.identity == "my-worker"
        assert config.workflow_poll_interval_ms == 200
        assert config.task_poll_interval_ms == 150
        assert config.workflow_poller_count == 8
        assert config.task_poller_count == 16
        assert config.shutdown_grace_time_ms == 60000
        assert config.force_shutdown_timeout_ms == 120000
        assert config.version_id == "v1.0.0"
        assert config.binary_checksum == "abc123"

    def test_missing_server_url(self) -> None:
        """Test that build() raises error without server_url."""
        with pytest.raises(ValueError, match="server_url is required"):
            WorkerBuilder.create().task_queue("test").build()

    def test_missing_task_queue(self) -> None:
        """Test that build() raises error without task_queue."""
        with pytest.raises(ValueError, match="task_queue is required"):
            WorkerBuilder.create().server_url("http://localhost:50051").build()

    def test_static_builder_method(self) -> None:
        """Test Worker.builder() static method."""
        builder = Worker.builder()
        assert isinstance(builder, WorkerBuilder)


class TestService:
    """Tests for Worker class."""

    @pytest.fixture(autouse=True)
    def reset_registry(self) -> None:
        """Reset the global registry before each test."""
        GlobalRegistry.get_instance().reset()

    def test_service_creation(self) -> None:
        """Test creating a Worker instance."""
        config = WorkerConfig(
            server_url="http://localhost:50051",
            task_queue="test-queue",
        )
        service = Worker(config)

        assert service.config == config
        assert service.state == WorkerState.STOPPED
        assert not service.is_running()

    def test_service_initial_stats(self) -> None:
        """Test Worker initial statistics."""
        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        stats = service.get_stats()
        assert stats.workflows_executed == 0
        assert stats.tasks_executed == 0
        assert stats.workflows_in_progress == 0
        assert stats.tasks_in_progress == 0
        assert stats.errors == 0
        assert stats.state == WorkerState.STOPPED
        assert stats.started_at is None

    def test_service_identity(self) -> None:
        """Test Worker identity information."""
        service = (
            Worker.builder()
            .server_url("http://localhost:50051")
            .task_queue("test-queue")
            .identity("my-worker")
            .version_id("v1.0.0")
            .binary_checksum("abc123")
            .build()
        )

        identity = service.get_identity()
        assert identity.worker_id == "my-worker"
        assert identity.version_id == "v1.0.0"
        assert identity.binary_checksum == "abc123"

    def test_service_capabilities_empty(self) -> None:
        """Test Worker capabilities with no handlers."""
        service = (
            Worker.builder()
            .server_url("http://localhost:50051")
            .task_queue("test-queue")
            .max_concurrent_workflows(50)
            .max_concurrent_tasks(100)
            .build()
        )

        capabilities = service.get_capabilities()
        assert capabilities.workflows == []
        assert capabilities.tasks == []
        assert capabilities.max_concurrent_workflows == 50
        assert capabilities.max_concurrent_tasks == 100


class TestWorkerWithHandlers:
    """Tests for Worker with registered workflow/task handlers."""

    @pytest.fixture(autouse=True)
    def reset_registry(self) -> None:
        """Reset the global registry before each test."""
        GlobalRegistry.get_instance().reset()

    def test_service_loads_workflows(self) -> None:
        """Test that Worker loads workflows from GlobalRegistry."""

        @workflow(name="TestWorkflow", version="1.0")
        class TestWorkflow:
            async def run(self, ctx: WorkflowContext) -> str:
                return "done"

        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        capabilities = service.get_capabilities()
        assert "TestWorkflow" in capabilities.workflows

    def test_service_loads_tasks(self) -> None:
        """Test that Worker loads tasks from GlobalRegistry."""

        @tasks
        class TestTasks:
            @task(name="test_task")
            async def test_task(self, ctx: TaskContext) -> str:
                return "done"

        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        capabilities = service.get_capabilities()
        assert "test_task" in capabilities.tasks


class TestRegisterTaskInstance:
    """Tests for register_task_instance and fail-fast validation."""

    @pytest.fixture(autouse=True)
    def reset_registry(self) -> None:
        """Reset the global registry before each test."""
        GlobalRegistry.get_instance().reset()

    def test_register_task_instance(self) -> None:
        """Test registering a pre-built task instance."""

        @tasks
        class MyTasks:
            def __init__(self, api_key: str) -> None:
                self.api_key = api_key

            @task(name="my-task")
            async def my_task(self, ctx: TaskContext) -> str:
                return self.api_key

        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        instance = MyTasks(api_key="sk_test_123")
        service.register_task_instance(instance)
        assert service._task_instances[MyTasks] is instance

    def test_register_task_instance_rejects_non_decorated_class(self) -> None:
        """Test that registering a non-@tasks class raises TypeError."""

        class NotATasks:
            pass

        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        with pytest.raises(TypeError, match="is not a @tasks-decorated class"):
            service.register_task_instance(NotATasks())

    def test_fail_fast_missing_instance_for_class_with_deps(self) -> None:
        """Test that run() fails fast when a task class needs deps but no instance registered."""

        @tasks
        class DependentTasks:
            def __init__(self, gateway: object, config: dict) -> None:
                self.gateway = gateway
                self.config = config

            @task(name="dependent-task")
            async def my_task(self, ctx: TaskContext) -> str:
                return "done"

        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        with pytest.raises(RuntimeError, match="requires constructor arguments.*gateway, config"):
            service._validate_task_instances()

    def test_no_fail_fast_when_instance_registered(self) -> None:
        """Test that validation passes when instance is registered."""

        @tasks
        class DependentTasks:
            def __init__(self, gateway: object) -> None:
                self.gateway = gateway

            @task(name="dep-task-ok")
            async def my_task(self, ctx: TaskContext) -> str:
                return "done"

        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        service.register_task_instance(DependentTasks(gateway=object()))
        # Should not raise
        service._validate_task_instances()

    def test_no_fail_fast_for_zero_arg_class(self) -> None:
        """Test that validation passes for classes with no __init__ args."""

        @tasks
        class SimpleTasks:
            @task(name="simple-task")
            async def my_task(self, ctx: TaskContext) -> str:
                return "done"

        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        # No instance registered, but class has no __init__ deps — should pass
        service._validate_task_instances()

    def test_no_fail_fast_for_default_arg_class(self) -> None:
        """Test that validation passes for classes where all __init__ args have defaults."""

        @tasks
        class DefaultTasks:
            def __init__(self, retries: int = 3, verbose: bool = False) -> None:
                self.retries = retries
                self.verbose = verbose

            @task(name="default-task")
            async def my_task(self, ctx: TaskContext) -> str:
                return "done"

        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        # All params have defaults — zero-arg construction is fine
        service._validate_task_instances()


@pytest.mark.skipif(
    os.environ.get("ORCHER_INTEGRATION_TESTS", "").lower() not in ("1", "true", "yes"),
    reason="Integration tests disabled. Set ORCHER_INTEGRATION_TESTS=1 to enable.",
)
class TestWorkerLifecycle:
    """Tests for Worker lifecycle (run/shutdown).

    These tests require a running ORCHER server.
    Set ORCHER_INTEGRATION_TESTS=1 to enable.
    """

    @pytest.fixture(autouse=True)
    def reset_registry(self) -> None:
        """Reset the global registry before each test."""
        GlobalRegistry.get_instance().reset()

    @pytest.mark.asyncio
    async def test_service_run_and_shutdown(self) -> None:
        """Test running and shutting down a Worker."""
        service = (
            Worker.builder()
            .server_url("http://localhost:50051")
            .task_queue("test-queue")
            .shutdown_grace_time(100)  # Fast shutdown for tests
            .build()
        )

        run_task = asyncio.create_task(service.run())

        # Poll for the RUNNING state for at most 0.5s.
        for _ in range(50):
            if service.state == WorkerState.RUNNING:
                break
            await asyncio.sleep(0.01)

        assert service.state == WorkerState.RUNNING
        assert service.is_running()

        await service.shutdown()
        await asyncio.wait_for(run_task, timeout=2.0)

        assert service.state == WorkerState.STOPPED
        assert not service.is_running()

    @pytest.mark.asyncio
    async def test_service_force_shutdown(self) -> None:
        """Test force shutdown."""
        service = (
            Worker.builder()
            .server_url("http://localhost:50051")
            .task_queue("test-queue")
            .shutdown_grace_time(100)
            .build()
        )

        run_task = asyncio.create_task(service.run())

        for _ in range(50):
            if service.state == WorkerState.RUNNING:
                break
            await asyncio.sleep(0.01)

        await service.shutdown(force=True)
        await asyncio.wait_for(run_task, timeout=2.0)

        assert service.state == WorkerState.STOPPED

    @pytest.mark.asyncio
    async def test_service_cannot_start_twice(self) -> None:
        """Test that starting a running service raises error."""
        service = (
            Worker.builder()
            .server_url("http://localhost:50051")
            .task_queue("test-queue")
            .shutdown_grace_time(100)
            .build()
        )

        run_task = asyncio.create_task(service.run())

        for _ in range(50):
            if service.state == WorkerState.RUNNING:
                break
            await asyncio.sleep(0.01)

        with pytest.raises(RuntimeError, match="Cannot start service in state"):
            await service.run()

        await service.shutdown()
        await asyncio.wait_for(run_task, timeout=2.0)

    @pytest.mark.asyncio
    async def test_service_stats_after_run(self) -> None:
        """Test that stats are updated after running."""
        service = (
            Worker.builder()
            .server_url("http://localhost:50051")
            .task_queue("test-queue")
            .shutdown_grace_time(100)
            .build()
        )

        run_task = asyncio.create_task(service.run())

        for _ in range(50):
            if service.state == WorkerState.RUNNING:
                break
            await asyncio.sleep(0.01)

        stats = service.get_stats()
        assert stats.started_at is not None
        assert isinstance(stats.started_at, datetime)
        assert stats.uptime_seconds >= 0

        await service.shutdown()
        await asyncio.wait_for(run_task, timeout=2.0)

        stats = service.get_stats()
        assert stats.stopped_at is not None

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self) -> None:
        """Test that shutdown can be called multiple times."""
        service = (
            Worker.builder()
            .server_url("http://localhost:50051")
            .task_queue("test-queue")
            .shutdown_grace_time(100)
            .build()
        )

        run_task = asyncio.create_task(service.run())

        for _ in range(50):
            if service.state == WorkerState.RUNNING:
                break
            await asyncio.sleep(0.01)

        await service.shutdown()

        # A second shutdown is a safe no-op.
        await service.shutdown()

        await asyncio.wait_for(run_task, timeout=2.0)
        assert service.state == WorkerState.STOPPED

    @pytest.mark.asyncio
    async def test_shutdown_on_stopped_service(self) -> None:
        """Test that shutdown on stopped service is no-op."""
        service = (
            Worker.builder().server_url("http://localhost:50051").task_queue("test-queue").build()
        )

        # Should not raise
        await service.shutdown()
        await service.shutdown()

        assert service.state == WorkerState.STOPPED


class TestWorkerState:
    """Tests for WorkerState enum."""

    def test_all_states(self) -> None:
        """Test all WorkerState values exist."""
        assert WorkerState.STOPPED is not None
        assert WorkerState.STARTING is not None
        assert WorkerState.RUNNING is not None
        assert WorkerState.SHUTTING_DOWN is not None
        assert WorkerState.FORCE_SHUTDOWN is not None

    def test_states_are_distinct(self) -> None:
        """Test that all states are distinct."""
        states = [
            WorkerState.STOPPED,
            WorkerState.STARTING,
            WorkerState.RUNNING,
            WorkerState.SHUTTING_DOWN,
            WorkerState.FORCE_SHUTDOWN,
        ]
        assert len(states) == len(set(states))
