"""Tests for the interceptors module."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, NoReturn

import pytest

if TYPE_CHECKING:
    from pytest import LogCaptureFixture

from orcher.interceptors import (
    ExecutionInfo,
    InterceptorChain,
    InterceptorContext,
    TaskInterceptor,
    TaskInterceptorChain,
    WorkflowInterceptor,
    WorkflowInterceptorChain,
)
from orcher.interceptors.builtin import (
    InMemoryMetricsCollector,
    LoggingInterceptor,
    MetricsInterceptor,
    TaskLoggingInterceptor,
    TaskMetricsInterceptor,
    WorkflowLoggingInterceptor,
    WorkflowMetricsInterceptor,
)
from orcher.interceptors.builtin.logging import LoggingConfig

# =============================================================================
# InterceptorContext Tests
# =============================================================================


class TestInterceptorContext:
    """Tests for InterceptorContext."""

    def test_create_context(self) -> None:
        """Test creating a context."""
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )
        assert ctx.workflow_id == "wf-123"
        assert ctx.run_id == "run-456"
        assert ctx.workflow_type == "MyWorkflow"
        assert ctx.task_name is None
        assert ctx.task_id is None
        assert ctx.parent is None

    def test_with_task(self) -> None:
        """Test creating a child context for task."""
        parent = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )
        parent.set_attribute("custom", "value")

        child = parent.with_task("my_task", "task-789")

        assert child.workflow_id == "wf-123"
        assert child.run_id == "run-456"
        assert child.workflow_type == "MyWorkflow"
        assert child.task_name == "my_task"
        assert child.task_id == "task-789"
        assert child.parent is parent
        # Attributes are copied
        assert child.get_attribute("custom") == "value"

    def test_attributes(self) -> None:
        """Test setting and getting attributes."""
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )

        ctx.set_attribute("key1", "value1")
        ctx.set_attribute("key2", 42)

        assert ctx.get_attribute("key1") == "value1"
        assert ctx.get_attribute("key2") == 42
        assert ctx.get_attribute("nonexistent") is None
        assert ctx.get_attribute("nonexistent", "default") == "default"


class TestExecutionInfo:
    """Tests for ExecutionInfo."""

    def test_create_execution_info(self) -> None:
        """Test creating execution info."""
        info = ExecutionInfo(input_data={"x": 1})
        assert info.input_data == {"x": 1}
        assert info.output_data is None
        assert info.error is None
        assert info.duration_ms is None
        assert info.success is True

    def test_execution_info_with_output(self) -> None:
        """Test execution info with output."""
        info = ExecutionInfo(
            input_data={"x": 1},
            output_data={"result": 2},
            duration_ms=100.5,
            success=True,
        )
        assert info.input_data == {"x": 1}
        assert info.output_data == {"result": 2}
        assert info.duration_ms == 100.5
        assert info.success is True

    def test_execution_info_with_error(self) -> None:
        """Test execution info with error."""
        error = ValueError("Something went wrong")
        info = ExecutionInfo(
            input_data={"x": 1},
            error=error,
            success=False,
        )
        assert info.error is error
        assert info.success is False


# =============================================================================
# Base Interceptor Tests
# =============================================================================


class TestInterceptor:
    """Tests for base Interceptor class."""

    def test_default_name(self) -> None:
        """Test default interceptor name."""

        class CustomInterceptor(WorkflowInterceptor):
            pass

        interceptor = CustomInterceptor()
        assert interceptor.name == "CustomInterceptor"

    def test_default_order(self) -> None:
        """Test default interceptor order."""
        interceptor = WorkflowInterceptor()
        assert interceptor.order == 100

    def test_custom_order(self) -> None:
        """Test custom interceptor order."""

        class EarlyInterceptor(WorkflowInterceptor):
            @property
            def order(self) -> int:
                return 10

        interceptor = EarlyInterceptor()
        assert interceptor.order == 10


# =============================================================================
# WorkflowInterceptor Tests
# =============================================================================


class TestWorkflowInterceptor:
    """Tests for WorkflowInterceptor."""

    @pytest.mark.asyncio
    async def test_default_intercept_passes_through(self) -> None:
        """Test that default intercept_execute passes through."""
        interceptor = WorkflowInterceptor()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )

        async def next_fn(data: Any) -> Any:
            return data * 2

        result = await interceptor.intercept_execute(ctx, 21, next_fn)
        assert result == 42

    @pytest.mark.asyncio
    async def test_custom_intercept(self) -> None:
        """Test custom intercept_execute implementation."""
        calls: list[str] = []

        class TrackingInterceptor(WorkflowInterceptor):
            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                calls.append("before")
                result = await next_fn(input_data)
                calls.append("after")
                return result

        interceptor = TrackingInterceptor()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )

        async def next_fn(data: Any) -> Any:
            calls.append("execute")
            return data

        await interceptor.intercept_execute(ctx, "input", next_fn)
        assert calls == ["before", "execute", "after"]

    @pytest.mark.asyncio
    async def test_callbacks_are_called(self) -> None:
        """Test that lifecycle callbacks are available."""
        calls: list[str] = []

        class CallbackInterceptor(WorkflowInterceptor):
            async def on_enter(self, context: InterceptorContext, info: ExecutionInfo) -> None:
                calls.append("enter")

            async def on_exit(self, context: InterceptorContext, info: ExecutionInfo) -> None:
                calls.append("exit")

            async def on_success(self, context: InterceptorContext, info: ExecutionInfo) -> None:
                calls.append("success")

            async def on_error(self, context: InterceptorContext, info: ExecutionInfo) -> None:
                calls.append("error")

        interceptor = CallbackInterceptor()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )
        info = ExecutionInfo(input_data="test")

        await interceptor.on_enter(ctx, info)
        await interceptor.on_success(ctx, info)
        await interceptor.on_exit(ctx, info)

        assert calls == ["enter", "success", "exit"]


# =============================================================================
# TaskInterceptor Tests
# =============================================================================


class TestTaskInterceptor:
    """Tests for TaskInterceptor."""

    @pytest.mark.asyncio
    async def test_default_intercept_passes_through(self) -> None:
        """Test that default intercept_execute passes through."""
        interceptor = TaskInterceptor()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
            task_name="my_task",
            task_id="task-789",
        )

        async def next_fn(data: Any) -> Any:
            return data + " processed"

        result = await interceptor.intercept_execute(ctx, "input", next_fn)
        assert result == "input processed"

    @pytest.mark.asyncio
    async def test_on_retry(self) -> None:
        """Test retry callback."""
        retry_info: list[tuple[int, int, Exception | None]] = []

        class RetryInterceptor(TaskInterceptor):
            async def on_retry(
                self,
                context: InterceptorContext,
                info: ExecutionInfo,
                attempt: int,
                max_attempts: int,
            ) -> None:
                retry_info.append((attempt, max_attempts, info.error))

        interceptor = RetryInterceptor()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
            task_name="my_task",
            task_id="task-789",
        )
        error = ValueError("retry error")
        info = ExecutionInfo(input_data="test", error=error, success=False)

        await interceptor.on_retry(ctx, info, 2, 5)

        assert len(retry_info) == 1
        assert retry_info[0] == (2, 5, error)


# =============================================================================
# InterceptorChain Tests
# =============================================================================


class TestInterceptorChain:
    """Tests for InterceptorChain."""

    def test_add_interceptors(self) -> None:
        """Test adding interceptors."""
        chain: InterceptorChain[str, str] = InterceptorChain()
        i1 = WorkflowInterceptor()
        i2 = WorkflowInterceptor()

        chain.add(i1).add(i2)

        assert len(chain) == 2
        assert chain.interceptors == [i1, i2]

    def test_interceptors_sorted_by_order(self) -> None:
        """Test that interceptors are sorted by order."""

        class LowOrder(WorkflowInterceptor):
            @property
            def order(self) -> int:
                return 10

        class HighOrder(WorkflowInterceptor):
            @property
            def order(self) -> int:
                return 200

        chain: InterceptorChain[str, str] = InterceptorChain()
        high = HighOrder()
        low = LowOrder()
        default = WorkflowInterceptor()  # order 100

        # Add in random order
        chain.add(high).add(default).add(low)

        assert chain.interceptors[0] is low
        assert chain.interceptors[1] is default
        assert chain.interceptors[2] is high

    def test_remove_interceptor(self) -> None:
        """Test removing an interceptor."""

        class NamedInterceptor(WorkflowInterceptor):
            pass

        chain: InterceptorChain[str, str] = InterceptorChain()
        interceptor = NamedInterceptor()
        chain.add(interceptor)

        assert len(chain) == 1
        removed = chain.remove("NamedInterceptor")
        assert removed is True
        assert len(chain) == 0

    def test_remove_nonexistent_returns_false(self) -> None:
        """Test removing nonexistent interceptor returns False."""
        chain: InterceptorChain[str, str] = InterceptorChain()
        removed = chain.remove("DoesNotExist")
        assert removed is False

    def test_clear(self) -> None:
        """Test clearing all interceptors."""
        chain: InterceptorChain[str, str] = InterceptorChain()
        chain.add(WorkflowInterceptor())
        chain.add(WorkflowInterceptor())

        assert len(chain) == 2
        chain.clear()
        assert len(chain) == 0

    @pytest.mark.asyncio
    async def test_execute_without_interceptors(self) -> None:
        """Test executing without interceptors."""
        chain: InterceptorChain[int, int] = InterceptorChain()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )

        async def target(x: int) -> int:
            return x * 2

        result = await chain.execute(ctx, 21, target)
        assert result == 42

    @pytest.mark.asyncio
    async def test_execute_with_interceptors(self) -> None:
        """Test executing with interceptors in chain."""
        calls: list[str] = []

        class Interceptor1(WorkflowInterceptor):
            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                calls.append("i1-before")
                result = await next_fn(input_data)
                calls.append("i1-after")
                return result

        class Interceptor2(WorkflowInterceptor):
            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                calls.append("i2-before")
                result = await next_fn(input_data)
                calls.append("i2-after")
                return result

        chain: InterceptorChain[str, str] = InterceptorChain()
        chain.add(Interceptor1()).add(Interceptor2())

        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )

        async def target(x: str) -> str:
            calls.append("target")
            return x.upper()

        result = await chain.execute(ctx, "hello", target)

        assert result == "HELLO"
        assert calls == ["i1-before", "i2-before", "target", "i2-after", "i1-after"]


# =============================================================================
# WorkflowInterceptorChain Tests
# =============================================================================


class TestWorkflowInterceptorChain:
    """Tests for WorkflowInterceptorChain."""

    def test_only_accepts_workflow_interceptors(self) -> None:
        """Test that only workflow interceptors can be added."""
        chain = WorkflowInterceptorChain()

        # Should work
        chain.add(WorkflowInterceptor())

        # Should fail
        with pytest.raises(TypeError):
            chain.add(TaskInterceptor())  # type: ignore

    @pytest.mark.asyncio
    async def test_notify_enter(self) -> None:
        """Test notifying interceptors of workflow entry."""
        calls: list[str] = []

        class Interceptor1(WorkflowInterceptor):
            async def on_enter(self, context: InterceptorContext, info: ExecutionInfo) -> None:
                calls.append("i1")

        class Interceptor2(WorkflowInterceptor):
            async def on_enter(self, context: InterceptorContext, info: ExecutionInfo) -> None:
                calls.append("i2")

        chain = WorkflowInterceptorChain()
        chain.add(Interceptor1()).add(Interceptor2())

        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )
        info = ExecutionInfo(input_data="test")

        await chain.notify_enter(ctx, info)

        assert calls == ["i1", "i2"]

    @pytest.mark.asyncio
    async def test_notify_exit_reverse_order(self) -> None:
        """Test notifying interceptors of workflow exit in reverse order."""
        calls: list[str] = []

        class Interceptor1(WorkflowInterceptor):
            async def on_exit(self, context: InterceptorContext, info: ExecutionInfo) -> None:
                calls.append("i1")

        class Interceptor2(WorkflowInterceptor):
            async def on_exit(self, context: InterceptorContext, info: ExecutionInfo) -> None:
                calls.append("i2")

        chain = WorkflowInterceptorChain()
        chain.add(Interceptor1()).add(Interceptor2())

        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
        )
        info = ExecutionInfo(input_data="test")

        await chain.notify_exit(ctx, info)

        # Exit is called in reverse order
        assert calls == ["i2", "i1"]


# =============================================================================
# TaskInterceptorChain Tests
# =============================================================================


class TestTaskInterceptorChain:
    """Tests for TaskInterceptorChain."""

    def test_only_accepts_task_interceptors(self) -> None:
        """Test that only task interceptors can be added."""
        chain = TaskInterceptorChain()

        # Should work
        chain.add(TaskInterceptor())

        # Should fail
        with pytest.raises(TypeError):
            chain.add(WorkflowInterceptor())  # type: ignore

    @pytest.mark.asyncio
    async def test_notify_retry(self) -> None:
        """Test notifying interceptors of task retry."""
        retry_info: list[tuple[str, int]] = []

        class RetryInterceptor(TaskInterceptor):
            async def on_retry(
                self,
                context: InterceptorContext,
                info: ExecutionInfo,
                attempt: int,
                max_attempts: int,
            ) -> None:
                retry_info.append((self.name, attempt))

        class RetryInterceptor2(TaskInterceptor):
            async def on_retry(
                self,
                context: InterceptorContext,
                info: ExecutionInfo,
                attempt: int,
                max_attempts: int,
            ) -> None:
                retry_info.append((self.name, attempt))

        chain = TaskInterceptorChain()
        chain.add(RetryInterceptor()).add(RetryInterceptor2())

        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="MyWorkflow",
            task_name="my_task",
            task_id="task-789",
        )
        info = ExecutionInfo(input_data="test", error=ValueError("err"))

        await chain.notify_retry(ctx, info, 3, 5)

        assert len(retry_info) == 2
        assert retry_info[0][1] == 3
        assert retry_info[1][1] == 3


# =============================================================================
# Logging Interceptor Tests
# =============================================================================


class TestLoggingConfig:
    """Tests for LoggingConfig."""

    def test_default_config(self) -> None:
        """Test default configuration."""
        config = LoggingConfig()
        assert config.log_input is True
        assert config.log_output is True
        assert config.log_duration is True
        assert config.max_input_length == 500
        assert config.max_output_length == 500

    def test_custom_config(self) -> None:
        """Test custom configuration."""
        logger = logging.getLogger("custom")
        config = LoggingConfig(
            log_input=False,
            log_output=False,
            log_duration=True,
            max_input_length=100,
            max_output_length=200,
            logger=logger,
        )
        assert config.log_input is False
        assert config.log_output is False
        assert config.max_input_length == 100
        assert config.max_output_length == 200
        assert config.logger is logger


class TestWorkflowLoggingInterceptor:
    """Tests for WorkflowLoggingInterceptor."""

    @pytest.mark.asyncio
    async def test_logs_workflow_execution(self, caplog: LogCaptureFixture) -> None:
        """Test that workflow execution is logged."""
        interceptor = WorkflowLoggingInterceptor()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        async def next_fn(data: Any) -> str:
            return "result"

        with caplog.at_level(logging.INFO, logger="orcher"):
            result = await interceptor.intercept_execute(ctx, "input", next_fn)

        assert result == "result"
        assert "Workflow started: TestWorkflow" in caplog.text
        assert "Workflow completed: TestWorkflow" in caplog.text

    @pytest.mark.asyncio
    async def test_logs_workflow_error(self, caplog: LogCaptureFixture) -> None:
        """Test that workflow errors are logged."""
        interceptor = WorkflowLoggingInterceptor()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        async def next_fn(data: Any) -> NoReturn:
            raise ValueError("test error")

        with caplog.at_level(logging.ERROR, logger="orcher"), pytest.raises(ValueError):
            await interceptor.intercept_execute(ctx, "input", next_fn)

        assert "Workflow failed: TestWorkflow" in caplog.text
        assert "test error" in caplog.text

    @pytest.mark.asyncio
    async def test_respects_config(self, caplog: LogCaptureFixture) -> None:
        """Test that logging respects configuration."""
        config = LoggingConfig(log_input=False, log_output=False)
        interceptor = WorkflowLoggingInterceptor(config)
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        async def next_fn(data: Any) -> str:
            return "secret_output"

        with caplog.at_level(logging.INFO, logger="orcher"):
            await interceptor.intercept_execute(ctx, "secret_input", next_fn)

        # Input and output should not be logged
        assert "secret_input" not in caplog.text
        assert "secret_output" not in caplog.text


class TestTaskLoggingInterceptor:
    """Tests for TaskLoggingInterceptor."""

    @pytest.mark.asyncio
    async def test_logs_task_execution(self, caplog: LogCaptureFixture) -> None:
        """Test that task execution is logged."""
        interceptor = TaskLoggingInterceptor()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
            task_name="test_task",
            task_id="task-789",
        )

        async def next_fn(data: Any) -> str:
            return "result"

        with caplog.at_level(logging.INFO, logger="orcher"):
            result = await interceptor.intercept_execute(ctx, "input", next_fn)

        assert result == "result"
        assert "Task started: test_task" in caplog.text
        assert "Task completed: test_task" in caplog.text

    @pytest.mark.asyncio
    async def test_logs_retry(self, caplog: LogCaptureFixture) -> None:
        """Test that task retry is logged."""
        interceptor = TaskLoggingInterceptor()
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
            task_name="test_task",
            task_id="task-789",
        )
        error = ValueError("retry error")
        info = ExecutionInfo(input_data="test", error=error, success=False)

        with caplog.at_level(logging.WARNING, logger="orcher"):
            await interceptor.on_retry(ctx, info, 2, 5)

        assert "Task retry: test_task" in caplog.text
        assert "attempt 2/5" in caplog.text


class TestLoggingInterceptor:
    """Tests for LoggingInterceptor factory."""

    def test_creates_workflow_interceptor(self) -> None:
        """Test creating workflow interceptor."""
        factory = LoggingInterceptor()
        interceptor = factory.workflow()

        assert isinstance(interceptor, WorkflowLoggingInterceptor)

    def test_creates_task_interceptor(self) -> None:
        """Test creating task interceptor."""
        factory = LoggingInterceptor()
        interceptor = factory.task()

        assert isinstance(interceptor, TaskLoggingInterceptor)

    def test_shares_config(self) -> None:
        """Test that config is shared."""
        factory = LoggingInterceptor(log_input=False, max_input_length=100)

        workflow = factory.workflow()
        task = factory.task()

        assert workflow._config is task._config
        assert workflow._config.log_input is False
        assert workflow._config.max_input_length == 100


# =============================================================================
# Metrics Interceptor Tests
# =============================================================================


class TestInMemoryMetricsCollector:
    """Tests for InMemoryMetricsCollector."""

    def test_record_counter(self) -> None:
        """Test recording counter metrics."""
        collector = InMemoryMetricsCollector()

        collector.record_counter("test.counter", 1)
        collector.record_counter("test.counter", 2)

        assert collector.get_counter_total("test.counter") == 3

    def test_record_gauge(self) -> None:
        """Test recording gauge metrics."""
        collector = InMemoryMetricsCollector()

        collector.record_gauge("test.gauge", 10)
        collector.record_gauge("test.gauge", 20)

        assert collector.get_gauge_latest("test.gauge") == 20

    def test_record_histogram(self) -> None:
        """Test recording histogram metrics."""
        collector = InMemoryMetricsCollector()

        collector.record_histogram("test.histogram", 100)
        collector.record_histogram("test.histogram", 200)

        histograms = collector.get_histogram("test.histogram")
        assert len(histograms) == 2
        assert histograms[0].value == 100
        assert histograms[1].value == 200

    def test_record_with_tags(self) -> None:
        """Test recording metrics with tags."""
        collector = InMemoryMetricsCollector()

        collector.record_counter("test.counter", 1, tags={"env": "prod"})

        counters = collector.get_counter("test.counter")
        assert len(counters) == 1
        assert counters[0].tags == {"env": "prod"}

    def test_workflow_execution_metrics(self) -> None:
        """Test workflow execution aggregation."""
        collector = InMemoryMetricsCollector()

        collector.record_workflow_execution("Workflow1", 100.0, True)
        collector.record_workflow_execution("Workflow1", 200.0, True)
        collector.record_workflow_execution("Workflow1", 150.0, False)

        metrics = collector.get_workflow_metrics("Workflow1")
        assert metrics is not None
        assert metrics.total_count == 3
        assert metrics.success_count == 2
        assert metrics.error_count == 1
        assert metrics.success_rate == pytest.approx(66.67, rel=0.01)
        assert metrics.avg_duration_ms == 150.0
        assert metrics.min_duration_ms == 100.0
        assert metrics.max_duration_ms == 200.0

    def test_task_execution_metrics(self) -> None:
        """Test task execution aggregation."""
        collector = InMemoryMetricsCollector()

        collector.record_task_execution("task1", 50.0, True)
        collector.record_task_execution("task1", 75.0, True)

        metrics = collector.get_task_metrics("task1")
        assert metrics is not None
        assert metrics.total_count == 2
        assert metrics.success_count == 2
        assert metrics.avg_duration_ms == 62.5

    def test_clear(self) -> None:
        """Test clearing all metrics."""
        collector = InMemoryMetricsCollector()

        collector.record_counter("test.counter", 1)
        collector.record_workflow_execution("Workflow1", 100.0, True)

        collector.clear()

        assert collector.get_counter_total("test.counter") == 0
        assert collector.get_workflow_metrics("Workflow1") is None


class TestWorkflowMetricsInterceptor:
    """Tests for WorkflowMetricsInterceptor."""

    @pytest.mark.asyncio
    async def test_records_workflow_metrics(self) -> None:
        """Test that workflow metrics are recorded."""
        collector = InMemoryMetricsCollector()
        interceptor = WorkflowMetricsInterceptor(collector)
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        async def next_fn(data: Any) -> str:
            return "result"

        await interceptor.intercept_execute(ctx, "input", next_fn)

        # Check counters
        assert collector.get_counter_total("workflow.started") == 1
        assert collector.get_counter_total("workflow.completed") == 1
        assert collector.get_counter_total("workflow.failed") == 0

        # Check histogram
        histograms = collector.get_histogram("workflow.duration_ms")
        assert len(histograms) == 1
        assert histograms[0].value > 0

        # Check aggregated metrics
        metrics = collector.get_workflow_metrics("TestWorkflow")
        assert metrics is not None
        assert metrics.success_count == 1

    @pytest.mark.asyncio
    async def test_records_workflow_failure(self) -> None:
        """Test that workflow failures are recorded."""
        collector = InMemoryMetricsCollector()
        interceptor = WorkflowMetricsInterceptor(collector)
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        async def next_fn(data: Any) -> NoReturn:
            raise ValueError("error")

        with pytest.raises(ValueError):
            await interceptor.intercept_execute(ctx, "input", next_fn)

        assert collector.get_counter_total("workflow.started") == 1
        assert collector.get_counter_total("workflow.completed") == 0
        assert collector.get_counter_total("workflow.failed") == 1


class TestTaskMetricsInterceptor:
    """Tests for TaskMetricsInterceptor."""

    @pytest.mark.asyncio
    async def test_records_task_metrics(self) -> None:
        """Test that task metrics are recorded."""
        collector = InMemoryMetricsCollector()
        interceptor = TaskMetricsInterceptor(collector)
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
            task_name="test_task",
            task_id="task-789",
        )

        async def next_fn(data: Any) -> str:
            return "result"

        await interceptor.intercept_execute(ctx, "input", next_fn)

        assert collector.get_counter_total("task.started") == 1
        assert collector.get_counter_total("task.completed") == 1

        metrics = collector.get_task_metrics("test_task")
        assert metrics is not None
        assert metrics.success_count == 1

    @pytest.mark.asyncio
    async def test_records_retry_metrics(self) -> None:
        """Test that retry metrics are recorded."""
        collector = InMemoryMetricsCollector()
        interceptor = TaskMetricsInterceptor(collector)
        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
            task_name="test_task",
            task_id="task-789",
        )
        info = ExecutionInfo(input_data="test", error=ValueError("err"))

        await interceptor.on_retry(ctx, info, 2, 5)

        assert collector.get_counter_total("task.retry") == 1


class TestMetricsInterceptor:
    """Tests for MetricsInterceptor factory."""

    def test_creates_workflow_interceptor(self) -> None:
        """Test creating workflow interceptor."""
        factory = MetricsInterceptor()
        interceptor = factory.workflow()

        assert isinstance(interceptor, WorkflowMetricsInterceptor)

    def test_creates_task_interceptor(self) -> None:
        """Test creating task interceptor."""
        factory = MetricsInterceptor()
        interceptor = factory.task()

        assert isinstance(interceptor, TaskMetricsInterceptor)

    def test_shares_collector(self) -> None:
        """Test that collector is shared."""
        collector = InMemoryMetricsCollector()
        factory = MetricsInterceptor(collector)

        workflow = factory.workflow()
        task = factory.task()

        assert workflow._collector is collector
        assert task._collector is collector
        assert factory.collector is collector


# =============================================================================
# Integration Tests
# =============================================================================


class TestInterceptorIntegration:
    """Integration tests for interceptors."""

    @pytest.mark.asyncio
    async def test_multiple_interceptors_in_chain(self) -> None:
        """Test multiple interceptors working together."""
        execution_order: list[str] = []

        class FirstInterceptor(WorkflowInterceptor):
            @property
            def order(self) -> int:
                return 10

            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                execution_order.append("first-before")
                result = await next_fn(input_data)
                execution_order.append("first-after")
                return result

        class SecondInterceptor(WorkflowInterceptor):
            @property
            def order(self) -> int:
                return 50

            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                execution_order.append("second-before")
                result = await next_fn(input_data)
                execution_order.append("second-after")
                return result

        class ThirdInterceptor(WorkflowInterceptor):
            @property
            def order(self) -> int:
                return 100

            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                execution_order.append("third-before")
                result = await next_fn(input_data)
                execution_order.append("third-after")
                return result

        chain = WorkflowInterceptorChain()
        # Add in random order
        chain.add(ThirdInterceptor())
        chain.add(FirstInterceptor())
        chain.add(SecondInterceptor())

        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        async def target(x: str) -> str:
            execution_order.append("target")
            return x.upper()

        result = await chain.execute(ctx, "hello", target)

        assert result == "HELLO"
        assert execution_order == [
            "first-before",
            "second-before",
            "third-before",
            "target",
            "third-after",
            "second-after",
            "first-after",
        ]

    @pytest.mark.asyncio
    async def test_interceptor_can_modify_input(self) -> None:
        """Test that interceptors can modify input."""

        class ModifyingInterceptor(WorkflowInterceptor):
            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                modified = input_data.upper()
                return await next_fn(modified)

        chain = WorkflowInterceptorChain()
        chain.add(ModifyingInterceptor())

        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        async def target(x: str) -> str:
            return x + "!"

        result = await chain.execute(ctx, "hello", target)

        assert result == "HELLO!"

    @pytest.mark.asyncio
    async def test_interceptor_can_modify_output(self) -> None:
        """Test that interceptors can modify output."""

        class ModifyingInterceptor(WorkflowInterceptor):
            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                result = await next_fn(input_data)
                return result.upper()

        chain = WorkflowInterceptorChain()
        chain.add(ModifyingInterceptor())

        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        async def target(x: str) -> str:
            return x + "!"

        result = await chain.execute(ctx, "hello", target)

        assert result == "HELLO!"

    @pytest.mark.asyncio
    async def test_interceptor_can_short_circuit(self) -> None:
        """Test that interceptors can short-circuit execution."""

        class CachingInterceptor(WorkflowInterceptor):
            def __init__(self) -> None:
                self.cache = {"cached_key": "cached_value"}

            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                if input_data in self.cache:
                    return self.cache[input_data]
                return await next_fn(input_data)

        chain = WorkflowInterceptorChain()
        chain.add(CachingInterceptor())

        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        target_called = False

        async def target(x: str) -> str:
            nonlocal target_called
            target_called = True
            return "from_target"

        # Cached value should be returned without calling target
        result = await chain.execute(ctx, "cached_key", target)
        assert result == "cached_value"
        assert target_called is False

        # Uncached value should call target
        result = await chain.execute(ctx, "uncached_key", target)
        assert result == "from_target"
        assert target_called is True

    @pytest.mark.asyncio
    async def test_error_propagation(self) -> None:
        """Test that errors propagate through interceptors."""
        cleanup_called = False

        class CleanupInterceptor(WorkflowInterceptor):
            async def intercept_execute(
                self,
                context: InterceptorContext,
                input_data: Any,
                next_fn: Callable[[Any], Awaitable[Any]],
            ) -> Any:
                nonlocal cleanup_called
                try:
                    return await next_fn(input_data)
                finally:
                    cleanup_called = True

        chain = WorkflowInterceptorChain()
        chain.add(CleanupInterceptor())

        ctx = InterceptorContext(
            workflow_id="wf-123",
            run_id="run-456",
            workflow_type="TestWorkflow",
        )

        async def target(x: str) -> str:
            raise ValueError("target error")

        with pytest.raises(ValueError, match="target error"):
            await chain.execute(ctx, "hello", target)

        assert cleanup_called is True
