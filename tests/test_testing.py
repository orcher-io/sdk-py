"""
Unit tests for the ORCHER testing utilities.

Tests cover:
- TimeController for time manipulation
- MockTaskRegistry and MockTaskBuilder for task mocking
- TestWorkflowEnvironment for workflow testing
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any

import pytest

from orcher import GlobalRegistry, TaskContext, task, workflow
from orcher.testing import (
    ExecutionStatus,
    MockTaskBuilder,
    MockTaskRegistry,
    TestEnvOptions,
    TestWorkflowEnvironment,
    TestWorkflowOptions,
    TimeController,
)
from orcher.testing.types import (
    ErrorMockStrategy,
    FixedMockStrategy,
    SequenceMockStrategy,
)
from orcher.workflow.context import WorkflowContext


class TestTimeController:
    """Tests for TimeController."""

    def test_initial_time(self) -> None:
        """Test controller initializes with given time."""
        initial = datetime(2025, 1, 1, 12, 0, 0)
        controller = TimeController(initial)
        assert controller.now() == initial

    def test_default_initial_time(self) -> None:
        """Test controller uses current time by default."""
        before = datetime.now()
        controller = TimeController()
        after = datetime.now()
        assert before <= controller.now() <= after

    @pytest.mark.asyncio
    async def test_advance_time(self) -> None:
        """Test advancing time by milliseconds."""
        initial = datetime(2025, 1, 1, 12, 0, 0)
        controller = TimeController(initial)

        await controller.advance(5000)  # 5 seconds

        expected = initial + timedelta(seconds=5)
        assert controller.now() == expected

    @pytest.mark.asyncio
    async def test_advance_to(self) -> None:
        """Test advancing to a specific time."""
        initial = datetime(2025, 1, 1, 12, 0, 0)
        target = datetime(2025, 1, 1, 13, 0, 0)
        controller = TimeController(initial)

        await controller.advance_to(target)

        assert controller.now() == target

    @pytest.mark.asyncio
    async def test_advance_backwards_raises(self) -> None:
        """Test that advancing backwards raises error."""
        initial = datetime(2025, 1, 1, 12, 0, 0)
        controller = TimeController(initial)

        past = datetime(2025, 1, 1, 11, 0, 0)
        with pytest.raises(ValueError, match="Cannot advance time backwards"):
            await controller.advance_to(past)

    @pytest.mark.asyncio
    async def test_timer_fires(self) -> None:
        """Test that timers fire when time advances."""
        controller = TimeController(datetime(2025, 1, 1))
        fired = []

        controller.create_timer(5000, lambda: fired.append("timer1"))

        await controller.advance(3000)
        assert fired == []  # Not fired yet

        await controller.advance(3000)
        assert fired == ["timer1"]  # Now fired

    @pytest.mark.asyncio
    async def test_multiple_timers(self) -> None:
        """Test multiple timers fire in order."""
        controller = TimeController(datetime(2025, 1, 1))
        fired = []

        controller.create_timer(5000, lambda: fired.append("timer1"))
        controller.create_timer(3000, lambda: fired.append("timer2"))
        controller.create_timer(7000, lambda: fired.append("timer3"))

        await controller.advance(10000)

        assert fired == ["timer2", "timer1", "timer3"]

    def test_cancel_timer(self) -> None:
        """Test cancelling a timer."""
        controller = TimeController(datetime(2025, 1, 1))

        timer_id = controller.create_timer(5000, lambda: None)
        assert controller.get_pending_timer_count() == 1

        result = controller.cancel_timer(timer_id)
        assert result is True
        assert controller.get_pending_timer_count() == 0

    @pytest.mark.asyncio
    async def test_cancelled_timer_doesnt_fire(self) -> None:
        """Test that cancelled timers don't fire."""
        controller = TimeController(datetime(2025, 1, 1))
        fired = []

        timer_id = controller.create_timer(5000, lambda: fired.append("timer"))
        controller.cancel_timer(timer_id)

        await controller.advance(10000)
        assert fired == []

    def test_elapsed_time(self) -> None:
        """Test elapsed time calculation."""
        controller = TimeController(datetime(2025, 1, 1))
        assert controller.elapsed_ms() == 0

    @pytest.mark.asyncio
    async def test_run_all_timers(self) -> None:
        """Test running all timers immediately."""
        controller = TimeController(datetime(2025, 1, 1))
        fired = []

        controller.create_timer(5000, lambda: fired.append(1))
        controller.create_timer(10000, lambda: fired.append(2))
        controller.create_timer(15000, lambda: fired.append(3))

        await controller.run_all_timers()

        assert fired == [1, 2, 3]

    def test_reset(self) -> None:
        """Test resetting controller."""
        initial = datetime(2025, 1, 1)
        controller = TimeController(initial)
        controller.set_time(datetime(2025, 6, 1))
        controller.create_timer(1000, lambda: None)

        controller.reset()

        assert controller.now() == initial
        assert controller.get_pending_timer_count() == 0


class TestMockTaskRegistry:
    """Tests for MockTaskRegistry."""

    def test_register_mock(self) -> None:
        """Test registering a mock."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", FixedMockStrategy(value="result"))

        assert registry.has_mock("task1")
        assert not registry.has_mock("task2")

    @pytest.mark.asyncio
    async def test_execute_fixed_mock(self) -> None:
        """Test executing a fixed value mock."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", FixedMockStrategy(value={"id": "123"}))

        result = await registry.execute("task1", {"input": "data"})

        assert result == {"id": "123"}

    @pytest.mark.asyncio
    async def test_execute_sequence_mock(self) -> None:
        """Test executing a sequence mock."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", SequenceMockStrategy(values=["first", "second", "third"]))

        assert await registry.execute("task1", {}) == "first"
        assert await registry.execute("task1", {}) == "second"
        assert await registry.execute("task1", {}) == "third"

    @pytest.mark.asyncio
    async def test_sequence_exhausted(self) -> None:
        """Test that exhausted sequence raises error."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", SequenceMockStrategy(values=["only"]))

        await registry.execute("task1", {})

        with pytest.raises(IndexError, match="sequence exhausted"):
            await registry.execute("task1", {})

    @pytest.mark.asyncio
    async def test_execute_error_mock(self) -> None:
        """Test executing an error mock."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", ErrorMockStrategy(error=ValueError("Test error")))

        with pytest.raises(ValueError, match="Test error"):
            await registry.execute("task1", {})

    @pytest.mark.asyncio
    async def test_execute_unmocked_task_raises(self) -> None:
        """Test that executing unmocked task raises error."""
        registry = MockTaskRegistry()

        with pytest.raises(ValueError, match="not mocked"):
            await registry.execute("unknown_task", {})

    @pytest.mark.asyncio
    async def test_call_tracking(self) -> None:
        """Test that calls are tracked."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", FixedMockStrategy(value="result"))

        await registry.execute("task1", {"a": 1})
        await registry.execute("task1", {"b": 2})

        assert registry.get_call_count("task1") == 2

        calls = registry.get_calls("task1")
        assert len(calls) == 2
        assert calls[0].input == {"a": 1}
        assert calls[1].input == {"b": 2}

    def test_verify_called(self) -> None:
        """Test verify_called assertion."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", FixedMockStrategy(value="x"))

        with pytest.raises(AssertionError, match="to be called"):
            registry.verify_called("task1")

    @pytest.mark.asyncio
    async def test_verify_called_success(self) -> None:
        """Test verify_called passes after call."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", FixedMockStrategy(value="x"))

        await registry.execute("task1", {})
        registry.verify_called("task1")  # Should not raise

    def test_verify_called_times(self) -> None:
        """Test verify_called_times assertion."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", FixedMockStrategy(value="x"))

        with pytest.raises(AssertionError, match="0 time"):
            registry.verify_called_times("task1", 2)

    def test_clear(self) -> None:
        """Test clearing registry."""
        registry = MockTaskRegistry()
        registry.register_mock("task1", FixedMockStrategy(value="x"))

        registry.clear()

        assert not registry.has_mock("task1")


class TestMockTaskBuilder:
    """Tests for MockTaskBuilder fluent API."""

    def test_returns(self) -> None:
        """Test returns() method."""
        registry = MockTaskRegistry()
        builder = MockTaskBuilder("task1", registry)

        builder.returns({"result": "value"})

        assert registry.has_mock("task1")

    def test_throws(self) -> None:
        """Test throws() method."""
        registry = MockTaskRegistry()
        builder = MockTaskBuilder("task1", registry)

        builder.throws(RuntimeError("Error"))

        mock = registry.get_mock("task1")
        assert mock is not None
        assert isinstance(mock.strategy, ErrorMockStrategy)

    def test_throws_string(self) -> None:
        """Test throws() with string argument."""
        registry = MockTaskRegistry()
        builder = MockTaskBuilder("task1", registry)

        builder.throws("Error message")

        mock = registry.get_mock("task1")
        assert mock is not None
        assert isinstance(mock.strategy, ErrorMockStrategy)
        assert str(mock.strategy.error) == "Error message"

    def test_returns_sequence(self) -> None:
        """Test returns_sequence() method."""
        registry = MockTaskRegistry()
        builder = MockTaskBuilder("task1", registry)

        builder.returns_sequence(["a", "b", "c"])

        mock = registry.get_mock("task1")
        assert mock is not None
        assert isinstance(mock.strategy, SequenceMockStrategy)

    @pytest.mark.asyncio
    async def test_with_fn(self) -> None:
        """Test with_fn() method."""
        registry = MockTaskRegistry()
        builder = MockTaskBuilder("calc", registry)

        builder.with_fn(lambda x: x * 2)

        result = await registry.execute("calc", 5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_with_fn_async(self) -> None:
        """Test with_fn() with async function."""
        registry = MockTaskRegistry()
        builder = MockTaskBuilder("calc", registry)

        async def async_fn(x: int) -> int:
            await asyncio.sleep(0)
            return x * 3

        builder.with_fn(async_fn)

        result = await registry.execute("calc", 5)
        assert result == 15


class TestTestWorkflowEnvironment:
    """Tests for TestWorkflowEnvironment."""

    @pytest.fixture(autouse=True)
    def reset_registry(self) -> None:
        """Reset the global registry before each test."""
        GlobalRegistry.get_instance().reset()

    def test_create_environment(self) -> None:
        """Test creating a test environment."""
        env = TestWorkflowEnvironment()
        assert env is not None

    def test_create_with_options(self) -> None:
        """Test creating environment with options."""
        env = TestWorkflowEnvironment(
            TestEnvOptions(
                namespace="custom",
                task_queue="custom-queue",
                timeout_ms=5000,
            )
        )

        stats = env.get_stats()
        assert stats.workflows_executed == 0

    def test_mock_task_fluent_api(self) -> None:
        """Test mock_task returns builder."""
        env = TestWorkflowEnvironment()

        builder = env.mock_task("my_task")

        assert isinstance(builder, MockTaskBuilder)

    def test_mock_task_and_check(self) -> None:
        """Test mocking a task and checking."""
        env = TestWorkflowEnvironment()

        env.mock_task("my_task").returns({"status": "ok"})

        assert env.is_task_mocked("my_task")
        assert not env.is_task_mocked("other_task")

    @pytest.mark.asyncio
    async def test_execute_simple_workflow(self) -> None:
        """Test executing a simple workflow."""

        @workflow(name="TestSimpleWorkflow", version="1.0")
        class TestSimpleWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> dict[str, Any]:
                return {"result": input_data.get("value", 0) * 2}

        env = TestWorkflowEnvironment()

        result = await env.execute_workflow(TestSimpleWorkflow, {"value": 21})

        assert result == {"result": 42}

        stats = env.get_stats()
        assert stats.workflows_executed == 1

    @pytest.mark.asyncio
    async def test_execute_workflow_with_mocked_task(self) -> None:
        """Test workflow that calls a mocked task."""

        @workflow(name="TestTaskWorkflow", version="1.0")
        class TestTaskWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> dict[str, Any]:
                task_result = await ctx.execute_task("fetch_data", input_data)
                return {"fetched": task_result}

        env = TestWorkflowEnvironment()
        env.mock_task("fetch_data").returns({"data": [1, 2, 3]})

        result = await env.execute_workflow(TestTaskWorkflow, {"query": "test"})

        assert result == {"fetched": {"data": [1, 2, 3]}}
        env.assert_task_called("fetch_data", times=1)

    @pytest.mark.asyncio
    async def test_workflow_failure(self) -> None:
        """Test workflow that fails."""

        @workflow(name="TestFailingWorkflow", version="1.0")
        class TestFailingWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> dict[str, Any]:
                raise ValueError("Workflow failed!")

        env = TestWorkflowEnvironment()

        with pytest.raises(ValueError, match="Workflow failed"):
            await env.execute_workflow(TestFailingWorkflow, {})

    @pytest.mark.asyncio
    async def test_assert_workflow_completed(self) -> None:
        """Test assert_workflow_completed."""

        @workflow(name="TestCompletedWorkflow", version="1.0")
        class TestCompletedWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> str:
                return "done"

        env = TestWorkflowEnvironment()

        await env.execute_workflow(
            TestCompletedWorkflow, {}, TestWorkflowOptions(workflow_id="test-wf-1")
        )

        env.assert_workflow_completed("test-wf-1")

    @pytest.mark.asyncio
    async def test_assert_workflow_failed(self) -> None:
        """Test assert_workflow_failed."""

        @workflow(name="TestFailedWorkflow", version="1.0")
        class TestFailedWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> str:
                raise RuntimeError("Expected failure")

        env = TestWorkflowEnvironment()

        with pytest.raises(RuntimeError):
            await env.execute_workflow(
                TestFailedWorkflow, {}, TestWorkflowOptions(workflow_id="test-wf-2")
            )

        env.assert_workflow_failed("test-wf-2")
        env.assert_workflow_failed("test-wf-2", "Expected failure")

    @pytest.mark.asyncio
    async def test_assert_task_called_with(self) -> None:
        """Test assert_task_called_with."""

        @workflow(name="TestAssertWorkflow", version="1.0")
        class TestAssertWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> dict[str, Any]:
                await ctx.execute_task("process", {"id": 123})
                return {}

        env = TestWorkflowEnvironment()
        env.mock_task("process").returns({"processed": True})

        await env.execute_workflow(TestAssertWorkflow, {})

        env.assert_task_called_with("process", {"id": 123})

    @pytest.mark.asyncio
    async def test_assert_task_not_called(self) -> None:
        """Test assert_task_not_called."""

        @workflow(name="TestNoCallWorkflow", version="1.0")
        class TestNoCallWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> dict[str, Any]:
                return {"skipped": True}

        env = TestWorkflowEnvironment()
        env.mock_task("optional_task").returns({})

        await env.execute_workflow(TestNoCallWorkflow, {})

        env.assert_task_not_called("optional_task")

    @pytest.mark.asyncio
    async def test_time_control(self) -> None:
        """Test time control in test environment."""
        env = TestWorkflowEnvironment(TestEnvOptions(initial_time=datetime(2025, 1, 1, 12, 0, 0)))

        assert env.get_current_time() == datetime(2025, 1, 1, 12, 0, 0)

        await env.advance_time(60000)  # 1 minute

        assert env.get_current_time() == datetime(2025, 1, 1, 12, 1, 0)

    def test_get_summary(self) -> None:
        """Test get_summary returns string."""
        env = TestWorkflowEnvironment()

        summary = env.get_summary()

        assert isinstance(summary, str)
        assert "Test Environment Summary" in summary

    @pytest.mark.asyncio
    async def test_cleanup(self) -> None:
        """Test cleanup clears state."""
        env = TestWorkflowEnvironment()
        env.mock_task("task1").returns({})

        await env.cleanup()

        assert not env.is_task_mocked("task1")

    def test_reset(self) -> None:
        """Test reset clears state."""
        env = TestWorkflowEnvironment()
        env.mock_task("task1").returns({})

        env.reset()

        assert not env.is_task_mocked("task1")
        assert env.get_stats().workflows_executed == 0


class TestExecutionTrace:
    """Tests for execution tracing."""

    @pytest.fixture(autouse=True)
    def reset_registry(self) -> None:
        """Reset the global registry before each test."""
        GlobalRegistry.get_instance().reset()

    @pytest.mark.asyncio
    async def test_trace_captured(self) -> None:
        """Test that execution trace is captured."""

        @workflow(name="TestTracedWorkflow", version="1.0")
        class TestTracedWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> str:
                return "result"

        env = TestWorkflowEnvironment()

        await env.execute_workflow(
            TestTracedWorkflow, {}, TestWorkflowOptions(workflow_id="traced-wf")
        )

        trace = env.get_execution_trace("traced-wf")
        assert trace is not None
        assert trace.workflow_id == "traced-wf"
        assert trace.status == ExecutionStatus.COMPLETED
        assert trace.result == "result"

    @pytest.mark.asyncio
    async def test_trace_includes_tasks(self) -> None:
        """Test that trace includes task executions."""

        @workflow(name="TestTraceTasksWorkflow", version="1.0")
        class TestTraceTasksWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> dict[str, Any]:
                r1 = await ctx.execute_task("task1", {"a": 1})
                r2 = await ctx.execute_task("task2", {"b": 2})
                return {"r1": r1, "r2": r2}

        env = TestWorkflowEnvironment()
        env.mock_task("task1").returns("result1")
        env.mock_task("task2").returns("result2")

        await env.execute_workflow(
            TestTraceTasksWorkflow, {}, TestWorkflowOptions(workflow_id="tasks-wf")
        )

        trace = env.get_execution_trace("tasks-wf")
        assert trace is not None
        assert len(trace.tasks_executed) == 2
        assert trace.tasks_executed[0].task_name == "task1"
        assert trace.tasks_executed[1].task_name == "task2"

    @pytest.mark.asyncio
    async def test_workflow_state(self) -> None:
        """Test workflow state tracking."""

        @workflow(name="TestStateWorkflow", version="1.0")
        class TestStateWorkflow:
            async def run(self, ctx: WorkflowContext, **input_data: Any) -> dict[str, Any]:
                ctx.set_state("counter", 0)
                ctx.set_state("counter", ctx.get_state("counter") + 1)
                ctx.set_state("counter", ctx.get_state("counter") + 1)
                return {"final": ctx.get_state("counter")}

        env = TestWorkflowEnvironment()

        result = await env.execute_workflow(
            TestStateWorkflow, {}, TestWorkflowOptions(workflow_id="state-wf")
        )

        assert result == {"final": 2}
        assert env.get_workflow_state("state-wf", "counter") == 2

        env.assert_state_equals("state-wf", "counter", 2)


class TestWorkerCallingConventions:
    """A workflow runs in the test environment as it is written for a worker."""

    @pytest.fixture(autouse=True)
    def reset_registry(self) -> None:
        GlobalRegistry.get_instance().reset()

    @pytest.mark.asyncio
    async def test_function_workflow_calls_task_by_reference_with_kwargs(self) -> None:
        @task(name="send-confirmation")
        async def send_confirmation(ctx: TaskContext, order_id: str) -> str:
            raise AssertionError("a mocked task never runs")

        @workflow(name="confirm-order")
        async def confirm_order(ctx: WorkflowContext, order_id: str) -> str:
            return await ctx.execute_task(send_confirmation, order_id=order_id)

        env = TestWorkflowEnvironment()
        env.mock_task("send-confirmation").returns("sent")

        assert await env.execute_workflow(confirm_order, "order-1") == "sent"
        env.assert_task_called_with("send-confirmation", {"order_id": "order-1"})

    @pytest.mark.asyncio
    async def test_dict_input_is_spread_as_kwargs(self) -> None:
        @workflow(name="greet")
        async def greet(ctx: WorkflowContext, name: str, punctuation: str = "!") -> str:
            return f"hello {name}{punctuation}"

        env = TestWorkflowEnvironment()

        assert await env.execute_workflow(greet, {"name": "ada"}) == "hello ada!"

    @pytest.mark.asyncio
    async def test_none_input_passes_nothing(self) -> None:
        @workflow(name="no-input")
        class NoInput:
            async def run(self, ctx: WorkflowContext) -> str:
                return "ran"

        env = TestWorkflowEnvironment()

        assert await env.execute_workflow(NoInput) == "ran"

    @pytest.mark.asyncio
    async def test_retry_policy_and_timeouts_are_accepted(self) -> None:
        @task(name="charge")
        async def charge(ctx: TaskContext, amount: int) -> str:
            raise AssertionError("a mocked task never runs")

        @workflow(name="pay")
        async def pay(ctx: WorkflowContext, amount: int) -> str:
            return await ctx.execute_task(
                charge, timeout=timedelta(seconds=5), retry_policy=None, amount=amount
            )

        env = TestWorkflowEnvironment()
        env.mock_task("charge").returns("charged")

        assert await env.execute_workflow(pay, 10) == "charged"
        env.assert_task_called_with("charge", {"amount": 10})
