"""Tests for ORCHER execution contexts."""

from datetime import UTC, datetime, timedelta

import pytest


class TestWorkflowContext:
    """Tests for WorkflowContext."""

    def test_context_creation(self) -> None:
        """Test creating a workflow context."""
        from orcher import WorkflowContext, WorkflowInfo

        info = WorkflowInfo(
            workflow_id="test-wf",
            run_id="test-run",
            workflow_type="TestWorkflow",
            task_queue="test-queue",
            namespace="default",
            attempt=1,
            started_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        ctx = WorkflowContext(info)

        assert ctx.info == info
        assert ctx.execution.workflow_id == "test-wf"
        assert ctx.execution.run_id == "test-run"
        assert not ctx.replaying

    def test_context_replaying(self) -> None:
        """Test replaying flag."""
        from orcher import WorkflowContext, WorkflowInfo

        info = WorkflowInfo(
            workflow_id="wf",
            run_id="run",
            workflow_type="Wf",
            task_queue="q",
            namespace="ns",
            attempt=1,
            started_at=datetime.now(),
        )

        ctx = WorkflowContext(info, replaying=True)
        assert ctx.replaying

    def test_deterministic_random(self) -> None:
        """Test that random is deterministic with same seed."""
        from orcher import WorkflowContext, WorkflowInfo

        info1 = WorkflowInfo(
            workflow_id="wf",
            run_id="run",
            workflow_type="Wf",
            task_queue="q",
            namespace="ns",
            attempt=1,
            started_at=datetime.now(),
        )
        info2 = WorkflowInfo(
            workflow_id="wf",
            run_id="run",
            workflow_type="Wf",
            task_queue="q",
            namespace="ns",
            attempt=1,
            started_at=datetime.now(),
        )

        ctx1 = WorkflowContext(info1)
        ctx2 = WorkflowContext(info2)

        # Same seed should produce same sequence
        values1 = [ctx1.random.random() for _ in range(10)]
        values2 = [ctx2.random.random() for _ in range(10)]

        assert values1 == values2

    def test_random_different_workflows(self) -> None:
        """Test that different workflows get different random sequences."""
        from orcher import WorkflowContext, WorkflowInfo

        info1 = WorkflowInfo(
            workflow_id="wf1",
            run_id="run",
            workflow_type="Wf",
            task_queue="q",
            namespace="ns",
            attempt=1,
            started_at=datetime.now(),
        )
        info2 = WorkflowInfo(
            workflow_id="wf2",
            run_id="run",
            workflow_type="Wf",
            task_queue="q",
            namespace="ns",
            attempt=1,
            started_at=datetime.now(),
        )

        ctx1 = WorkflowContext(info1)
        ctx2 = WorkflowContext(info2)

        # Different seeds should produce different sequences (with high probability)
        values1 = [ctx1.random.random() for _ in range(10)]
        values2 = [ctx2.random.random() for _ in range(10)]

        assert values1 != values2

    def test_deterministic_time(self) -> None:
        """Test deterministic time."""
        from orcher import WorkflowContext, WorkflowInfo

        start = datetime(2024, 1, 1, 12, 0, 0)
        info = WorkflowInfo(
            workflow_id="wf",
            run_id="run",
            workflow_type="Wf",
            task_queue="q",
            namespace="ns",
            attempt=1,
            started_at=start,
        )

        ctx = WorkflowContext(info)

        # Time should start at workflow start
        assert ctx.time.now() == start

    def test_query_handler_registration(self) -> None:
        """Test registering query handlers."""
        from orcher import WorkflowContext, WorkflowInfo

        info = WorkflowInfo(
            workflow_id="wf",
            run_id="run",
            workflow_type="Wf",
            task_queue="q",
            namespace="ns",
            attempt=1,
            started_at=datetime.now(),
        )

        ctx = WorkflowContext(info)

        def get_status() -> str:
            return "running"

        ctx.register_query_handler("get-status", get_status)
        assert "get-status" in ctx._query_handlers


class TestWorkflowRandom:
    """Tests for WorkflowRandom."""

    def test_random_float(self) -> None:
        """Test random float generation."""
        from orcher import WorkflowRandom

        rng = WorkflowRandom(42)
        value = rng.random()
        assert 0.0 <= value < 1.0

    def test_randint(self) -> None:
        """Test random integer generation."""
        from orcher import WorkflowRandom

        rng = WorkflowRandom(42)
        for _ in range(100):
            value = rng.randint(1, 10)
            assert 1 <= value <= 10

    def test_choice(self) -> None:
        """Test random choice."""
        from orcher import WorkflowRandom

        rng = WorkflowRandom(42)
        options = ["a", "b", "c"]
        choice = rng.choice(options)
        assert choice in options

    def test_shuffle(self) -> None:
        """Test shuffle."""
        from orcher import WorkflowRandom

        rng = WorkflowRandom(42)
        items = [1, 2, 3, 4, 5]
        original = items.copy()
        rng.shuffle(items)
        # Items should be same but possibly different order
        assert sorted(items) == sorted(original)

    def test_sample(self) -> None:
        """Test sample."""
        from orcher import WorkflowRandom

        rng = WorkflowRandom(42)
        population = [1, 2, 3, 4, 5]
        sample = rng.sample(population, 3)
        assert len(sample) == 3
        assert all(s in population for s in sample)
        assert len(set(sample)) == 3  # All unique

    def test_uuid(self) -> None:
        """Test deterministic UUID generation."""
        from orcher import WorkflowRandom

        rng1 = WorkflowRandom(42)
        rng2 = WorkflowRandom(42)

        uuid1 = rng1.uuid()
        uuid2 = rng2.uuid()

        assert uuid1 == uuid2
        # UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        parts = uuid1.split("-")
        assert len(parts) == 5


class TestWorkflowTime:
    """Tests for WorkflowTime."""

    def test_now(self) -> None:
        """Test getting current time."""
        from orcher import WorkflowTime

        start = datetime(2024, 6, 15, 10, 30, 0)
        time = WorkflowTime(start)

        assert time.now() == start

    def test_advances_to_a_journal_time_and_never_back(self) -> None:

        from orcher import WorkflowTime

        start = datetime(2024, 6, 15, 10, 30, 0, tzinfo=UTC)
        time = WorkflowTime(start)

        an_hour_later_ms = int(start.timestamp() * 1000) + 3_600_000
        time._advance_to_ms(an_hour_later_ms)
        assert time.now() == datetime(2024, 6, 15, 11, 30, 0, tzinfo=UTC)
        time._advance_to_ms(an_hour_later_ms - 60_000)
        assert time.now() == datetime(2024, 6, 15, 11, 30, 0, tzinfo=UTC)
        assert time.started_at() == start
        assert time.elapsed() == timedelta(hours=1)


class TestTaskContext:
    """Tests for TaskContext."""

    def test_context_creation(self) -> None:
        """Test creating a task context."""
        from orcher import TaskContext, TaskInfo

        info = TaskInfo(
            task_id="task-123",
            task_type="process-order",
            workflow_id="wf-456",
            run_id="run-789",
            task_queue="orders",
            namespace="default",
            attempt=1,
            scheduled_at=datetime(2024, 1, 1, 12, 0, 0),
            started_at=datetime(2024, 1, 1, 12, 0, 5),
        )

        ctx = TaskContext(info)

        assert ctx.info == info
        assert ctx.task_id == "task-123"
        assert ctx.task_type == "process-order"
        assert ctx.attempt == 1

    def test_cancellation_token(self) -> None:
        """Test cancellation token."""
        from orcher import TaskContext, TaskInfo

        info = TaskInfo(
            task_id="task",
            task_type="task",
            workflow_id="wf",
            run_id="run",
            task_queue="q",
            namespace="ns",
            attempt=1,
            scheduled_at=datetime.now(),
            started_at=datetime.now(),
        )

        ctx = TaskContext(info)

        assert not ctx.cancellation_token.is_cancelled
        ctx.cancellation_token.cancel()
        assert ctx.cancellation_token.is_cancelled

    def test_raise_if_cancelled(self) -> None:
        """Test raise_if_cancelled."""
        from orcher import CancellationToken
        from orcher.errors import TaskError

        token = CancellationToken()

        # Should not raise when not cancelled
        token.raise_if_cancelled()

        # Should raise when cancelled
        token.cancel()
        with pytest.raises(TaskError):
            token.raise_if_cancelled()

    def test_heartbeat_enabled(self) -> None:
        """Test heartbeat enabled check."""
        from orcher import TaskContext, TaskInfo

        info = TaskInfo(
            task_id="task",
            task_type="task",
            workflow_id="wf",
            run_id="run",
            task_queue="q",
            namespace="ns",
            attempt=1,
            scheduled_at=datetime.now(),
            started_at=datetime.now(),
        )

        ctx_enabled = TaskContext(info, heartbeat_enabled=True)
        ctx_disabled = TaskContext(info, heartbeat_enabled=False)

        assert ctx_enabled.can_heartbeat()
        assert not ctx_disabled.can_heartbeat()

    @pytest.mark.asyncio
    async def test_heartbeat(self) -> None:
        """Test heartbeat call."""
        from orcher import TaskContext, TaskInfo

        info = TaskInfo(
            task_id="task",
            task_type="task",
            workflow_id="wf",
            run_id="run",
            task_queue="q",
            namespace="ns",
            attempt=1,
            scheduled_at=datetime.now(),
            started_at=datetime.now(),
        )

        ctx = TaskContext(info)

        # Should not raise
        await ctx.heartbeat()
        await ctx.heartbeat({"progress": 50})

    def test_repr(self) -> None:
        """Test string representation."""
        from orcher import TaskContext, TaskInfo

        info = TaskInfo(
            task_id="task-123",
            task_type="my-task",
            workflow_id="wf",
            run_id="run",
            task_queue="q",
            namespace="ns",
            attempt=2,
            scheduled_at=datetime.now(),
            started_at=datetime.now(),
        )

        ctx = TaskContext(info)
        repr_str = repr(ctx)

        assert "task-123" in repr_str
        assert "my-task" in repr_str
        assert "attempt=2" in repr_str


class TestChildWorkflowHandle:
    """Tests for ChildWorkflowHandle."""

    def test_handle_creation(self) -> None:
        """Test creating a child workflow handle."""
        from orcher import ChildWorkflowHandle

        handle = ChildWorkflowHandle(
            workflow_id="child-123",
            run_id="run-456",
            workflow_type="ChildWorkflow",
        )

        assert handle.workflow_id == "child-123"
        assert handle.run_id == "run-456"

    def test_handle_repr(self) -> None:
        """Test string representation."""
        from orcher import ChildWorkflowHandle

        handle = ChildWorkflowHandle(
            workflow_id="child-123",
            run_id="run-456",
            workflow_type="ChildWorkflow",
        )

        repr_str = repr(handle)
        assert "child-123" in repr_str
        assert "ChildWorkflow" in repr_str
