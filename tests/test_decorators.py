"""Tests for ORCHER decorators."""


import pytest


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    """Reset the global registry before each test."""
    from orcher.decorators import GlobalRegistry

    GlobalRegistry.reset()


class TestWorkflowDecorator:
    """Tests for @workflow decorator."""

    def test_workflow_registration(self) -> None:
        """Test that @workflow registers the workflow."""
        from orcher import WorkflowContext, workflow
        from orcher.decorators import GlobalRegistry

        @workflow(name="test-workflow", version="1.0")
        class TestWorkflow:
            async def run(self, ctx: WorkflowContext) -> str:
                return "done"

        registry = GlobalRegistry.get_instance()
        assert registry.has_workflow("test-workflow")

        metadata = registry.get_workflow("test-workflow")
        assert metadata is not None
        assert metadata.name == "test-workflow"
        assert metadata.version == "1.0"
        assert metadata.cls is TestWorkflow

    def test_workflow_with_description(self) -> None:
        """Test workflow with description."""
        from orcher import WorkflowContext, workflow
        from orcher.decorators import GlobalRegistry

        @workflow(name="described-workflow", description="A test workflow")
        class DescribedWorkflow:
            async def run(self, ctx: WorkflowContext) -> None:
                pass

        metadata = GlobalRegistry.get_instance().get_workflow("described-workflow")
        assert metadata is not None
        assert metadata.description == "A test workflow"

    def test_workflow_missing_run_method(self) -> None:
        """Test that workflow without run method raises error."""
        from orcher import workflow

        with pytest.raises(ValueError, match="must have a 'run' method"):

            @workflow(name="invalid-workflow")
            class InvalidWorkflow:
                pass

    def test_workflow_run_missing_ctx_parameter(self) -> None:
        """Test that workflow run with missing ctx raises error."""
        from orcher import workflow

        with pytest.raises(ValueError, match="must accept at least"):

            @workflow(name="missing-ctx-workflow")
            class MissingCtxWorkflow:
                async def run(self) -> None:
                    pass

    def test_workflow_duplicate_name(self) -> None:
        """Test that duplicate workflow names raise error."""
        from orcher import WorkflowContext, workflow

        @workflow(name="duplicate-workflow")
        class FirstWorkflow:
            async def run(self, ctx: WorkflowContext) -> None:
                pass

        with pytest.raises(ValueError, match="already registered"):

            @workflow(name="duplicate-workflow")
            class SecondWorkflow:
                async def run(self, ctx: WorkflowContext) -> None:
                    pass

    def test_workflow_metadata_attached(self) -> None:
        """Test that workflow metadata is attached to class."""
        from orcher import WorkflowContext, workflow

        @workflow(name="attached-workflow", version="2.0")
        class AttachedWorkflow:
            async def run(self, ctx: WorkflowContext) -> None:
                pass

        assert hasattr(AttachedWorkflow, "__orcher_workflow__")
        assert AttachedWorkflow.__orcher_workflow_name__ == "attached-workflow"
        assert AttachedWorkflow.__orcher_workflow_version__ == "2.0"

    def test_workflow_with_parameters(self) -> None:
        """Test workflow with input parameters."""
        from orcher import WorkflowContext, workflow
        from orcher.decorators import GlobalRegistry

        @workflow(name="params-workflow")
        class ParamsWorkflow:
            async def run(self, ctx: WorkflowContext, name: str, count: int) -> str:
                return f"{name}:{count}"

        metadata = GlobalRegistry.get_instance().get_workflow("params-workflow")
        assert metadata is not None


class TestTaskDecorator:
    """Tests for @task decorator."""

    def test_task_decoration(self) -> None:
        """Test that @task decorates the method."""
        from orcher import TaskContext, task

        class TestTasks:
            @task(name="test-task")
            async def my_task(self, ctx: TaskContext, value: int) -> int:
                return value * 2

        # Task metadata should be attached to method
        assert hasattr(TestTasks.my_task, "__orcher_is_task__")
        assert TestTasks.my_task.__orcher_task_name__ == "test-task"

    def test_task_with_options(self) -> None:
        """Test task with timeout and retry options."""
        from orcher import TaskContext, task

        class OptionsTasks:
            @task(
                name="options-task",
                timeout=30.0,
                heartbeat_timeout=10.0,
                retry_policy={"max_attempts": 5},
            )
            async def my_task(self, ctx: TaskContext) -> None:
                pass

        assert OptionsTasks.my_task.__orcher_task_timeout__ == 30.0
        assert OptionsTasks.my_task.__orcher_task_heartbeat_timeout__ == 10.0
        assert OptionsTasks.my_task.__orcher_task_retry_policy__ == {"max_attempts": 5}

    def test_task_missing_ctx_parameter(self) -> None:
        """Test that task without ctx raises error."""
        from orcher import task

        with pytest.raises(ValueError, match="must accept at least"):

            class InvalidTasks:
                @task(name="invalid-task")
                async def my_task(self) -> None:
                    pass


class TestTaskReference:
    """Tests for TaskReference."""

    def test_task_reference_creation(self) -> None:
        """Test creating a TaskReference."""
        from orcher import TaskReference

        class DummyClass:
            pass

        ref = TaskReference(
            task_name="my-task",
            handler_class=DummyClass,
            method_name="my_method",
        )

        assert ref.task_name == "my-task"
        assert ref.handler_class is DummyClass
        assert ref.method_name == "my_method"

    def test_task_reference_equality(self) -> None:
        """Test TaskReference equality."""
        from orcher import TaskReference

        class DummyClass:
            pass

        ref1 = TaskReference("task", DummyClass, "method")
        ref2 = TaskReference("task", DummyClass, "method")
        ref3 = TaskReference("other", DummyClass, "method")

        assert ref1 == ref2
        assert ref1 != ref3

    def test_task_reference_hash(self) -> None:
        """Test TaskReference can be used in sets/dicts."""
        from orcher import TaskReference

        class DummyClass:
            pass

        ref1 = TaskReference("task", DummyClass, "method")
        ref2 = TaskReference("task", DummyClass, "method")

        task_set = {ref1}
        assert ref2 in task_set

    def test_task_reference_repr(self) -> None:
        """Test TaskReference string representation."""
        from orcher import TaskReference

        class MyTasks:
            pass

        ref = TaskReference("my-task", MyTasks, "do_something")
        repr_str = repr(ref)

        assert "my-task" in repr_str
        assert "MyTasks" in repr_str
        assert "do_something" in repr_str


class TestTasks:
    """Tests for tasks function."""

    def test_register_creates_task_references(self) -> None:
        """Test that tasks creates TaskReferences."""
        from orcher import TaskContext, TaskReference, task, tasks
        from orcher.decorators import GlobalRegistry

        class PaymentTasks:
            @task(name="charge-card")
            async def charge_card(self, ctx: TaskContext, amount: int) -> str:
                return f"charged-{amount}"

            @task(name="refund")
            async def refund(self, ctx: TaskContext, tx_id: str) -> bool:
                return True

        # Register the task class
        tasks(PaymentTasks)

        # Check TaskReferences are created
        assert isinstance(PaymentTasks.charge_card, TaskReference)
        assert isinstance(PaymentTasks.refund, TaskReference)
        assert PaymentTasks.charge_card.task_name == "charge-card"
        assert PaymentTasks.refund.task_name == "refund"

        # Check registry
        registry = GlobalRegistry.get_instance()
        assert registry.has_task("charge-card")
        assert registry.has_task("refund")

    def test_register_task_metadata(self) -> None:
        """Test task metadata is properly stored."""
        from orcher import TaskContext, task, tasks
        from orcher.decorators import GlobalRegistry

        class MetadataTasks:
            @task(name="meta-task", timeout=60.0)
            async def meta_task(self, ctx: TaskContext) -> None:
                pass

        tasks(MetadataTasks)

        metadata = GlobalRegistry.get_instance().get_task("meta-task")
        assert metadata is not None
        assert metadata.name == "meta-task"
        assert metadata.handler_class is MetadataTasks
        assert metadata.method_name == "meta_task"
        assert metadata.timeout == 60.0

    def test_duplicate_task_name_raises(self) -> None:
        """Test that duplicate task names raise error."""
        from orcher import TaskContext, task, tasks

        class FirstTasks:
            @task(name="duplicate-task")
            async def task1(self, ctx: TaskContext) -> None:
                pass

        class SecondTasks:
            @task(name="duplicate-task")
            async def task2(self, ctx: TaskContext) -> None:
                pass

        tasks(FirstTasks)

        with pytest.raises(ValueError, match="already registered"):
            tasks(SecondTasks)


class TestGlobalRegistry:
    """Tests for GlobalRegistry."""

    def test_singleton(self) -> None:
        """Test that GlobalRegistry is a singleton."""
        from orcher.decorators import GlobalRegistry

        reg1 = GlobalRegistry.get_instance()
        reg2 = GlobalRegistry.get_instance()
        assert reg1 is reg2

    def test_list_workflows(self) -> None:
        """Test listing all workflows."""
        from orcher import WorkflowContext, workflow
        from orcher.decorators import GlobalRegistry

        @workflow(name="wf1")
        class Workflow1:
            async def run(self, ctx: WorkflowContext) -> None:
                pass

        @workflow(name="wf2")
        class Workflow2:
            async def run(self, ctx: WorkflowContext) -> None:
                pass

        registry = GlobalRegistry.get_instance()
        workflows = registry.list_workflows()
        names = [w.name for w in workflows]

        assert "wf1" in names
        assert "wf2" in names
        assert registry.workflow_count == 2

    def test_list_tasks(self) -> None:
        """Test listing all tasks."""
        from orcher import TaskContext, task, tasks
        from orcher.decorators import GlobalRegistry

        class Tasks:
            @task(name="t1")
            async def task1(self, ctx: TaskContext) -> None:
                pass

            @task(name="t2")
            async def task2(self, ctx: TaskContext) -> None:
                pass

        tasks(Tasks)

        registry = GlobalRegistry.get_instance()
        tasks = registry.list_tasks()
        names = [t.name for t in tasks]

        assert "t1" in names
        assert "t2" in names
        assert registry.task_count == 2

    def test_reset(self) -> None:
        """Test resetting the registry."""
        from orcher import WorkflowContext, workflow
        from orcher.decorators import GlobalRegistry

        @workflow(name="reset-test")
        class ResetWorkflow:
            async def run(self, ctx: WorkflowContext) -> None:
                pass

        registry = GlobalRegistry.get_instance()
        assert registry.has_workflow("reset-test")

        GlobalRegistry.reset()
        assert not registry.has_workflow("reset-test")
        assert registry.workflow_count == 0
