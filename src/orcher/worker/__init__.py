"""
Worker runtime for executing workflows and tasks.

The Worker polls the Orcher server for work and runs the registered handlers.

Components:
- Worker: Main worker class that coordinates polling and execution
- WorkerBuilder: Fluent API for constructing Worker instances
- WorkerConfig: Configuration options for the Worker
- WorkflowPoller/TaskPoller: Poll the server for work
- WorkflowExecutor/TaskExecutor: Execute workflow and task code

Example:
    >>> from orcher import Worker, workflow, task
    >>>
    >>> @workflow(name="OrderWorkflow", version="1.0")
    >>> class OrderWorkflow:
    >>>     async def run(self, ctx, order_id: str) -> dict:
    >>>         return {"status": "completed"}
    >>>
    >>> @task(name="SendEmail")
    >>> async def send_email(ctx, to: str, subject: str) -> bool:
    >>>     return True
    >>>
    >>> async def main():
    >>>     worker = (
    >>>         Worker.builder()
    >>>         .server_url("http://localhost:50051")
    >>>         .namespace("default")
    >>>         .task_queue("order-queue")
    >>>         .build()
    >>>     )
    >>>     await worker.run()
"""

from orcher.worker.builder import WorkerBuilder
from orcher.worker.config import WorkerConfig

# Execution Pool (concurrent execution)
from orcher.worker.execution_pool import (
    DetachedExecutionManager,
    ExecutionPool,
    WorkResult,
    WorkUnit,
    WorkUnitType,
)

# Executors
from orcher.worker.executor import (
    ExecutionResult,
    ExecutionStatus,
    Executor,
    ExecutorConfig,
    TaskExecutor,
    WorkflowExecutor,
)

# Pollers
from orcher.worker.poller import (
    Poller,
    PollerConfig,
    PollerState,
    PollResult,
    TaskPoller,
    TaskTask,
    WorkflowPoller,
    WorkflowTask,
)
from orcher.worker.worker import Worker, WorkerState

__all__ = [
    # Worker
    "Worker",
    "WorkerConfig",
    "WorkerBuilder",
    "WorkerState",
    # Pollers
    "Poller",
    "PollerState",
    "PollerConfig",
    "WorkflowPoller",
    "TaskPoller",
    "PollResult",
    "WorkflowTask",
    "TaskTask",
    # Executors
    "Executor",
    "ExecutorConfig",
    "WorkflowExecutor",
    "TaskExecutor",
    "ExecutionResult",
    "ExecutionStatus",
    # Execution Pool
    "ExecutionPool",
    "DetachedExecutionManager",
    "WorkUnit",
    "WorkUnitType",
    "WorkResult",
]
