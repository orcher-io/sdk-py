"""
ORCHER Python SDK

A Python SDK for building durable workflows and tasks with the ORCHER platform.

Terminology:
    - Workflow: deterministic orchestration logic.
    - Task: a unit of work executed by a worker; side effects are allowed.
    - Event: a named message sent to a workflow from outside.
    - Worker: a process that polls for and executes workflows and tasks.

Example:
    >>> from orcher import workflow, task, WorkflowContext, TaskContext
    >>>
    >>> @task(name="greet")
    ... async def greet(ctx: TaskContext, name: str) -> str:
    ...     return f"Hello, {name}!"
    >>>
    >>> @workflow(name="greeting-workflow", version="1.0")
    ... async def greeting_workflow(ctx: WorkflowContext, name: str) -> str:
    ...     result = await ctx.execute_task(greet, name=name)
    ...     return result
"""

from __future__ import annotations

__version__ = "0.4.6"  # x-release-please-version

# Actor types and client API
from orcher.actor import ActorContext, ActorKey, OperationMode, SharedActorContext
from orcher.client import Client, ClientConfig, WorkflowHandle

# Decorators and registry. The decorators are imported under a `_decorator` alias and
# bound to their public names at the end of this module. Importing a submodule of the
# same name (for example `orcher.workflow`) sets that attribute on the package, which
# would otherwise shadow the decorator.
from orcher.decorators.actor import actor as _actor_decorator
from orcher.decorators.actor import operation as _operation_decorator
from orcher.decorators.query import query as _query_decorator
from orcher.decorators.registry import (
    GlobalRegistry,
    HandlerType,
    QueryMetadata,
    TaskMetadata,
    UpdateMetadata,
    WorkflowMetadata,
)
from orcher.decorators.task import task as _task_decorator
from orcher.decorators.task_class import register_task_class
from orcher.decorators.task_class import tasks as _tasks_decorator
from orcher.decorators.update import update as _update_decorator
from orcher.decorators.workflow import workflow as _workflow_decorator

# Errors
from orcher.errors import (
    ClientError,
    ConfigurationError,
    ErrorCode,
    ErrorSeverity,
    Failure,
    OrcherError,
    TaskError,
    WorkerError,
    WorkflowError,
)

# Interceptors
from orcher.interceptors import (
    ExecutionInfo,
    Interceptor,
    InterceptorChain,
    InterceptorContext,
    TaskInterceptor,
    TaskInterceptorChain,
    WorkflowInterceptor,
    WorkflowInterceptorChain,
)

# Task context - import specific items from task module
from orcher.task import (
    CancellationToken,
    TaskContext,
    TaskInfo,
    TaskReference,
)

# Core types
from orcher.types import (
    Payload,
    RetryPolicy,
    WorkflowExecution,
    WorkflowIdReusePolicy,
    WorkflowStatus,
)

# Worker (runtime)
from orcher.worker import Worker, WorkerBuilder, WorkerConfig, WorkerState

# Workflow context - import specific items from workflow module
from orcher.workflow import (
    ChildWorkflowHandle,
    Saga,
    SagaBuilder,
    WorkflowContext,
    WorkflowInfo,
    WorkflowRandom,
    WorkflowTime,
)

# Bind the public decorator names last, over any submodule attribute of the same name.
workflow = _workflow_decorator
task = _task_decorator
query = _query_decorator
update = _update_decorator
actor = _actor_decorator
operation = _operation_decorator
tasks = _tasks_decorator

__all__ = [
    # Version
    "__version__",
    # Decorators
    "workflow",
    "task",
    "query",
    "update",
    "actor",
    "operation",
    "tasks",
    "register_task_class",
    # Registry (advanced usage)
    "GlobalRegistry",
    "WorkflowMetadata",
    "TaskMetadata",
    "QueryMetadata",
    "UpdateMetadata",
    "HandlerType",
    # Client API
    "Client",
    "ClientConfig",
    "WorkflowHandle",
    # Workflow context
    "WorkflowContext",
    "WorkflowInfo",
    "WorkflowRandom",
    "WorkflowTime",
    "ChildWorkflowHandle",
    "Saga",
    "SagaBuilder",
    # Task context
    "TaskContext",
    "TaskInfo",
    "TaskReference",
    "CancellationToken",
    # Core types
    "WorkflowExecution",
    "WorkflowStatus",
    "Payload",
    "RetryPolicy",
    "WorkflowIdReusePolicy",
    "Failure",
    # Errors
    "OrcherError",
    "WorkflowError",
    "TaskError",
    "ClientError",
    "WorkerError",
    "ConfigurationError",
    "ErrorCode",
    "ErrorSeverity",
    # Worker (runtime)
    "Worker",
    "WorkerBuilder",
    "WorkerConfig",
    "WorkerState",
    # Actor types
    "ActorContext",
    "SharedActorContext",
    "ActorKey",
    "OperationMode",
    # Interceptors
    "Interceptor",
    "InterceptorContext",
    "ExecutionInfo",
    "InterceptorChain",
    "WorkflowInterceptor",
    "WorkflowInterceptorChain",
    "TaskInterceptor",
    "TaskInterceptorChain",
]

# Load the native extension eagerly so a missing build is reported by `_check_native`.
try:
    from orcher._native import _get_core_version as _get_native_version

    __native_version__ = _get_native_version()
except ImportError as e:
    # The native module is missing, usually a source checkout that has not been built.
    __native_version__ = None
    _import_error = e


def _check_native() -> None:
    """Check if native module is available, raise helpful error if not."""
    if __native_version__ is None:
        raise ImportError(
            "ORCHER native module not found. "
            "Please install the package with: pip install orcher-sdk\n"
            "Or build from source with: maturin develop\n"
            f"Original error: {_import_error}"
        )
