"""Interceptors for cross-cutting concerns in workflow and task executions.

Interceptors add logging, metrics, tracing, validation, and other
cross-cutting concerns without changing workflow or task code.

Example usage:

    from orcher.interceptors import (
        WorkflowInterceptor,
        TaskInterceptor,
        InterceptorContext,
        ExecutionInfo,
    )
    from orcher.interceptors.builtin import LoggingInterceptor, MetricsInterceptor

    # A custom interceptor
    class MyWorkflowInterceptor(WorkflowInterceptor):
        async def intercept_execute(self, context, input_data, next_fn):
            print(f"Starting workflow: {context.workflow_type}")
            result = await next_fn(input_data)
            print(f"Finished workflow: {context.workflow_type}")
            return result

    # Built-in interceptors
    logging = LoggingInterceptor(log_input=True, log_output=True)
    metrics = MetricsInterceptor()

    service = (
        Worker.builder()
        .workflow_interceptor(logging.workflow())
        .workflow_interceptor(metrics.workflow())
        .task_interceptor(logging.task())
        .task_interceptor(metrics.task())
        .build()
    )
"""

from orcher.interceptors.base import (
    ExecutionInfo,
    Interceptor,
    InterceptorContext,
    NextFn,
    TaskInterceptor,
    WorkflowInterceptor,
)
from orcher.interceptors.chain import (
    InterceptorChain,
    TaskInterceptorChain,
    WorkflowInterceptorChain,
)

__all__ = [
    # Base types
    "Interceptor",
    "InterceptorContext",
    "ExecutionInfo",
    "NextFn",
    # Workflow interceptors
    "WorkflowInterceptor",
    "WorkflowInterceptorChain",
    # Task interceptors
    "TaskInterceptor",
    "TaskInterceptorChain",
    # Generic chain
    "InterceptorChain",
]
