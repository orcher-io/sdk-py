"""``WorkerBuilder.interceptor()`` must add what it is given or refuse it.

The built-in ``LoggingInterceptor``, ``MetricsInterceptor`` and
``TracingInterceptor`` are factories, not interceptors: ``workflow()`` and
``task()`` make the interceptors. The ``interceptor()`` docstring showed
``.interceptor(LoggingInterceptor())``, and the builder dropped it without a
word because it was neither a workflow nor a task interceptor. A factory now
adds both of its interceptors, and anything else that would be dropped raises.
"""

from __future__ import annotations

import warnings
from typing import Any

import pytest

from orcher.interceptors import InterceptorFactory, NextFn
from orcher.interceptors.base import (
    Interceptor,
    InterceptorContext,
    TaskInterceptor,
    WorkflowInterceptor,
)
from orcher.interceptors.builtin import (
    LoggingInterceptor,
    MetricsInterceptor,
    TaskLoggingInterceptor,
    TaskMetricsInterceptor,
    TaskTracingInterceptor,
    TracingInterceptor,
    WorkflowLoggingInterceptor,
    WorkflowMetricsInterceptor,
    WorkflowTracingInterceptor,
)
from orcher.worker.builder import WorkerBuilder


def _tracing_factory() -> TracingInterceptor:
    # Warns when OpenTelemetry is not installed; the factory still works.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ImportWarning)
        return TracingInterceptor()


@pytest.mark.parametrize(
    ("make_factory", "workflow_type", "task_type"),
    [
        (LoggingInterceptor, WorkflowLoggingInterceptor, TaskLoggingInterceptor),
        (MetricsInterceptor, WorkflowMetricsInterceptor, TaskMetricsInterceptor),
        (_tracing_factory, WorkflowTracingInterceptor, TaskTracingInterceptor),
    ],
)
def test_builtin_factory_adds_both_interceptors(
    make_factory: Any, workflow_type: type, task_type: type
) -> None:
    """The docstring's own example: a built-in factory passed as it is."""
    factory = make_factory()
    assert isinstance(factory, InterceptorFactory)

    builder = WorkerBuilder().interceptor(factory)

    assert [type(i) for i in builder._workflow_interceptors] == [workflow_type]
    assert [type(i) for i in builder._task_interceptors] == [task_type]


def test_factory_interceptors_share_its_collector() -> None:
    """Both interceptors made from one factory report to the same place."""
    metrics = MetricsInterceptor()
    builder = WorkerBuilder().interceptor(metrics)

    assert builder._workflow_interceptors[0]._collector is metrics.collector
    assert builder._task_interceptors[0]._collector is metrics.collector


class _Workflow(WorkflowInterceptor):
    pass


class _Task(TaskInterceptor):
    pass


class _Both(WorkflowInterceptor, TaskInterceptor):
    async def intercept_execute(
        self, context: InterceptorContext, input_data: Any, next_fn: NextFn
    ) -> Any:
        return await next_fn(input_data)


def test_interceptor_instances_are_still_routed_by_type() -> None:
    wi, ti, both = _Workflow(), _Task(), _Both()
    builder = WorkerBuilder().interceptor(wi).interceptor(ti).interceptor(both)

    assert builder._workflow_interceptors == [wi, both]
    assert builder._task_interceptors == [ti, both]


@pytest.mark.parametrize("value", [object(), Interceptor(), "logging", None])
def test_anything_else_raises_instead_of_being_dropped(value: Any) -> None:
    builder = WorkerBuilder()
    with pytest.raises(TypeError, match="interceptor\\(\\) expects"):
        builder.interceptor(value)
    assert builder._workflow_interceptors == []
    assert builder._task_interceptors == []


def test_factory_returning_non_interceptors_raises_and_adds_nothing() -> None:
    class _BadFactory:
        def workflow(self) -> object:
            return object()

        def task(self) -> TaskInterceptor:
            return _Task()

    builder = WorkerBuilder()
    with pytest.raises(TypeError, match="_BadFactory.workflow\\(\\) and .task\\(\\)"):
        builder.interceptor(_BadFactory())  # type: ignore[arg-type]
    assert builder._workflow_interceptors == []
    assert builder._task_interceptors == []
