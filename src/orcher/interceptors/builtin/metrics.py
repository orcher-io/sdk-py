"""Metrics interceptor for workflow and task executions."""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from orcher.errors import WorkflowSuspendedError
from orcher.interceptors.base import (
    ExecutionInfo,
    InterceptorContext,
    NextFn,
    TaskInterceptor,
    WorkflowInterceptor,
)


@dataclass
class MetricPoint:
    """A single metric data point."""

    name: str
    value: float
    tags: dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class ExecutionMetrics:
    """Aggregated execution metrics for one workflow type or task name."""

    total_count: int = 0
    success_count: int = 0
    error_count: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float("inf")
    max_duration_ms: float = 0.0

    @property
    def avg_duration_ms(self) -> float:
        """Average duration in milliseconds."""
        if self.total_count == 0:
            return 0.0
        return self.total_duration_ms / self.total_count

    @property
    def success_rate(self) -> float:
        """Success rate as a percentage."""
        if self.total_count == 0:
            return 0.0
        return (self.success_count / self.total_count) * 100

    def record(self, duration_ms: float, success: bool) -> None:
        """Record an execution."""
        self.total_count += 1
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
        self.total_duration_ms += duration_ms
        self.min_duration_ms = min(self.min_duration_ms, duration_ms)
        self.max_duration_ms = max(self.max_duration_ms, duration_ms)


class MetricsCollector(ABC):
    """Abstract interface for collecting metrics."""

    @abstractmethod
    def record_counter(
        self,
        name: str,
        value: float = 1.0,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Record a counter metric."""
        pass

    @abstractmethod
    def record_gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Record a gauge metric."""
        pass

    @abstractmethod
    def record_histogram(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Record a histogram metric."""
        pass


class InMemoryMetricsCollector(MetricsCollector):
    """In-memory metrics collector for testing and development.

    Not thread-safe; use it from a single event loop.
    """

    def __init__(self) -> None:
        self._counters: dict[str, list[MetricPoint]] = {}
        self._gauges: dict[str, list[MetricPoint]] = {}
        self._histograms: dict[str, list[MetricPoint]] = {}
        self._workflow_metrics: dict[str, ExecutionMetrics] = {}
        self._task_metrics: dict[str, ExecutionMetrics] = {}

    def record_counter(
        self,
        name: str,
        value: float = 1.0,
        tags: dict[str, str] | None = None,
    ) -> None:
        if name not in self._counters:
            self._counters[name] = []
        self._counters[name].append(MetricPoint(name, value, tags or {}))

    def record_gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        if name not in self._gauges:
            self._gauges[name] = []
        self._gauges[name].append(MetricPoint(name, value, tags or {}))

    def record_histogram(
        self,
        name: str,
        value: float,
        tags: dict[str, str] | None = None,
    ) -> None:
        if name not in self._histograms:
            self._histograms[name] = []
        self._histograms[name].append(MetricPoint(name, value, tags or {}))

    def get_counter(self, name: str) -> list[MetricPoint]:
        """Get all recorded counter values."""
        return self._counters.get(name, [])

    def get_counter_total(self, name: str) -> float:
        """Get the sum of all counter values."""
        return sum(p.value for p in self._counters.get(name, []))

    def get_gauge(self, name: str) -> list[MetricPoint]:
        """Get all recorded gauge values."""
        return self._gauges.get(name, [])

    def get_gauge_latest(self, name: str) -> float | None:
        """Get the latest gauge value."""
        gauges = self._gauges.get(name, [])
        if not gauges:
            return None
        return gauges[-1].value

    def get_histogram(self, name: str) -> list[MetricPoint]:
        """Get all recorded histogram values."""
        return self._histograms.get(name, [])

    def record_workflow_execution(
        self,
        workflow_type: str,
        duration_ms: float,
        success: bool,
    ) -> None:
        """Record a workflow execution."""
        if workflow_type not in self._workflow_metrics:
            self._workflow_metrics[workflow_type] = ExecutionMetrics()
        self._workflow_metrics[workflow_type].record(duration_ms, success)

    def record_task_execution(
        self,
        task_name: str,
        duration_ms: float,
        success: bool,
    ) -> None:
        """Record a task execution."""
        if task_name not in self._task_metrics:
            self._task_metrics[task_name] = ExecutionMetrics()
        self._task_metrics[task_name].record(duration_ms, success)

    def get_workflow_metrics(self, workflow_type: str) -> ExecutionMetrics | None:
        """Get metrics for a workflow type."""
        return self._workflow_metrics.get(workflow_type)

    def get_task_metrics(self, task_name: str) -> ExecutionMetrics | None:
        """Get metrics for a task."""
        return self._task_metrics.get(task_name)

    def get_all_workflow_metrics(self) -> dict[str, ExecutionMetrics]:
        """Get all workflow metrics."""
        return dict(self._workflow_metrics)

    def get_all_task_metrics(self) -> dict[str, ExecutionMetrics]:
        """Get all task metrics."""
        return dict(self._task_metrics)

    def clear(self) -> None:
        """Clear all metrics."""
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()
        self._workflow_metrics.clear()
        self._task_metrics.clear()


class WorkflowMetricsInterceptor(WorkflowInterceptor):
    """Interceptor that collects metrics for workflow executions."""

    def __init__(self, collector: MetricsCollector) -> None:
        self._collector = collector

    @property
    def order(self) -> int:
        return 10  # System level, runs early

    async def intercept_execute(
        self,
        context: InterceptorContext,
        input_data: Any,
        next_fn: NextFn,
    ) -> Any:
        workflow_type = context.workflow_type
        tags = {
            "workflow_type": workflow_type,
            "workflow_id": context.workflow_id,
        }

        self._collector.record_counter("workflow.started", tags=tags)

        start_time = time.perf_counter()
        try:
            result = await next_fn(input_data)
        except WorkflowSuspendedError:
            # The activation is waiting for work to finish: it neither
            # completed nor failed.
            self._collector.record_counter("workflow.suspended", tags=tags)
            raise
        except BaseException:
            self._record_end(workflow_type, tags, start_time, success=False)
            raise
        self._record_end(workflow_type, tags, start_time, success=True)
        return result

    def _record_end(
        self, workflow_type: str, tags: dict[str, str], start_time: float, *, success: bool
    ) -> None:
        duration_ms = (time.perf_counter() - start_time) * 1000

        if success:
            self._collector.record_counter("workflow.completed", tags=tags)
        else:
            self._collector.record_counter("workflow.failed", tags=tags)

        self._collector.record_histogram(
            "workflow.duration_ms",
            duration_ms,
            tags=tags,
        )

        # Only the in-memory collector keeps per-type aggregates.
        if isinstance(self._collector, InMemoryMetricsCollector):
            self._collector.record_workflow_execution(workflow_type, duration_ms, success)


class TaskMetricsInterceptor(TaskInterceptor):
    """Interceptor that collects metrics for task executions."""

    def __init__(self, collector: MetricsCollector) -> None:
        self._collector = collector

    @property
    def order(self) -> int:
        return 10  # System level, runs early

    async def intercept_execute(
        self,
        context: InterceptorContext,
        input_data: Any,
        next_fn: NextFn,
    ) -> Any:
        task_name = context.task_name or "unknown"
        tags = {
            "task_name": task_name,
            "workflow_id": context.workflow_id,
        }

        self._collector.record_counter("task.started", tags=tags)

        start_time = time.perf_counter()
        success = False
        try:
            result = await next_fn(input_data)
            success = True
            return result
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000

            if success:
                self._collector.record_counter("task.completed", tags=tags)
            else:
                self._collector.record_counter("task.failed", tags=tags)

            self._collector.record_histogram(
                "task.duration_ms",
                duration_ms,
                tags=tags,
            )

            # Only the in-memory collector keeps per-type aggregates.
            if isinstance(self._collector, InMemoryMetricsCollector):
                self._collector.record_task_execution(task_name, duration_ms, success)

    async def on_retry(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
        attempt: int,
        max_attempts: int,
    ) -> None:
        """Record retry metrics."""
        task_name = context.task_name or "unknown"
        tags = {
            "task_name": task_name,
            "attempt": str(attempt),
        }
        self._collector.record_counter("task.retry", tags=tags)


class MetricsInterceptor:
    """Factory for creating workflow and task metrics interceptors.

    Example usage:
        collector = InMemoryMetricsCollector()
        metrics = MetricsInterceptor(collector)
        service = (
            Worker.builder()
            .workflow_interceptor(metrics.workflow())
            .task_interceptor(metrics.task())
            .build()
        )

        # Later, query metrics:
        workflow_metrics = collector.get_workflow_metrics("MyWorkflow")
        print(f"Success rate: {workflow_metrics.success_rate}%")
    """

    def __init__(self, collector: MetricsCollector | None = None) -> None:
        self._collector = collector or InMemoryMetricsCollector()

    @property
    def collector(self) -> MetricsCollector:
        """Get the underlying metrics collector."""
        return self._collector

    def workflow(self) -> WorkflowMetricsInterceptor:
        """Create a workflow metrics interceptor."""
        return WorkflowMetricsInterceptor(self._collector)

    def task(self) -> TaskMetricsInterceptor:
        """Create a task metrics interceptor."""
        return TaskMetricsInterceptor(self._collector)
