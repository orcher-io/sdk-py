"""OpenTelemetry tracing interceptors for workflow and task executions.

The interceptors create one span per workflow or task execution. OpenTelemetry
is optional: without it installed, they pass executions through untouched.

Example usage:

    from orcher.interceptors.builtin import TracingInterceptor

    # Requires the opentelemetry-api and opentelemetry-sdk packages.
    tracing = TracingInterceptor(service_name="my-service")

    service = (
        Worker.builder()
        .workflow_interceptor(tracing.workflow())
        .task_interceptor(tracing.task())
        .build()
    )
"""

from typing import Any

from orcher.interceptors.base import (
    ExecutionInfo,
    InterceptorContext,
    NextFn,
    TaskInterceptor,
    WorkflowInterceptor,
)

__all__ = [
    "TracingInterceptor",
    "WorkflowTracingInterceptor",
    "TaskTracingInterceptor",
]

# OpenTelemetry is an optional dependency.
OTEL_AVAILABLE = False
_trace_module: Any = None
_SpanKind: Any = None
_Status: Any = None
_StatusCode: Any = None

try:
    from opentelemetry import trace as _trace_mod  # type: ignore[import-not-found]
    from opentelemetry.trace import SpanKind, Status, StatusCode  # type: ignore[import-not-found]

    OTEL_AVAILABLE = True
    _trace_module = _trace_mod
    _SpanKind = SpanKind
    _Status = Status
    _StatusCode = StatusCode
except ImportError:
    pass


class WorkflowTracingInterceptor(WorkflowInterceptor):
    """Interceptor that creates OpenTelemetry spans for workflow executions."""

    def __init__(
        self,
        tracer: Any | None = None,
        service_name: str = "orcher",
        record_input: bool = False,
        record_output: bool = False,
    ) -> None:
        """Initialize the workflow tracing interceptor.

        Args:
            tracer: Optional OpenTelemetry Tracer. Defaults to a tracer from
                the global tracer provider.
            service_name: Service name for the tracer.
            record_input: Whether to record input data as span attributes.
            record_output: Whether to record output data as span attributes.
        """
        self._service_name = service_name
        self._record_input = record_input
        self._record_output = record_output
        self._tracer: Any | None = None

        if OTEL_AVAILABLE and _trace_module is not None:
            if tracer is not None:
                self._tracer = tracer
            else:
                self._tracer = _trace_module.get_tracer(service_name)

    @property
    def name(self) -> str:
        return "WorkflowTracingInterceptor"

    @property
    def order(self) -> int:
        # Tracing should run early to capture full execution
        return 10

    async def intercept_execute(
        self,
        context: InterceptorContext,
        input_data: Any,
        next_fn: NextFn,
    ) -> Any:
        """Intercept workflow execution and create a span."""
        if not OTEL_AVAILABLE or self._tracer is None:
            return await next_fn(input_data)

        span_name = f"workflow.{context.workflow_type}"

        with self._tracer.start_as_current_span(
            span_name,
            kind=_SpanKind.INTERNAL,
        ) as span:
            span.set_attribute("orcher.workflow.id", context.workflow_id)
            span.set_attribute("orcher.workflow.run_id", context.run_id)
            span.set_attribute("orcher.workflow.type", context.workflow_type)

            if self._record_input and input_data is not None:
                span.set_attribute("orcher.workflow.input", str(input_data)[:1000])

            # Lets the on_* hooks add events to this span.
            context.set_attribute("otel.span", span)

            try:
                result = await next_fn(input_data)

                if self._record_output and result is not None:
                    span.set_attribute("orcher.workflow.output", str(result)[:1000])

                span.set_status(_Status(_StatusCode.OK))
                return result

            except Exception as e:
                span.set_status(_Status(_StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise

    async def on_enter(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Add an event when workflow starts."""
        span = context.get_attribute("otel.span")
        if span is not None and OTEL_AVAILABLE:
            span.add_event("workflow.started")

    async def on_success(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Add an event when workflow succeeds."""
        span = context.get_attribute("otel.span")
        if span is not None and OTEL_AVAILABLE:
            span.add_event(
                "workflow.completed",
                attributes={"duration_ms": info.duration_ms or 0},
            )

    async def on_error(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
    ) -> None:
        """Add an event when workflow fails."""
        span = context.get_attribute("otel.span")
        if span is not None and OTEL_AVAILABLE:
            span.add_event(
                "workflow.failed",
                attributes={"error": str(info.error) if info.error else "unknown"},
            )


class TaskTracingInterceptor(TaskInterceptor):
    """Interceptor that creates OpenTelemetry spans for task executions."""

    def __init__(
        self,
        tracer: Any | None = None,
        service_name: str = "orcher",
        record_input: bool = False,
        record_output: bool = False,
    ) -> None:
        """Initialize the task tracing interceptor.

        Args:
            tracer: Optional OpenTelemetry Tracer. Defaults to a tracer from
                the global tracer provider.
            service_name: Service name for the tracer.
            record_input: Whether to record input data as span attributes.
            record_output: Whether to record output data as span attributes.
        """
        self._service_name = service_name
        self._record_input = record_input
        self._record_output = record_output
        self._tracer: Any | None = None

        if OTEL_AVAILABLE and _trace_module is not None:
            if tracer is not None:
                self._tracer = tracer
            else:
                self._tracer = _trace_module.get_tracer(service_name)

    @property
    def name(self) -> str:
        return "TaskTracingInterceptor"

    @property
    def order(self) -> int:
        # Tracing should run early to capture full execution
        return 10

    async def intercept_execute(
        self,
        context: InterceptorContext,
        input_data: Any,
        next_fn: NextFn,
    ) -> Any:
        """Intercept task execution and create a span."""
        if not OTEL_AVAILABLE or self._tracer is None:
            return await next_fn(input_data)

        task_name = context.task_name or "unknown"
        span_name = f"task.{task_name}"

        with self._tracer.start_as_current_span(
            span_name,
            kind=_SpanKind.INTERNAL,
        ) as span:
            span.set_attribute("orcher.task.name", task_name)
            span.set_attribute("orcher.task.id", context.task_id or "")
            span.set_attribute("orcher.workflow.id", context.workflow_id)
            span.set_attribute("orcher.workflow.run_id", context.run_id)
            span.set_attribute("orcher.workflow.type", context.workflow_type)

            if self._record_input and input_data is not None:
                span.set_attribute("orcher.task.input", str(input_data)[:1000])

            # Lets on_retry add events to this span.
            context.set_attribute("otel.task.span", span)

            try:
                result = await next_fn(input_data)

                if self._record_output and result is not None:
                    span.set_attribute("orcher.task.output", str(result)[:1000])

                span.set_status(_Status(_StatusCode.OK))
                return result

            except Exception as e:
                span.set_status(_Status(_StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise

    async def on_retry(
        self,
        context: InterceptorContext,
        info: ExecutionInfo,
        attempt: int,
        max_attempts: int,
    ) -> None:
        """Add an event when task retries."""
        span = context.get_attribute("otel.task.span")
        if span is not None and OTEL_AVAILABLE:
            span.add_event(
                "task.retry",
                attributes={
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "error": str(info.error) if info.error else "unknown",
                },
            )


class TracingInterceptor:
    """Factory for creating workflow and task tracing interceptors.

    Creates matched workflow and task interceptors that share one tracer and
    configuration.

    Example:
        tracing = TracingInterceptor(service_name="my-service")

        service = (
            Worker.builder()
            .workflow_interceptor(tracing.workflow())
            .task_interceptor(tracing.task())
            .build()
        )
    """

    def __init__(
        self,
        service_name: str = "orcher",
        record_input: bool = False,
        record_output: bool = False,
    ) -> None:
        """Initialize the tracing interceptor factory.

        Args:
            service_name: Service name for the tracer.
            record_input: Whether to record input data as span attributes.
            record_output: Whether to record output data as span attributes.
        """
        if not OTEL_AVAILABLE:
            import warnings

            warnings.warn(
                "OpenTelemetry packages not installed. Install with: "
                "pip install opentelemetry-api opentelemetry-sdk",
                ImportWarning,
                stacklevel=2,
            )

        self._service_name = service_name
        self._record_input = record_input
        self._record_output = record_output
        self._tracer: Any | None = None

        if OTEL_AVAILABLE and _trace_module is not None:
            self._tracer = _trace_module.get_tracer(service_name)

    def workflow(self) -> WorkflowTracingInterceptor:
        """Create a workflow tracing interceptor."""
        return WorkflowTracingInterceptor(
            tracer=self._tracer,
            service_name=self._service_name,
            record_input=self._record_input,
            record_output=self._record_output,
        )

    def task(self) -> TaskTracingInterceptor:
        """Create a task tracing interceptor."""
        return TaskTracingInterceptor(
            tracer=self._tracer,
            service_name=self._service_name,
            record_input=self._record_input,
            record_output=self._record_output,
        )

    @staticmethod
    def is_available() -> bool:
        """Check if OpenTelemetry is available."""
        return OTEL_AVAILABLE
