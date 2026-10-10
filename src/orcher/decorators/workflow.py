"""The ``@workflow`` decorator for defining workflow functions and classes."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, Protocol, TypeVar, cast, overload

from orcher.decorators.registry import GlobalRegistry, HandlerType, WorkflowMetadata

__all__ = ["workflow"]

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")


class _WorkflowDecorator(Protocol):
    """What ``workflow(...)`` returns: it gives back the class or function it decorates."""

    @overload
    def __call__(self, fn: type[T], /) -> type[T]: ...

    @overload
    def __call__(self, fn: F, /) -> F: ...


def workflow(
    *,
    name: str,
    version: str = "1.0",
    description: str = "",
    cron_schedule: str | None = None,
) -> _WorkflowDecorator:
    """Define a workflow function or class.

    Apply it to either:

    - a function that takes ``WorkflowContext`` as its first argument, or
    - a class whose ``run`` method takes ``WorkflowContext`` after ``self``.

    Workflows must be deterministic, because they are replayed from the
    execution journal and each replay must make the same decisions. A
    workflow must not:

    - call ``random.random()`` (use ``ctx.random``),
    - call ``datetime.now()`` (use ``ctx.time.now()``),
    - call external services directly (use tasks), or
    - depend on global mutable state.

    Args:
        name: Unique name for the workflow. Used for identification.
        version: Version string for the workflow. Defaults to "1.0".
        description: Optional description of the workflow.
        cron_schedule: Optional cron expression for periodic execution
            (e.g., "0 9 * * *" for daily at 9am).

    Returns:
        A decorator that registers the workflow function or class.

    Raises:
        ValueError: If the function/class signature is invalid.
        ValueError: If the workflow name is already registered.

    Example (function-based):
        >>> from orcher import workflow, WorkflowContext
        >>>
        >>> @workflow(name="order-processing", version="1.0")
        ... async def process_order(ctx: WorkflowContext, order_id: str) -> dict:
        ...     result = await ctx.execute_task(process_payment, order_id=order_id)
        ...     return {"status": "completed", "result": result}

    Example (class-based):
        >>> from orcher import workflow, WorkflowContext
        >>>
        >>> @workflow(name="order-processing", version="1.0")
        ... class OrderWorkflow:
        ...     async def run(self, ctx: WorkflowContext, order_id: str) -> dict:
        ...         result = await ctx.execute_task(process_payment, order_id=order_id)
        ...         return {"status": "completed", "result": result}
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        if not callable(fn):
            raise ValueError(
                f"@workflow decorator must be applied to a function or class, got {type(fn)}"
            )

        is_class = inspect.isclass(fn)

        if is_class:
            cls = cast(type[Any], fn)

            if not hasattr(cls, "run"):
                raise ValueError(
                    f"Workflow class '{cls.__name__}' must have a 'run' method. "
                    f"Define: async def run(self, ctx: WorkflowContext, ...) -> ..."
                )

            run_method = cls.run
            if not callable(run_method):
                raise ValueError(f"Workflow class '{cls.__name__}' run attribute must be a method.")

            # `run` is read from the class, so it is unbound: (self, ctx, ...).
            sig = inspect.signature(run_method)
            params = list(sig.parameters.keys())

            if len(params) < 2:
                raise ValueError(
                    f"Workflow class '{cls.__name__}.run' must accept at least "
                    f"(self, ctx: WorkflowContext). Found parameters: {params}"
                )

            metadata = WorkflowMetadata(
                name=name,
                version=version,
                handler=cls,
                handler_type=HandlerType.CLASS,
                description=description,
                cron_schedule=cron_schedule,
                run_method=run_method,
            )

            registry = GlobalRegistry.get_instance()
            registry.register_workflow(metadata)

            # Attach metadata to the class for introspection
            cls.__orcher_workflow__ = metadata
            cls.__orcher_workflow_name__ = name
            cls.__orcher_workflow_version__ = version
            cls.__orcher_cron_schedule__ = cron_schedule

            return cls

        else:
            sig = inspect.signature(fn)
            params = list(sig.parameters.keys())

            if len(params) < 1:
                raise ValueError(
                    f"Workflow function '{fn.__name__}' must accept at least "
                    f"(ctx: WorkflowContext). Found parameters: {params}"
                )

            metadata = WorkflowMetadata(
                name=name,
                version=version,
                handler=fn,
                handler_type=HandlerType.FUNCTION,
                description=description,
                cron_schedule=cron_schedule,
            )

            registry = GlobalRegistry.get_instance()
            registry.register_workflow(metadata)

            # Attach metadata to the function for introspection
            fn.__orcher_workflow__ = metadata  # type: ignore
            fn.__orcher_workflow_name__ = name  # type: ignore
            fn.__orcher_workflow_version__ = version  # type: ignore
            fn.__orcher_cron_schedule__ = cron_schedule  # type: ignore

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return fn(*args, **kwargs)

            # The wrapper is what callers hold, so it carries the same metadata.
            wrapper.__orcher_workflow__ = metadata  # type: ignore
            wrapper.__orcher_workflow_name__ = name  # type: ignore
            wrapper.__orcher_workflow_version__ = version  # type: ignore
            wrapper.__orcher_cron_schedule__ = cron_schedule  # type: ignore

            return wrapper

    return cast(_WorkflowDecorator, decorator)
