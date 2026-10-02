"""The ``@task`` decorator for defining task functions and methods."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar, get_type_hints

from orcher.decorators.registry import GlobalRegistry, HandlerType, TaskMetadata

if TYPE_CHECKING:
    from orcher.types import RetryPolicy

__all__ = ["task"]

F = TypeVar("F", bound=Callable[..., Any])


def task(
    *,
    name: str,
    retry_policy: RetryPolicy | dict[str, Any] | None = None,
    retry: int | None = None,
    timeout: float | None = None,
    heartbeat_timeout: float | None = None,
) -> Callable[[F], F]:
    """Define a task function or task method.

    Tasks are units of work that may have side effects, such as I/O or calls
    to external services. A Service executes them and records each result in
    the execution journal, so a replayed workflow reuses the result instead of
    running the task again.

    The decorated function should:
    - Accept TaskContext as the first argument
    - Be async (recommended) or sync
    - Return a serializable result

    Args:
        name: Unique name for the task. Used for identification and routing.
        retry_policy: Optional retry policy. Prefer a ``RetryPolicy`` object,
            which uses ``timedelta`` durations. A plain ``dict`` is also
            accepted, with these optional keys:
            - max_attempts: Maximum retry attempts (default 3)
            - initial_interval_seconds: Initial retry interval, seconds (default 1)
            - max_interval_seconds: Maximum retry interval, seconds (default 60)
            - backoff_coefficient: Multiplier for exponential backoff (default 2.0)
            - non_retryable_errors: List of error-type names that never retry

            The engine honors whole-second retry intervals and rounds
            sub-second values down. Use ``RetryPolicy`` for type-safe durations.
        retry: Shorthand for ``retry_policy={"max_attempts": retry}``. Ignored
            when ``retry_policy`` is given.
        timeout: Optional task execution timeout in seconds.
        heartbeat_timeout: Optional heartbeat timeout in seconds.

    Returns:
        A decorator that registers the task function.

    Raises:
        ValueError: If the function signature is invalid.
        ValueError: If the task name is already registered.

    Example:
        >>> from orcher import task, TaskContext
        >>>
        >>> @task(name="charge-card", timeout=30.0)
        ... async def charge_card(ctx: TaskContext, amount: int, token: str) -> dict:
        ...     # Can make external API calls, database writes, etc.
        ...     result = await payment_gateway.charge(amount, token)
        ...     return {"charge_id": result.id, "status": "succeeded"}
        >>>
        >>> @task(name="send-receipt")
        ... async def send_receipt(ctx: TaskContext, email: str, charge_id: str) -> bool:
        ...     await email_service.send(email, f"Receipt for {charge_id}")
        ...     return True
        >>>
        >>> # In a workflow, execute tasks:
        >>> @workflow(name="payment-flow", version="1.0")
        ... async def payment_flow(ctx: WorkflowContext, amount: int, token: str, email: str):
        ...     charge = await ctx.execute_task(charge_card, amount=amount, token=token)
        ...     await ctx.execute_task(send_receipt, email=email, charge_id=charge["charge_id"])
        ...     return charge
    """

    # `retry=N` means retry_policy={"max_attempts": N}, matching the Rust SDK's
    # #[task(retry = N)]. An explicit retry_policy takes precedence.
    if retry is not None and retry_policy is None:
        retry_policy = {"max_attempts": retry}

    def decorator(fn: F) -> F:
        if not callable(fn):
            raise ValueError(f"@task decorator must be applied to a function, got {type(fn)}")

        sig = inspect.signature(fn)
        params = list(sig.parameters.keys())

        # A first parameter named 'self' marks a method on a @tasks class.
        is_method = len(params) > 0 and params[0] == "self"

        if is_method:
            if len(params) < 2:
                raise ValueError(
                    f"Task method '{fn.__name__}' must accept at least (self, ctx: TaskContext). "
                    f"Found parameters: {params}"
                )
        else:
            if len(params) < 1:
                raise ValueError(
                    f"Task function '{fn.__name__}' must accept at least (ctx: TaskContext). "
                    f"Found parameters: {params}"
                )

        # The return type hint lets a recorded result (a plain dict) be
        # restored to its dataclass on replay.
        return_type = None
        try:
            type_hints = get_type_hints(fn)
            return_type = type_hints.get("return")
        except Exception:
            # Hints can fail to resolve (forward refs, missing imports). Results
            # are then returned as recorded, without coercion.
            pass

        metadata = TaskMetadata(
            name=name,
            handler=fn,
            handler_type=HandlerType.FUNCTION if not is_method else HandlerType.CLASS,
            retry_policy=retry_policy,
            timeout=timeout,
            heartbeat_timeout=heartbeat_timeout,
            return_type=return_type,
        )

        # Methods are registered by the enclosing @tasks class decorator, which
        # knows the owning class.
        if not is_method:
            registry = GlobalRegistry.get_instance()
            registry.register_task(metadata)

        # Attach metadata to the function for introspection
        fn.__orcher_task__ = metadata  # type: ignore
        fn.__orcher_task_name__ = name  # type: ignore
        fn.__orcher_is_task__ = True  # type: ignore
        fn.__orcher_task_timeout__ = timeout  # type: ignore
        fn.__orcher_task_heartbeat_timeout__ = heartbeat_timeout  # type: ignore
        fn.__orcher_task_retry_policy__ = retry_policy  # type: ignore

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)

        # The wrapper is what callers hold, so it carries the same metadata.
        wrapper.__orcher_task__ = metadata  # type: ignore
        wrapper.__orcher_task_name__ = name  # type: ignore
        wrapper.__orcher_is_task__ = True  # type: ignore
        wrapper.__orcher_task_timeout__ = timeout  # type: ignore
        wrapper.__orcher_task_heartbeat_timeout__ = heartbeat_timeout  # type: ignore
        wrapper.__orcher_task_retry_policy__ = retry_policy  # type: ignore

        return wrapper  # type: ignore

    return decorator
