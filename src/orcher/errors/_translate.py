"""Turn native exceptions into the errors the public API documents.

Every method on `WorkflowHandle` documents that it raises `WorkflowError` or
`ClientError`. The native layer raises its own exception types and some
builtins, so this module maps them to the documented errors, built with the
matching constructors (`WorkflowError.not_found`, `.cancelled`, `.terminated`,
and so on).

The mapping keys off the **exception type**, never the message. The native
layer raises a distinct subclass for each kind of failure, so no message text
has to be parsed, and rewording a message cannot break the mapping.
"""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from orcher.errors.client import ClientError
from orcher.errors.workflow import WorkflowError

T = TypeVar("T")


def _native_types() -> dict[str, Any]:
    """Look up the native exception types, tolerating an older native build.

    The Python package and the compiled extension are versioned separately, and
    an older extension may define fewer of these types. Missing types are
    skipped instead of raising at import time, so a version mismatch does not
    make the whole package unimportable.
    """
    try:
        from orcher import _native
    except Exception:  # pragma: no cover - the package cannot work without it
        return {}

    names = (
        "WorkflowNotFoundError",
        "WorkflowAlreadyExistsError",
        "WorkflowFailedError",
        "WorkflowCancelledError",
        "WorkflowTerminatedError",
        "TaskFailedError",
        "TaskCancelledError",
        "DeterminismError",
        "NativeError",
    )
    return {name: getattr(_native, name) for name in names if hasattr(_native, name)}


def translate(exc: BaseException, *, workflow_id: str = "unknown") -> BaseException:
    """Map a native exception to the documented one, or return it unchanged.

    Anything unrecognized is returned as-is. Wrapping an unknown exception
    would hide it, and losing the original is worse than surfacing an
    unexpected type.
    """
    native = _native_types()
    message = str(exc)

    def is_a(name: str) -> bool:
        cls = native.get(name)
        return cls is not None and isinstance(exc, cls)

    if is_a("WorkflowNotFoundError"):
        return WorkflowError.not_found(workflow_id)
    if is_a("WorkflowAlreadyExistsError"):
        # When the server reports it, the native error names the run that
        # holds the id.
        return WorkflowError.already_exists(
            getattr(exc, "workflow_id", None) or workflow_id,
            run_id=getattr(exc, "run_id", None),
        )
    if is_a("WorkflowCancelledError"):
        return WorkflowError.cancelled(workflow_id)
    if is_a("WorkflowTerminatedError"):
        return WorkflowError.terminated(workflow_id, message)
    if is_a("WorkflowFailedError"):
        return WorkflowError.execution_failed(workflow_id, message)
    if is_a("TaskFailedError") or is_a("TaskCancelledError") or is_a("DeterminismError"):
        return WorkflowError.execution_failed(workflow_id, message)

    # Transport-shaped failures the native layer raises as builtins.
    if isinstance(exc, ConnectionError):
        return ClientError.connection_failed("unknown", message)
    if isinstance(exc, TimeoutError):
        return ClientError.timeout(message)

    # Any remaining NativeError is a communication-level failure from the
    # caller's point of view; the specific kinds were handled above.
    #
    # A gRPC NOT_FOUND from the server also lands here. The native layer
    # reports gRPC statuses generically, not as WorkflowNotFoundError, so
    # "no such workflow" surfaces as a request failure. That is accurate,
    # whereas guessing a workflow error from a status code would not be.
    if is_a("NativeError"):
        return ClientError.invalid_request(message)

    return exc


def translates_native_errors(
    method: Callable[..., Awaitable[T]],
) -> Callable[..., Awaitable[T]]:
    """Wrap a `WorkflowHandle` coroutine so it raises what its docstring says.

    Applied per method, not as a broad catch at the client, so the workflow id
    is available to the documented constructors and the traceback still points
    at the call the user made.
    """

    @functools.wraps(method)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> T:
        try:
            return await method(self, *args, **kwargs)
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            translated = translate(exc, workflow_id=getattr(self, "workflow_id", "unknown"))
            if translated is exc:
                raise
            raise translated from exc

    return wrapper
