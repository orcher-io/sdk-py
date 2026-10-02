"""Error context and chaining utilities.

Attach diagnostic context to errors, and collect several errors into a chain
that keeps each one with its context.
"""

from __future__ import annotations

import sys
import traceback
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar

__all__ = [
    "ErrorContext",
    "ErrorChain",
    "with_context",
    "capture_exception",
    "format_error_chain",
]

E = TypeVar("E", bound=Exception)


@dataclass
class ErrorContext:
    """Context information for an error.

    Attributes:
        message: Additional context message.
        operation: Name of the operation being performed.
        component: Component/module where the error occurred.
        workflow_id: Associated workflow ID (if applicable).
        task_id: Associated task ID (if applicable).
        timestamp: When the error occurred.
        metadata: Additional metadata key-value pairs.
        stack_trace: Captured stack trace.
    """

    message: str = ""
    operation: str | None = None
    component: str | None = None
    workflow_id: str | None = None
    task_id: str | None = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)
    stack_trace: str | None = None

    def with_operation(self, operation: str) -> ErrorContext:
        """Add operation name to context."""
        self.operation = operation
        return self

    def with_component(self, component: str) -> ErrorContext:
        """Add component name to context."""
        self.component = component
        return self

    def with_workflow(self, workflow_id: str) -> ErrorContext:
        """Add workflow ID to context."""
        self.workflow_id = workflow_id
        return self

    def with_task(self, task_id: str) -> ErrorContext:
        """Add task ID to context."""
        self.task_id = task_id
        return self

    def with_metadata(self, key: str, value: Any) -> ErrorContext:
        """Add metadata to context."""
        self.metadata[key] = value
        return self

    def capture_stack(self) -> ErrorContext:
        """Capture current stack trace."""
        self.stack_trace = "".join(traceback.format_stack()[:-1])
        return self

    def format(self) -> str:
        """Format the context as a string."""
        parts = []

        if self.message:
            parts.append(self.message)

        if self.operation:
            parts.append(f"operation={self.operation}")

        if self.component:
            parts.append(f"component={self.component}")

        if self.workflow_id:
            parts.append(f"workflow_id={self.workflow_id}")

        if self.task_id:
            parts.append(f"task_id={self.task_id}")

        if self.metadata:
            meta_str = ", ".join(f"{k}={v!r}" for k, v in self.metadata.items())
            parts.append(f"metadata={{{meta_str}}}")

        return " | ".join(parts) if parts else ""


class ContextualError(Exception):
    """An error with attached context information.

    Wraps any exception with context that helps diagnose it.
    """

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        context: ErrorContext | None = None,
    ) -> None:
        self.original_message = message
        self.cause = cause
        self.context = context or ErrorContext()
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        parts = [self.original_message]

        context_str = self.context.format()
        if context_str:
            parts.append(f"[{context_str}]")

        if self.cause:
            parts.append(f"caused by: {self.cause}")

        return " ".join(parts)

    def with_context(self, context: ErrorContext) -> ContextualError:
        """Add or replace context."""
        self.context = context
        return self

    def add_metadata(self, key: str, value: Any) -> ContextualError:
        """Add metadata to the context."""
        self.context.with_metadata(key, value)
        return self


class ErrorChain:
    """A chain of errors preserving the full error history.

    Collects every error that occurred, each with its context, so all of them
    can be reported together.

    Example:
        >>> chain = ErrorChain()
        >>> try:
        ...     do_something()
        ... except Exception as e:
        ...     chain.add(e, context="During initialization")
        ...
        >>> try:
        ...     do_something_else()
        ... except Exception as e:
        ...     chain.add(e, context="During processing")
        ...     chain.raise_if_any()
    """

    def __init__(self) -> None:
        self._errors: list[tuple[Exception, ErrorContext]] = []

    def add(
        self,
        error: Exception,
        context: str | None = None,
        **metadata: Any,
    ) -> ErrorChain:
        """Add an error to the chain.

        Args:
            error: The exception to add.
            context: Optional context message.
            **metadata: Additional metadata to attach.

        Returns:
            Self for chaining.
        """
        ctx = ErrorContext(message=context or "")
        ctx.capture_stack()
        for key, value in metadata.items():
            ctx.with_metadata(key, value)

        self._errors.append((error, ctx))
        return self

    def add_with_context(
        self,
        error: Exception,
        context: ErrorContext,
    ) -> ErrorChain:
        """Add an error with a full ErrorContext object."""
        self._errors.append((error, context))
        return self

    @property
    def is_empty(self) -> bool:
        """Check if the chain has no errors."""
        return len(self._errors) == 0

    @property
    def count(self) -> int:
        """Get the number of errors in the chain."""
        return len(self._errors)

    @property
    def errors(self) -> list[Exception]:
        """Get all errors in the chain."""
        return [e for e, _ in self._errors]

    @property
    def first(self) -> Exception | None:
        """Get the first error in the chain."""
        return self._errors[0][0] if self._errors else None

    @property
    def last(self) -> Exception | None:
        """Get the last error in the chain."""
        return self._errors[-1][0] if self._errors else None

    def raise_if_any(
        self,
        message: str = "Multiple errors occurred",
        error_class: type[Exception] = Exception,
    ) -> None:
        """Raise an exception if there are any errors in the chain.

        Args:
            message: The message for the combined exception.
            error_class: The exception class to raise.

        Raises:
            The specified exception class if there are errors.
        """
        if self._errors:
            raise error_class(format_error_chain(self._errors, message))

    def format(self, include_stack: bool = False) -> str:
        """Format the error chain as a string.

        Args:
            include_stack: Whether to include stack traces.

        Returns:
            Formatted string representation of the error chain.
        """
        return format_error_chain(self._errors, include_stack=include_stack)

    def __len__(self) -> int:
        return len(self._errors)

    def __bool__(self) -> bool:
        return len(self._errors) > 0

    def __iter__(self) -> Iterator[tuple[Exception, ErrorContext]]:
        return iter(self._errors)


def with_context(
    message: str,
    *,
    operation: str | None = None,
    component: str | None = None,
    workflow_id: str | None = None,
    task_id: str | None = None,
    **metadata: Any,
) -> ErrorContext:
    """Create an ErrorContext with the given parameters.

    Convenience constructor; extra keyword arguments become metadata.

    Example:
        >>> ctx = with_context(
        ...     "Failed to process order",
        ...     operation="process_order",
        ...     workflow_id="order-123",
        ...     order_id=456,
        ... )
    """
    ctx = ErrorContext(
        message=message,
        operation=operation,
        component=component,
        workflow_id=workflow_id,
        task_id=task_id,
        metadata=metadata,
    )
    return ctx


def capture_exception(
    context: str | None = None,
    **metadata: Any,
) -> tuple[BaseException | None, ErrorContext | None]:
    """Capture the current exception with context.

    Call it inside an ``except`` block. Returns ``(None, None)`` when no
    exception is being handled.

    Example:
        >>> try:
        ...     risky_operation()
        ... except:
        ...     exc, ctx = capture_exception("During risky operation")
        ...     # Handle or re-raise with context
    """
    exc_info = sys.exc_info()
    if exc_info[1] is None:
        return None, None

    exception = exc_info[1]
    ctx = ErrorContext(
        message=context or "",
        stack_trace="".join(traceback.format_exception(*exc_info)),
        metadata=metadata,
    )

    return exception, ctx


def format_error_chain(
    errors: list[tuple[Exception, ErrorContext]],
    header: str = "Error chain",
    include_stack: bool = False,
) -> str:
    """Format a list of errors with their contexts.

    Args:
        errors: List of (exception, context) tuples.
        header: Header for the error chain.
        include_stack: Whether to include stack traces.

    Returns:
        Formatted string representation.
    """
    if not errors:
        return "No errors"

    lines = [f"{header} ({len(errors)} error{'s' if len(errors) > 1 else ''}):"]

    for i, (error, context) in enumerate(errors, 1):
        lines.append(f"\n  [{i}] {type(error).__name__}: {error}")

        context_str = context.format()
        if context_str:
            lines.append(f"      Context: {context_str}")

        if include_stack and context.stack_trace:
            lines.append("      Stack trace:")
            for line in context.stack_trace.strip().split("\n"):
                lines.append(f"        {line}")

    return "\n".join(lines)
