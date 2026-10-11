"""How call arguments travel as one JSON input and become arguments again.

A workflow, query or update receives a single JSON value. The caller's
arguments are packed into it and the worker unpacks it into call arguments:

- no arguments are sent as ``null`` and passed as nothing;
- one argument is sent as itself; a dict is spread as keyword arguments, any
  other value is passed as one positional argument;
- several arguments are sent as ``{"__orcher_args__": [...]}`` and passed one
  to one as positional arguments.

Several arguments cannot be sent as a plain list: one list argument is sent
that way, and the worker could not tell the two apart.
"""

from __future__ import annotations

from typing import Any

#: The key of the object several positional arguments are sent in.
POSITIONAL_ARGS_KEY = "__orcher_args__"


def pack_arguments(args: tuple[Any, ...] | list[Any]) -> Any:
    """Pack call arguments into the single JSON input sent for them."""
    if len(args) == 0:
        return None
    if len(args) == 1:
        return args[0]
    return {POSITIONAL_ARGS_KEY: list(args)}


def unpack_arguments(data: Any) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Turn a received JSON input back into positional and keyword arguments."""
    if isinstance(data, dict):
        if len(data) == 1 and isinstance(data.get(POSITIONAL_ARGS_KEY), list):
            return tuple(data[POSITIONAL_ARGS_KEY]), {}
        return (), data
    if data is None:
        return (), {}
    return (data,), {}
