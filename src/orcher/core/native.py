"""PyO3 native module loader for ORCHER Python SDK.

This module handles loading the native Rust bindings and provides
helpful error messages if the native module is not available.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "get_native_module",
    "get_core_version",
    "get_native_version",
    "is_native_available",
    "require_native",
    "NativeError",
]

# Cached native module reference
_native_module: Any | None = None
_native_error: Exception | None = None


class NativeError(Exception):
    """Error raised when native module is not available."""

    pass


def _load_native() -> None:
    """Attempt to load the native module."""
    global _native_module, _native_error

    if _native_module is not None or _native_error is not None:
        return  # Already attempted

    try:
        from orcher import _native

        _native_module = _native
    except ImportError as e:
        _native_error = e


def get_native_module() -> Any:
    """Get the native module, raising an error if not available.

    Returns:
        The native module.

    Raises:
        NativeError: If the native module is not available.
    """
    _load_native()

    if _native_module is None:
        raise NativeError(
            "ORCHER native module not found. "
            "Please install the package with: pip install orcher-sdk\n"
            "Or build from source with: maturin develop\n"
            f"Original error: {_native_error}"
        )

    return _native_module


def is_native_available() -> bool:
    """Check if the native module is available.

    Returns:
        True if the native module can be loaded.
    """
    _load_native()
    return _native_module is not None


def require_native() -> None:
    """Require the native module, raising an error if not available.

    This is a convenience function for code that requires native bindings.

    Raises:
        NativeError: If the native module is not available.
    """
    get_native_module()


def get_core_version() -> str:
    """Get the version of the ORCHER SDK Core.

    Returns:
        The core version string.

    Raises:
        NativeError: If the native module is not available.
    """
    native = get_native_module()
    return native._get_core_version()


def get_native_version() -> str:
    """Get the version of the native Python bindings.

    Returns:
        The native module version string.

    Raises:
        NativeError: If the native module is not available.
    """
    native = get_native_module()
    return native._get_native_version()


def health_check() -> bool:
    """Perform a health check on the native module.

    Returns:
        True if the native module is healthy.

    Raises:
        NativeError: If the native module is not available.
    """
    native = get_native_module()
    return native._health_check()
