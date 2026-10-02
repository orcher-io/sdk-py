"""Core native bindings module for ORCHER Python SDK.

This module provides the Python interface to the native Rust bindings
via PyO3. It wraps the low-level native module and provides type-safe
access to the ORCHER SDK Core functionality.

The native module is loaded lazily, so a missing build surfaces as a clear
error at first use rather than at import time.
"""

from orcher.core.bridge import (
    from_native_payload,
    from_native_retry_policy,
    to_native_payload,
    to_native_retry_policy,
)
from orcher.core.native import (
    get_core_version,
    get_native_version,
    is_native_available,
    require_native,
)

__all__ = [
    # Native module access
    "get_core_version",
    "get_native_version",
    "is_native_available",
    "require_native",
    # Bridge utilities
    "to_native_payload",
    "from_native_payload",
    "to_native_retry_policy",
    "from_native_retry_policy",
]
