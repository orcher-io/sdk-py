"""Smoke tests to verify the ORCHER SDK loads correctly."""

import pytest


class TestSmokeTests:
    """Basic smoke tests to verify the SDK is working."""

    def test_import_orcher(self) -> None:
        """Test that the orcher package can be imported."""
        import orcher

        assert orcher.__version__ is not None
        assert isinstance(orcher.__version__, str)

    def test_native_module_loads(self) -> None:
        """Test that the native module loads and provides core version."""
        import orcher

        # The native version is only present when the native module is built.
        if orcher.__native_version__ is not None:
            assert isinstance(orcher.__native_version__, str)
            assert len(orcher.__native_version__) > 0

    def test_native_functions(self) -> None:
        """Test native module functions."""
        # Skip only when the module itself is absent. A missing function must
        # fail the test, not skip it, so the functions are not imported by name
        # inside a `try`.
        native = pytest.importorskip(
            "orcher._native", reason="Native module not built - run 'maturin develop' first"
        )

        assert native._get_native_version() == native.__version__
        assert native._get_core_version() == native.__core_version__
        assert len(native._get_core_version()) > 0
        assert native._health_check() is True

    def test_native_module_metadata(self) -> None:
        """Test native module metadata attributes."""
        try:
            from orcher import _native

            assert hasattr(_native, "__version__")
            assert hasattr(_native, "__core_version__")

        except ImportError:
            pytest.skip("Native module not built - run 'maturin develop' first")


class TestVersionConsistency:
    """Test version consistency across the SDK."""

    def test_python_version_format(self) -> None:
        """Test that Python version follows semver format."""
        import orcher

        version = orcher.__version__
        parts = version.split(".")
        assert len(parts) >= 2, "Version should have at least major.minor"

        # Major and minor should be numeric
        assert parts[0].isdigit(), "Major version should be numeric"
        assert parts[1].isdigit(), "Minor version should be numeric"
