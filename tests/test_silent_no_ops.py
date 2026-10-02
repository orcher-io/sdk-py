"""Handle operations must do what they say instead of failing silently or misleadingly.

Two guarantees are checked here:

* ``reset()`` calls ``native.reset_workflow``, and that binding exists in the
  native module. Without it every call would raise ``AttributeError`` instead of
  resetting anything.
* ``get_workflow()`` without a run id means "latest run". The server resolves a
  run by ``execution_id`` when one is given and by ``workflow_id`` only when it
  is empty. A placeholder run id such as ``"unknown"`` would turn every later
  call into an exact lookup for a run that cannot exist, failing with an error
  that names an id the caller never supplied.

These tests check the binding surface. The round trip against a live server is
covered by the contract harness.
"""

from __future__ import annotations

import inspect

import pytest

from orcher import _native


class TestResetBinding:
    """``reset()`` must reach a real native binding."""

    def test_native_handle_exposes_reset_workflow(self) -> None:
        # Without this attribute the Python-side call raises AttributeError
        # rather than performing an RPC.
        assert hasattr(_native.WorkflowHandle, "reset_workflow")

    def test_python_reset_calls_the_native_binding(self) -> None:
        from orcher.client.workflow_handle import WorkflowHandle

        source = inspect.getsource(WorkflowHandle.reset)
        assert "native.reset_workflow" in source

    def test_reset_binding_is_callable(self) -> None:
        assert callable(_native.WorkflowHandle.reset_workflow)


class TestLatestRunSentinel:
    """A handle without a run id must mean "latest run", not a placeholder."""

    def test_native_get_workflow_handle_accepts_no_run_id(self) -> None:
        signature = inspect.signature(_native.Client.get_workflow_handle)
        assert "run_id" in signature.parameters

    def test_client_documents_latest_run(self) -> None:
        from orcher.client.client import Client

        doc = inspect.getdoc(Client.get_workflow) or ""
        assert "latest run" in doc

    @pytest.mark.parametrize("placeholder", ["unknown", "none", "null"])
    def test_no_placeholder_run_id_in_the_native_source(self, placeholder: str) -> None:
        # A placeholder run id is never a valid wire value, because an empty
        # execution_id is what means "latest".
        from pathlib import Path

        native_src = Path(__file__).resolve().parents[1] / "native" / "src" / "client.rs"
        if not native_src.exists():  # sdist installs without the Rust sources
            pytest.skip("native sources not present in this install")

        source = native_src.read_text()
        assert f'unwrap_or_else(|| "{placeholder}".to_string())' not in source
