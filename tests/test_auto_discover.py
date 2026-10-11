"""auto_discover imports each module once, by its real name, and reports failures.

A module the program also imports by name must be the same module, so its
decorators run once; and a file that cannot be imported, a duplicate
registration among them, must stop the build rather than leave the worker
silently without the workflows it holds.
"""

from __future__ import annotations

import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from textwrap import dedent

import pytest

from orcher import Worker
from orcher.decorators import GlobalRegistry


@pytest.fixture
def root(tmp_path: Path) -> Iterator[Path]:
    """A scan root; whatever the test imports from it is forgotten afterwards."""
    path_before = list(sys.path)
    modules_before = set(sys.modules)
    GlobalRegistry.reset()
    yield tmp_path
    sys.path[:] = path_before
    for name in set(sys.modules) - modules_before:
        del sys.modules[name]
    GlobalRegistry.reset()


def write(path: Path, source: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(source))
    return path


def workflow_module(name: str, log: Path) -> str:
    """A module that registers workflow ``name`` and logs each time it runs."""
    return f"""
        from pathlib import Path
        from orcher import WorkflowContext, workflow

        with Path({str(log)!r}).open("a") as log:
            log.write(__name__ + "\\n")

        @workflow(name={name!r})
        async def run(ctx: WorkflowContext) -> None:
            return None
    """


def build(*paths: Path) -> Worker:
    return (
        Worker.builder()
        .server_url("http://localhost:50051")
        .task_queue("q")
        .auto_discover([str(p) for p in paths])
        .build()
    )


def package_name() -> str:
    return f"discovered_{uuid.uuid4().hex[:8]}"


def test_a_module_is_imported_under_its_real_name(root: Path) -> None:
    pkg = package_name()
    log = root / "ran.log"
    write(root / pkg / "__init__.py", "")
    write(root / pkg / "flows.py", workflow_module("discover-real-name", log))

    worker = build(root)

    assert log.read_text().splitlines() == [f"{pkg}.flows"]
    assert sys.modules[f"{pkg}.flows"].__file__ == str(root / pkg / "flows.py")
    assert "discover-real-name" in worker._workflow_handlers


def test_a_module_already_imported_is_not_imported_again(root: Path) -> None:
    pkg = package_name()
    log = root / "ran.log"
    write(root / pkg / "__init__.py", "")
    write(root / pkg / "flows.py", workflow_module("discover-once", log))
    sys.path.append(str(root))
    __import__(f"{pkg}.flows")

    # Scanning the package itself, as well as its parent, finds the same file.
    worker = build(root, root / pkg)

    assert log.read_text().splitlines() == [f"{pkg}.flows"]
    assert "discover-once" in worker._workflow_handlers


def test_same_named_files_in_different_directories_are_both_imported(root: Path) -> None:
    a, b = package_name(), package_name()
    log = root / "ran.log"
    write(root / a / "tasks.py", workflow_module("discover-a", log))
    write(root / b / "tasks.py", workflow_module("discover-b", log))

    worker = build(root)

    assert sorted(log.read_text().splitlines()) == sorted([f"{a}.tasks", f"{b}.tasks"])
    assert {"discover-a", "discover-b"} <= set(worker._workflow_handlers)


def test_a_module_that_fails_to_import_fails_the_build(root: Path) -> None:
    pkg = package_name()
    log = root / "ran.log"
    write(root / pkg / "__init__.py", "")
    write(root / pkg / "good.py", workflow_module("discover-good", log))
    broken = write(root / pkg / "broken.py", "import a_module_that_does_not_exist\n")

    with pytest.raises(ImportError) as raised:
        build(root)

    message = str(raised.value)
    assert str(broken) in message
    assert "a_module_that_does_not_exist" in message
    assert isinstance(raised.value.__cause__, ModuleNotFoundError)
    # The others are still imported, so every failure is reported in one go.
    assert log.read_text().splitlines() == [f"{pkg}.good"]


def test_a_duplicate_registration_fails_the_build(root: Path) -> None:
    pkg = package_name()
    log = root / "ran.log"
    write(root / pkg / "__init__.py", "")
    write(root / pkg / "first.py", workflow_module("discover-twice", log))
    second = write(root / pkg / "second.py", workflow_module("discover-twice", log))

    with pytest.raises(ImportError) as raised:
        build(root)

    assert str(second) in str(raised.value)
    assert "already registered" in str(raised.value)
    assert isinstance(raised.value.__cause__, ValueError)
