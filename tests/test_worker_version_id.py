"""The release a worker declares, and how it is normalized.

`version_id` is opaque to the server, which compares it for equality and binds
an execution to it on first claim. The behavior worth pinning is the empty
case: the wire type is a bare string, so a blank value must mean "not declared"
rather than a release literally named "".
"""

import os
from unittest import mock

from orcher.worker.config import WorkerConfig


def test_version_id_defaults_to_undeclared() -> None:
    config = WorkerConfig(server_url="http://localhost:50051", task_queue="q")
    assert config.version_id is None


def test_version_id_is_read_from_the_environment() -> None:
    with mock.patch.dict(os.environ, {"ORCHER_SERVER_URL": "http://localhost:50051",
         "ORCHER_TASK_QUEUE": "q", "ORCHER_VERSION_ID": "sha-deadbeef"}, clear=False):
        config = WorkerConfig.from_env()
    assert config.version_id == "sha-deadbeef"


def test_a_blank_environment_value_is_not_a_release() -> None:
    # An exported-but-empty variable is the realistic failure: it would
    # otherwise bind every execution to a release named "" and look correct.
    for blank in ("", "   ", "\t"):
        with mock.patch.dict(os.environ, {"ORCHER_SERVER_URL": "http://localhost:50051",
         "ORCHER_TASK_QUEUE": "q", "ORCHER_VERSION_ID": blank}, clear=False):
            config = WorkerConfig.from_env()
        assert config.version_id is None, f"{blank!r} was treated as a declared release"


def test_an_unset_environment_leaves_it_undeclared() -> None:
    with mock.patch.dict(
        os.environ,
        {"ORCHER_SERVER_URL": "http://localhost:50051", "ORCHER_TASK_QUEUE": "q"},
        clear=True,
    ):
        config = WorkerConfig.from_env()
    assert config.version_id is None


def test_the_native_config_carries_the_release() -> None:
    # The Python value is only useful if it survives into the native config the
    # pollers are built from.
    from orcher._native import WorkerConfig as NativeWorkerConfig

    native = NativeWorkerConfig(
        server_url="http://localhost:50051", task_queue="q", version_id="release-7"
    )
    assert native.version_id == "release-7"


def test_the_native_config_normalises_blank_too() -> None:
    from orcher._native import WorkerConfig as NativeWorkerConfig

    native = NativeWorkerConfig(
        server_url="http://localhost:50051", task_queue="q", version_id="  "
    )
    assert native.version_id is None


def test_the_builder_also_defaults_from_the_environment() -> None:
    # Parity: ORCHER_VERSION_ID must work whether a worker is built via the
    # builder or from_env; otherwise the variable would silently work in one
    # path and not the other.
    from orcher.worker.builder import WorkerBuilder

    with mock.patch.dict(os.environ, {"ORCHER_VERSION_ID": "sha-from-ci"}, clear=False):
        builder = WorkerBuilder()
    assert builder._version_id == "sha-from-ci"


def test_an_explicit_release_beats_the_environment() -> None:
    from orcher.worker.builder import WorkerBuilder

    with mock.patch.dict(os.environ, {"ORCHER_VERSION_ID": "sha-from-ci"}, clear=False):
        builder = WorkerBuilder().version_id("explicit")
    assert builder._version_id == "explicit"
