"""A worker authenticates with an API key, and never shows it.

The worker accepted an organization but had no way to take a key, and the
native layer sent none on any connection it built, so a worker could not
connect to a server that requires authentication at all. These pin the key's
path from each way of configuring a worker down to the native config the
connections are built from, and that printing a config does not print it.
"""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace
from unittest import mock

import pytest

from orcher.client.config import ClientConfig
from orcher.worker.builder import WorkerBuilder
from orcher.worker.config import WorkerConfig

KEY = "orch_secret_0123456789"
REQUIRED = {"ORCHER_SERVER_URL": "http://localhost:50051", "ORCHER_TASK_QUEUE": "q"}


def test_from_env_reads_the_api_key() -> None:
    with mock.patch.dict(os.environ, {**REQUIRED, "ORCHER_API_KEY": KEY}, clear=True):
        config = WorkerConfig.from_env()
    assert config.api_key == KEY


def test_from_env_reads_the_api_key_under_its_prefix() -> None:
    env = {"APP_SERVER_URL": "http://localhost:50051", "APP_TASK_QUEUE": "q", "APP_API_KEY": KEY}
    with mock.patch.dict(os.environ, env, clear=True):
        config = WorkerConfig.from_env(prefix="APP_")
    assert config.api_key == KEY


def test_from_env_without_a_key_sends_none() -> None:
    for env in (REQUIRED, {**REQUIRED, "ORCHER_API_KEY": ""}, {**REQUIRED, "ORCHER_API_KEY": " "}):
        with mock.patch.dict(os.environ, env, clear=True):
            config = WorkerConfig.from_env()
        # An exported-but-empty variable must not become "authorization: Bearer ".
        assert config.api_key is None, env


def test_the_builder_takes_a_key() -> None:
    with mock.patch.dict(os.environ, {}, clear=True):
        builder = WorkerBuilder().api_key(KEY)
    assert builder._api_key == KEY


def test_the_builder_defaults_the_key_from_the_environment() -> None:
    # Parity with from_env and with the client, which both read ORCHER_API_KEY.
    with mock.patch.dict(os.environ, {"ORCHER_API_KEY": KEY}, clear=True):
        builder = WorkerBuilder()
    assert builder._api_key == KEY


def test_an_explicit_key_beats_the_environment() -> None:
    with mock.patch.dict(os.environ, {"ORCHER_API_KEY": "from-env"}, clear=True):
        builder = WorkerBuilder().api_key(KEY)
    assert builder._api_key == KEY


def test_the_built_worker_carries_the_key() -> None:
    with mock.patch.dict(os.environ, {}, clear=True):
        worker = (
            WorkerBuilder()
            .server_url("http://localhost:50051")
            .task_queue("q")
            .api_key(KEY)
            .build()
        )
    assert worker.config.api_key == KEY


def test_the_worker_hands_the_key_to_the_native_config() -> None:
    # The Python value only matters if it reaches the native config every
    # connection is built from. Stop at the bridge, which would connect.
    native = pytest.importorskip("orcher._native")
    from orcher.worker import worker as worker_module

    built: list[object] = []

    class StopAtBridgeError(Exception):
        pass

    def bridge(config: object) -> None:
        built.append(config)
        raise StopAtBridgeError

    stub = SimpleNamespace(WorkerConfig=native.WorkerConfig, BridgeWorker=bridge)
    config = WorkerConfig(
        server_url="http://localhost:50051",
        task_queue="q",
        api_key=KEY,
        organization_id="org_123",
    )
    worker = worker_module.Worker(config)
    with (
        mock.patch.object(worker_module, "is_native_available", return_value=True),
        mock.patch.object(worker_module, "get_native_module", return_value=stub),
        pytest.raises(StopAtBridgeError),
    ):
        asyncio.run(worker._initialize())

    (native_config,) = built
    assert native_config.api_key == KEY
    assert native_config.organization_id == "org_123"


def test_the_native_config_refuses_a_key_it_could_not_send() -> None:
    native = pytest.importorskip("orcher._native")
    with pytest.raises(ValueError, match="api_key"):
        native.WorkerConfig(server_url="http://localhost:50051", task_queue="q", api_key="a\nb")


def test_no_config_prints_the_key() -> None:
    native = pytest.importorskip("orcher._native")
    configs = [
        WorkerConfig(server_url="http://localhost:50051", task_queue="q", api_key=KEY),
        ClientConfig(server_url="http://localhost:50051", api_key=KEY),
        native.WorkerConfig(server_url="http://localhost:50051", task_queue="q", api_key=KEY),
        native.ClientConfig(server_url="http://localhost:50051", api_key=KEY),
    ]
    for config in configs:
        for shown in (repr(config), str(config)):
            assert KEY not in shown, f"{type(config).__name__} printed the key: {shown}"


def test_a_blank_client_key_is_no_key() -> None:
    # The README passes os.environ.get("ORCHER_API_KEY") straight through.
    for blank in ("", "  "):
        assert ClientConfig(server_url="http://localhost:50051", api_key=blank).api_key is None
