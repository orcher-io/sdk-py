"""A worker takes the client's TLS options and hands them to every connection.

The native worker config always accepted CA and client certificate paths, but
the Python worker never passed them, and the builder and config had nowhere to
put them, so a worker could not reach a server that requires a private CA or a
client certificate. These pin each way of configuring TLS down to the native
config the connections are built from.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from orcher.client.config import ClientConfig
from orcher.errors import ConfigurationError
from orcher.worker.builder import WorkerBuilder
from orcher.worker.config import WorkerConfig

CA, CERT, KEY = "/certs/ca.pem", "/certs/client.pem", "/certs/client-key.pem"


def _builder() -> WorkerBuilder:
    return WorkerBuilder().server_url("https://orcher.example:443").task_queue("q")


def test_the_builder_takes_tls_paths() -> None:
    worker = _builder().tls(ca_cert_path=CA, client_cert_path=CERT, client_key_path=KEY).build()
    assert worker.config.tls_ca_cert_path == CA
    assert worker.config.tls_client_cert_path == CERT
    assert worker.config.tls_client_key_path == KEY


def test_the_builder_takes_path_objects() -> None:
    worker = _builder().tls(ca_cert_path=Path(CA)).build()
    assert worker.config.tls_ca_cert_path == CA
    assert worker.config.tls_client_cert_path is None


def test_without_tls_the_worker_has_no_paths() -> None:
    worker = _builder().build()
    assert worker.config.tls_ca_cert_path is None
    assert worker.config.tls_client_cert_path is None
    assert worker.config.tls_client_key_path is None


@pytest.mark.parametrize(
    "kwargs", [{"client_cert_path": CERT}, {"client_key_path": KEY}], ids=["cert", "key"]
)
def test_a_half_client_identity_is_rejected(kwargs: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="set together"):
        _builder().tls(**kwargs)
    with pytest.raises(ValueError, match="set together"):
        WorkerConfig(
            server_url="https://x",
            task_queue="q",
            **{f"tls_{k}": v for k, v in kwargs.items()},
        )
    with pytest.raises(ConfigurationError, match="set together"):
        ClientConfig(server_url="https://x", **{f"tls_{k}": v for k, v in kwargs.items()})


def test_from_env_reads_tls_paths() -> None:
    env = {
        "ORCHER_SERVER_URL": "https://orcher.example",
        "ORCHER_TASK_QUEUE": "q",
        "ORCHER_TLS_CA_CERT_PATH": CA,
        "ORCHER_TLS_CLIENT_CERT_PATH": CERT,
        "ORCHER_TLS_CLIENT_KEY_PATH": KEY,
    }
    with mock.patch.dict(os.environ, env, clear=True):
        config = WorkerConfig.from_env()
    assert (config.tls_ca_cert_path, config.tls_client_cert_path, config.tls_client_key_path) == (
        CA,
        CERT,
        KEY,
    )


def test_the_worker_hands_tls_paths_to_the_native_config() -> None:
    # The paths only matter if they reach the native config every driver's
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
    worker = _builder().tls(ca_cert_path=CA, client_cert_path=CERT, client_key_path=KEY).build()
    with (
        mock.patch.object(worker_module, "is_native_available", return_value=True),
        mock.patch.object(worker_module, "get_native_module", return_value=stub),
        pytest.raises(StopAtBridgeError),
    ):
        asyncio.run(worker._initialize())

    (native_config,) = built
    assert native_config.tls_ca_cert_path == CA
    assert native_config.tls_client_cert_path == CERT
    assert native_config.tls_client_key_path == KEY


def test_the_client_reads_the_api_key_from_the_environment() -> None:
    # Parity with the worker builder and WorkerConfig.from_env.
    with mock.patch.dict(os.environ, {"ORCHER_API_KEY": "orch_env"}, clear=True):
        assert ClientConfig(server_url="http://localhost:50051").api_key == "orch_env"
        # An explicit value, including None, beats the environment.
        assert ClientConfig(server_url="http://localhost:50051", api_key="x").api_key == "x"
        assert ClientConfig(server_url="http://localhost:50051", api_key=None).api_key is None
    with mock.patch.dict(os.environ, {"ORCHER_API_KEY": " "}, clear=True):
        assert ClientConfig(server_url="http://localhost:50051").api_key is None
    with mock.patch.dict(os.environ, {}, clear=True):
        assert ClientConfig(server_url="http://localhost:50051").api_key is None
