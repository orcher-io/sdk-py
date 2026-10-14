"""A worker takes the client's TLS options and hands them to every connection.

The native worker config always accepted CA and client certificate paths, but
the Python worker never passed them, and the builder and config had nowhere to
put them, so a worker could not reach a server that requires a private CA or a
client certificate. These pin each way of configuring TLS down to the native
config the connections are built from.
"""

from __future__ import annotations

import os
from pathlib import Path
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


def test_the_worker_hands_tls_paths_to_the_worker_host() -> None:
    # The paths only matter if they reach the host every connection is built
    # from.
    from tests.test_worker_api_key import started_host_config

    worker = _builder().tls(ca_cert_path=CA, client_cert_path=CERT, client_key_path=KEY).build()
    host_config = started_host_config(worker)
    assert host_config["tls_ca_cert_path"] == CA
    assert host_config["tls_client_cert_path"] == CERT
    assert host_config["tls_client_key_path"] == KEY


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
