"""Replay recorded workflow activations and check the SDK still answers each
the way it did when the expected results were recorded.

``contract/replay/<corpus>/`` holds activations recorded from this catalog
with ``ORCHER_RECORD_ACTIVATIONS`` (layout ``<workflow>/<run>/<n>-activation.pb``,
each an encoded ``orcher.v1.PollWorkflowExecutionResponse``), and
``expected.json`` beside them what the SDK answered each with: the commands,
the steps reached, the query and update answers, or the failure, as sdk-core
reads them before sending them to the engine.

The expected results were produced while the worker still had its
journal-reading path as well, and both paths agreed on every one.

Each activation is resolved by the kernel into a ``RunActivation``, answered
by ``Worker.run_activation``, and the answer turned back by the kernel, as the
worker host does on a live worker.

No engine is needed. Requires the native module (``maturin develop``)::

    PYTHONPATH=src:contract python contract/replay_check.py [--bless]

``--bless`` rewrites ``expected.json`` from what the SDK answers now, for a
change meant to alter what it sends. Exits non-zero if any activation differs
from its expected result or cannot be run, or if there is no activation to
check.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

# Registers the catalog's workflows with the global registry, which the
# worker collects when it is created.
import catalog

from orcher import _native
from orcher.worker.config import WorkerConfig
from orcher.worker.worker import Worker

NAMESPACE = "contract"
ROOT = Path(__file__).parent / "replay"


def corpora() -> list[Path]:
    """Every corpus under ``contract/replay``, in order."""
    return sorted(p for p in ROOT.iterdir() if p.is_dir())


def recorded_activations(corpus: Path) -> list[tuple[str, bytes]]:
    """Every recorded activation in a corpus, keyed ``<workflow>/<run>/<file>``."""
    out: list[tuple[str, bytes]] = []
    for path in sorted(corpus.glob("*/*/*-activation.pb")):
        out.append((path.relative_to(corpus).as_posix(), path.read_bytes()))
    return out


async def answer(worker: Worker, recorded: bytes) -> dict[str, Any]:
    activation = json.loads(_native._kernel_resolve(recorded, NAMESPACE))
    # Some catalog workflows count their activations in the process; each
    # activation is answered as if it were the run's first in this process.
    catalog.ACTIVATIONS.clear()
    reply = await worker.run_activation(activation)
    run_id = activation["run"]["run_id"]
    return json.loads(_native._kernel_execution_result(run_id, json.dumps(reply)))


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bless", action="store_true", help="rewrite expected.json")
    args = parser.parse_args()

    # The workflows' own failures are expected results here, not news.
    logging.disable(logging.CRITICAL)
    # Never run, so it connects to nothing.
    worker = Worker(
        WorkerConfig(server_url="http://127.0.0.1:1", task_queue=NAMESPACE, namespace=NAMESPACE)
    )

    checked = 0
    problems: list[str] = []
    for corpus in corpora():
        expected_path = corpus / "expected.json"
        expected: dict[str, Any] = {} if args.bless else json.loads(expected_path.read_text())
        answered: dict[str, Any] = {}
        for key, recorded in recorded_activations(corpus):
            checked += 1
            try:
                result = await answer(worker, recorded)
            except ValueError as e:
                problems.append(f"{key}: the kernel refused the answer: {e}")
                continue
            answered[key] = result
            if args.bless:
                continue
            if key not in expected:
                problems.append(f"{key}: no expected result")
            elif result != expected[key]:
                problems.append(
                    f"{key}:\n"
                    f"  expected: {json.dumps(expected[key])}\n"
                    f"  answered: {json.dumps(result)}"
                )
        if args.bless:
            if not problems:
                expected_path.write_text(json.dumps(answered, indent=2, sort_keys=True) + "\n")
                print(f"blessed {expected_path} ({len(answered)} activations)")
        else:
            for key in expected:
                if key not in answered:
                    problems.append(f"{key}: expected, but not in the corpus")

    for problem in problems:
        print(f"DIFFERS {problem}\n")
    print(f"{checked} recorded activations replayed, {len(problems)} differ")
    return 1 if checked == 0 or problems else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
