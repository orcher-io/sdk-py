"""Replay recorded workflow activations and check the SDK still answers each
the way it did when the expected results were recorded.

``contract/replay/<corpus>/`` holds activations recorded from this catalog
with ``ORCHER_RECORD_ACTIVATIONS`` (layout ``<workflow>/<run>/<n>-activation.pb``,
each an encoded ``orcher.v1.PollWorkflowExecutionResponse``), and
``expected.json`` beside them what the SDK answered each with: the commands,
the steps reached, the query and update answers, or the failure, as sdk-core
reads them before sending them to the engine.

Each activation is answered two ways, and both must give its expected result:

- the kernel path: resolved by the kernel into a ``RunActivation``, answered
  by ``Worker.run_activation``, and the answer turned back by the kernel, as
  the worker host does on a live worker;
- the journal path: the jobs sdk-core derives from the journal, answered by
  the worker's journal-reading execution, as the worker runs today.

The kernel protocol of sdk-core 0.14 carries neither a failed run's traceback
nor the steps it reached, so for a failed run those two fields are left out of
what is compared.

No engine is needed. Requires the native module (``maturin develop``)::

    PYTHONPATH=src:contract python contract/replay_check.py [--bless]

``--bless`` rewrites ``expected.json`` from what the journal path answers,
and requires the kernel path to answer the same. Exits non-zero if any
activation differs from its expected result or cannot be run, or if there is
no activation to check.
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


def comparable(result: dict[str, Any]) -> dict[str, Any]:
    """A result as sdk-core reads it, without what the kernel protocol of
    sdk-core 0.14 does not carry for a failed run."""
    if not result.get("successful") and isinstance(result.get("error"), dict):
        result = dict(result, error=dict(result["error"], details=None))
        result.pop("reached_steps", None)
    return result


async def kernel_answer(worker: Worker, recorded: bytes) -> dict[str, Any]:
    activation = json.loads(_native._kernel_resolve(recorded, NAMESPACE))
    # Some catalog workflows count their activations in the process; each
    # activation is answered as if it were the run's first in this process.
    catalog.ACTIVATIONS.clear()
    answer = await worker.run_activation(activation)
    run_id = activation["run"]["run_id"]
    return json.loads(_native._kernel_execution_result(run_id, json.dumps(answer)))


async def journal_answer(worker: Worker, recorded: bytes) -> dict[str, Any]:
    request = json.loads(_native._legacy_execution_request(recorded))
    catalog.ACTIVATIONS.clear()
    result = await worker._execute_workflow(request)
    return comparable(json.loads(_native._parse_execution_result(json.dumps(result))))


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
                kernel = await kernel_answer(worker, recorded)
            except ValueError as e:
                problems.append(f"{key}: the kernel refused the answer: {e}")
                continue
            journal = await journal_answer(worker, recorded)
            if args.bless:
                answered[key] = journal
                if kernel != journal:
                    problems.append(
                        f"{key}: the kernel path differs from the journal path\n"
                        f"  journal: {json.dumps(journal)}\n  kernel:  {json.dumps(kernel)}"
                    )
                continue
            if key not in expected:
                problems.append(f"{key}: no expected result")
                continue
            for path, result in (("kernel", kernel), ("journal", journal)):
                if result != expected[key]:
                    problems.append(
                        f"{key} ({path} path):\n"
                        f"  expected: {json.dumps(expected[key])}\n"
                        f"  answered: {json.dumps(result)}"
                    )
        if args.bless:
            if not problems:
                expected_path.write_text(json.dumps(answered, indent=2, sort_keys=True) + "\n")
                print(f"blessed {expected_path} ({len(answered)} activations)")
        else:
            for key in expected:
                if key not in dict(recorded_activations(corpus)):
                    problems.append(f"{key}: expected, but not in the corpus")

    for problem in problems:
        print(f"DIFFERS {problem}\n")
    print(f"{checked} recorded activations replayed, {len(problems)} differ")
    return 1 if checked == 0 or problems else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
