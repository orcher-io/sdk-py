"""The public API speaks Orcher's vocabulary.

Orcher sends events, runs tasks and restarts a workflow fresh; it has no
signals, activities or continue-as-new. A public name borrowed from another
engine's vocabulary misleads users about what the API does.
"""

from __future__ import annotations

import enum
import importlib
import inspect
import pkgutil
import re

import orcher

FOREIGN = re.compile(r"signal|activit|continue_?as_?new", re.IGNORECASE)


def _public_modules() -> list[object]:
    modules = [orcher]
    for info in pkgutil.walk_packages(orcher.__path__, "orcher."):
        if any(part.startswith("_") for part in info.name.split(".")):
            continue
        modules.append(importlib.import_module(info.name))
    return modules


def _public_names() -> set[str]:
    names: set[str] = set()
    for module in _public_modules():
        for name, value in vars(module).items():
            if name.startswith("_"):
                continue
            names.add(f"{module.__name__}.{name}")
            if inspect.isclass(value) and value.__module__.startswith("orcher"):
                for member in vars(value):
                    if not member.startswith("_"):
                        names.add(f"{module.__name__}.{name}.{member}")
                if issubclass(value, enum.Enum):
                    names.update(
                        f"{module.__name__}.{name}.{m.value}" for m in value
                        if isinstance(m.value, str)
                    )
    return names


def test_no_public_name_uses_another_engines_vocabulary() -> None:
    assert sorted(n for n in _public_names() if FOREIGN.search(n)) == []
