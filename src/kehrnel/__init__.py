"""kehrnel package root."""

from __future__ import annotations

import importlib
import sys


def _alias_engine_packages() -> None:
    """Expose canonical engine libs under legacy import paths."""
    for name in ("core", "common", "domains", "strategies"):
        legacy = f"{__name__}.{name}"
        target = f"{__name__}.engine.{name}"
        if legacy not in sys.modules:
            sys.modules[legacy] = importlib.import_module(target)


_alias_engine_packages()
