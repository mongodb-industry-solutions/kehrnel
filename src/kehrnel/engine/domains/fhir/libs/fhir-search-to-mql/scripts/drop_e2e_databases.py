#!/usr/bin/env python3
"""Drop E2E databases and clear tests/e2e/results (wrapper)."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

_GEN_SCRIPT = (
    Path(__file__).resolve().parents[1].parent
    / "fhir-data-generation"
    / "scripts"
    / "drop_e2e_databases.py"
)

if __name__ == "__main__":
    if not _GEN_SCRIPT.is_file():
        print(f"Missing {_GEN_SCRIPT}", file=sys.stderr)
        raise SystemExit(1)
    runpy.run_path(str(_GEN_SCRIPT), run_name="__main__")
