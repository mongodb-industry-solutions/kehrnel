#!/usr/bin/env python3
"""Regenerate the committed Kehrnel v1 foundation contract schemas."""

from __future__ import annotations

from pathlib import Path

from kehrnel.contracts.schema_registry import write_contract_schemas


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    write_contract_schemas(root / "src" / "kehrnel" / "contracts" / "schemas" / "v1")
