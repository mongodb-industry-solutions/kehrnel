#!/usr/bin/env python3
"""Refresh content-addressed catalog refs after strategy package edits."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from kehrnel.engine.core.blueprint_templates import (
    DEFAULT_BLUEPRINT_TEMPLATE_CATALOG_PATH,
)
from kehrnel.engine.core.catalog_maintenance import (
    CatalogRefreshError,
    refresh_bundled_catalogs,
)
from kehrnel.engine.core.outcome_catalog import (
    DEFAULT_OUTCOME_CATALOG_PATH,
    DEFAULT_STRATEGY_ROOT,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh exact strategy, blueprint, outcome, and reference-architecture "
            "refs plus all derived bundled catalog digests."
        )
    )
    parser.add_argument(
        "--strategy",
        action="append",
        required=True,
        dest="strategy_ids",
        help="Installed strategy id to refresh; repeat for multiple strategies.",
    )
    parser.add_argument(
        "--strategy-root",
        type=Path,
        default=DEFAULT_STRATEGY_ROOT,
    )
    parser.add_argument(
        "--blueprint-catalog",
        type=Path,
        default=DEFAULT_BLUEPRINT_TEMPLATE_CATALOG_PATH,
    )
    parser.add_argument(
        "--outcome-catalog",
        type=Path,
        default=DEFAULT_OUTCOME_CATALOG_PATH,
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Validate and report drift without writing; exit 1 when refresh "
            "is needed."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = refresh_bundled_catalogs(
            args.strategy_ids,
            strategy_root=args.strategy_root,
            blueprint_catalog_path=args.blueprint_catalog,
            outcome_catalog_path=args.outcome_catalog,
            write=not args.check,
        )
    except CatalogRefreshError as exc:
        print(f"catalog refresh failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report.to_wire(), ensure_ascii=False, indent=2, sort_keys=True))
    return 1 if args.check and report.changed else 0


if __name__ == "__main__":
    raise SystemExit(main())
