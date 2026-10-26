"""Lightweight CLI helpers for strategy packs."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from kehrnel.engine.core.pack_loader import (
    iter_strategy_manifest_paths,
    load_strategy,
    load_strategy_manifest,
)
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.engine.core.manifest import StrategyManifest


def _strategy_paths() -> list[Path]:
    paths = [Path(__file__).resolve().parents[1] / "engine" / "strategies"]
    extra = os.getenv("KEHRNEL_STRATEGY_PATHS")
    if extra:
        extra_paths = []
        for sep in (":", ","):
            if sep in extra:
                extra_paths = [p for p in extra.split(sep) if p]
                break
        if not extra_paths:
            extra_paths = [extra]
        for part in extra_paths:
            if part:
                paths.append(Path(part))
    return paths


def _discover_manifests() -> list[StrategyManifest]:
    manifests: list[StrategyManifest] = []
    for base in _strategy_paths():
        if not base.exists():
            continue
        for manifest_path in iter_strategy_manifest_paths(base):
            try:
                manifests.append(load_strategy_manifest(manifest_path))
            except Exception as exc:
                print(f"Skipping invalid strategy manifest {manifest_path}: {exc}", file=sys.stderr)
                continue
    return manifests


def _cmd_load(args):
    manifest = load_strategy(args.strategy_id, args.path)
    runtime = StrategyRuntime(FileActivationRegistry(Path(args.registry)))
    runtime.register_manifest(manifest)
    print(f"Loaded {manifest.id} into registry {args.registry}")


def _cmd_list(args):
    manifests = _discover_manifests()
    if not manifests:
        print("No strategy packs found.")
        return
    for m in manifests:
        print(f"{m.id}  v{m.version}  ({m.domain}) -> {m.entrypoint}")


def main(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Kehrnel CLI")
    sub = parser.add_subparsers(dest="command")

    load_cmd = sub.add_parser("load", help="Load a strategy pack from disk")
    load_cmd.add_argument("path", help="Path to strategy pack directory")
    load_cmd.add_argument("--strategy-id", dest="strategy_id", help="Expected strategy id (optional)")
    load_cmd.add_argument(
        "--registry",
        dest="registry",
        default=str(Path(".kehrnel_registry.json")),
        help="Path to activation registry file",
    )
    load_cmd.set_defaults(func=_cmd_load)

    list_cmd = sub.add_parser("list", help="Discover available strategy packs")
    list_cmd.set_defaults(func=_cmd_list)

    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
