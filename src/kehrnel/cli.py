"""Lightweight CLI helpers for strategy packs."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.pack_validator import StrategyPackValidator


def _is_public_strategy_id(strategy_id: object) -> bool:
    return str(strategy_id or "").strip().lower() != "openehr.rps_dual_ibm"


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
        for manifest_path in base.glob("**/manifest.json"):
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            if not _is_public_strategy_id(data.get("id")):
                continue
            base_dir = manifest_path.parent
            defaults_path = base_dir / "defaults.json"
            schema_path = base_dir / "schema.json"
            spec_field = data.get("spec")
            if isinstance(spec_field, dict):
                spec_path = base_dir / (spec_field.get("path") or "spec.json")
            elif isinstance(spec_field, str):
                spec_path = base_dir / spec_field
            else:
                spec_path = base_dir / "spec.json"
            if defaults_path.exists():
                data["default_config"] = json.loads(defaults_path.read_text(encoding="utf-8"))
            if schema_path.exists():
                data["config_schema"] = json.loads(schema_path.read_text(encoding="utf-8"))
            if data.get("pack_format") == "strategy-pack/v1" and spec_path.exists():
                try:
                    data["pack_spec"] = json.loads(spec_path.read_text(encoding="utf-8"))
                except Exception:
                    pass
            # validate
            errors = StrategyPackValidator(data, base_dir).validate()
            if errors:
                continue
            manifests.append(StrategyManifest(**data))
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
