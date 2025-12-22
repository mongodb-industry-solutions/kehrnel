from __future__ import annotations

import importlib
import importlib.metadata
from typing import Dict, Iterable, Tuple

from .manifest import StrategyManifest
from .plugin import StrategyPlugin


class StrategyLoadError(RuntimeError):
    pass


def _split_path(path: str) -> Tuple[str, str]:
    if ":" not in path:
        raise StrategyLoadError("Entrypoint must be in the form 'module:attr'")
    module_path, attr = path.split(":", 1)
    return module_path, attr


def load_strategy_from_path(path: str, manifest_override: StrategyManifest | None = None) -> Tuple[StrategyManifest, StrategyPlugin]:
    """
    Load a strategy given a dotted path to either:
    - a StrategyPlugin subclass (requires module-level MANIFEST/manifest), or
    - a StrategyManifest instance (requires module-level PLUGIN/Plugin).
    """
    module_path, attr = _split_path(path)
    mod = importlib.import_module(module_path)
    obj = getattr(mod, attr, None)
    if obj is None:
        raise StrategyLoadError(f"Attribute '{attr}' not found in {module_path}")

    # Case 1: attr is a manifest; expect PLUGIN or Plugin in module
    if isinstance(obj, StrategyManifest):
        manifest = obj
        plugin_cls = getattr(mod, "PLUGIN", None) or getattr(mod, "Plugin", None)
        if plugin_cls is None:
            raise StrategyLoadError(f"No plugin class exported in {module_path}")
        if not issubclass(plugin_cls, StrategyPlugin):
            raise StrategyLoadError("PLUGIN must subclass StrategyPlugin")
        return manifest, plugin_cls(manifest)

    # Case 2: attr is plugin class; manifest must be provided or module exports MANIFEST/manifest
    if isinstance(obj, type) and issubclass(obj, StrategyPlugin):
        manifest = manifest_override or getattr(mod, "MANIFEST", None) or getattr(mod, "manifest", None)
        if manifest is None:
            raise StrategyLoadError(f"No manifest found alongside plugin {module_path}:{attr}")
        if not isinstance(manifest, StrategyManifest):
            raise StrategyLoadError("Manifest must be a StrategyManifest instance")
        return manifest, obj(manifest)

    raise StrategyLoadError(f"Unsupported entrypoint target: {type(obj)}")


def load_strategy_from_entrypoint(entrypoint_name: str, group: str = "kehrnel.strategy") -> Tuple[StrategyManifest, StrategyPlugin]:
    eps = importlib.metadata.entry_points()
    matches = [ep for ep in eps.select(group=group) if ep.name == entrypoint_name]
    if not matches:
        raise StrategyLoadError(f"Entrypoint '{entrypoint_name}' not found in group '{group}'")
    ep = matches[0]
    manifest = None
    # Entry point object already has the module:path string
    return load_strategy_from_path(ep.value, manifest_override=manifest)


def discover_entrypoint_manifests(group: str = "kehrnel.strategy") -> Dict[str, StrategyManifest]:
    """Enumerate registered entrypoints and return their manifests (without instantiating plugins)."""
    manifests: Dict[str, StrategyManifest] = {}
    eps = importlib.metadata.entry_points().select(group=group)
    for ep in eps:
        try:
            manifest, _ = load_strategy_from_path(ep.value)
            manifests[ep.name] = manifest
        except Exception:
            continue  # Skip broken registrations; can be logged by caller
    return manifests
