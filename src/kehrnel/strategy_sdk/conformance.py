"""Executable conformance checks for trusted Strategy SDK plug-ins."""

from __future__ import annotations

from dataclasses import dataclass
from inspect import signature
from typing import Iterable, Tuple, Type

from .loader import StrategyLoadError, load_strategy_from_path
from .manifest import StrategyManifest
from .plugin import StrategyPlugin


_CAPABILITY_HOOKS = {
    "ingest": ("ingest",),
    "transform": ("transform",),
    "reverse_transform": ("reverse_transform",),
    "validate": ("validate_config",),
    "query": ("compile_query", "execute_query"),
    "authoritative_write": ("execute_authoritative_write",),
    "plan": ("plan",),
    "apply": ("apply",),
}

_HOOK_ARGUMENT_COUNTS = {
    "validate_config": 1,
    "plan": 1,
    "apply": 2,
    "transform": 2,
    "reverse_transform": 2,
    "ingest": 2,
    "compile_query": 3,
    "execute_query": 2,
    "execute_authoritative_write": 2,
    "run_op": 3,
}


@dataclass(frozen=True)
class StrategyConformanceDiagnostic:
    code: str
    message: str
    path: str

    def as_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "path": self.path,
        }


@dataclass(frozen=True)
class StrategyConformanceReport:
    strategy_id: str
    strategy_version: str
    diagnostics: Tuple[StrategyConformanceDiagnostic, ...]

    @property
    def is_conformant(self) -> bool:
        return not self.diagnostics

    def as_dict(self) -> dict[str, object]:
        return {
            "strategyId": self.strategy_id,
            "strategyVersion": self.strategy_version,
            "isConformant": self.is_conformant,
            "diagnostics": [item.as_dict() for item in self.diagnostics],
        }


class StrategyConformanceError(RuntimeError):
    def __init__(self, report: StrategyConformanceReport):
        self.report = report
        summary = "; ".join(item.message for item in report.diagnostics)
        super().__init__(summary or "strategy is conformant")


def _overrides(plugin_type: Type[StrategyPlugin], hook: str) -> bool:
    return getattr(plugin_type, hook, None) is not getattr(StrategyPlugin, hook, None)


def _accepts_runtime_arguments(plugin_type: Type[StrategyPlugin], hook: str) -> bool:
    method = getattr(plugin_type, hook, None)
    if method is None:
        return False
    argument_count = _HOOK_ARGUMENT_COUNTS[hook]
    try:
        signature(method).bind(*([object()] * (argument_count + 1)))
    except (TypeError, ValueError):
        return False
    return True


def validate_strategy_plugin(
    manifest: StrategyManifest,
    plugin: StrategyPlugin,
) -> StrategyConformanceReport:
    """Validate an already-loaded trusted plug-in against its manifest."""

    diagnostics: list[StrategyConformanceDiagnostic] = []
    plugin_type = type(plugin)

    if not isinstance(plugin, StrategyPlugin):
        diagnostics.append(
            StrategyConformanceDiagnostic(
                code="PLUGIN_ABI_MISMATCH",
                message="entrypoint must subclass kehrnel.strategy_sdk.StrategyPlugin",
                path="/entrypoint",
            )
        )
    plugin_manifest = getattr(plugin, "manifest", None)
    if plugin_manifest is None:
        diagnostics.append(
            StrategyConformanceDiagnostic(
                code="PLUGIN_MANIFEST_MISSING",
                message="entrypoint did not expose its hydrated manifest",
                path="/entrypoint",
            )
        )
    elif (
        getattr(plugin_manifest, "id", None) != manifest.id
        or getattr(plugin_manifest, "version", None) != manifest.version
    ):
        diagnostics.append(
            StrategyConformanceDiagnostic(
                code="PLUGIN_MANIFEST_MISMATCH",
                message="entrypoint manifest identity differs from the validated pack",
                path="/entrypoint",
            )
        )

    for capability in manifest.capabilities:
        required_hooks = _CAPABILITY_HOOKS.get(capability)
        if not required_hooks:
            continue
        for hook in required_hooks:
            if not _overrides(plugin_type, hook):
                diagnostics.append(
                    StrategyConformanceDiagnostic(
                        code="CAPABILITY_HOOK_MISSING",
                        message=f"capability '{capability}' requires the '{hook}' hook",
                        path=f"/capabilities/{capability}",
                    )
                )
            elif not _accepts_runtime_arguments(plugin_type, hook):
                diagnostics.append(
                    StrategyConformanceDiagnostic(
                        code="CAPABILITY_HOOK_SIGNATURE_INVALID",
                        message=(
                            f"capability '{capability}' hook '{hook}' does not "
                            "accept the runtime arguments"
                        ),
                        path=f"/capabilities/{capability}",
                    )
                )

    if manifest.ops and not _overrides(plugin_type, "run_op"):
        diagnostics.append(
            StrategyConformanceDiagnostic(
                code="OP_DISPATCHER_MISSING",
                message="manifest operations require the 'run_op' hook",
                path="/ops",
            )
        )
    elif manifest.ops and not _accepts_runtime_arguments(plugin_type, "run_op"):
        diagnostics.append(
            StrategyConformanceDiagnostic(
                code="OP_DISPATCHER_SIGNATURE_INVALID",
                message="the 'run_op' hook does not accept the runtime arguments",
                path="/ops",
            )
        )

    return StrategyConformanceReport(
        strategy_id=manifest.id,
        strategy_version=manifest.version,
        diagnostics=tuple(diagnostics),
    )


def validate_strategy_entrypoint(
    manifest: StrategyManifest,
) -> StrategyConformanceReport:
    """Import and validate a trusted pack entrypoint.

    Static pack validation remains import-free. This executable check is for
    built-in packs and reviewed packages before activation.
    """

    if not manifest.entrypoint:
        return StrategyConformanceReport(
            strategy_id=manifest.id,
            strategy_version=manifest.version,
            diagnostics=(
                StrategyConformanceDiagnostic(
                    code="ENTRYPOINT_MISSING",
                    message="strategy manifest has no executable entrypoint",
                    path="/entrypoint",
                ),
            ),
        )
    try:
        loaded_manifest, plugin = load_strategy_from_path(
            manifest.entrypoint,
            manifest_override=manifest,
        )
    except (ImportError, AttributeError, TypeError, StrategyLoadError) as exc:
        return StrategyConformanceReport(
            strategy_id=manifest.id,
            strategy_version=manifest.version,
            diagnostics=(
                StrategyConformanceDiagnostic(
                    code="ENTRYPOINT_LOAD_FAILED",
                    message=str(exc),
                    path="/entrypoint",
                ),
            ),
        )
    return validate_strategy_plugin(loaded_manifest, plugin)


def require_strategy_conformance(manifest: StrategyManifest) -> None:
    report = validate_strategy_entrypoint(manifest)
    if not report.is_conformant:
        raise StrategyConformanceError(report)


def reports_are_conformant(reports: Iterable[StrategyConformanceReport]) -> bool:
    return all(report.is_conformant for report in reports)


__all__ = [
    "StrategyConformanceDiagnostic",
    "StrategyConformanceError",
    "StrategyConformanceReport",
    "reports_are_conformant",
    "require_strategy_conformance",
    "validate_strategy_entrypoint",
    "validate_strategy_plugin",
]
