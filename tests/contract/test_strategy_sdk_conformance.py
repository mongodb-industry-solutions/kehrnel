import asyncio
from pathlib import Path

from kehrnel.engine.core.pack_loader import (
    iter_strategy_manifest_paths,
    load_strategy_manifest,
)
from kehrnel.engine.core.plugin import StrategyPlugin as RuntimeStrategyPlugin
from kehrnel.engine.core.types import StrategyContext as RuntimeStrategyContext
from kehrnel.strategy_sdk import (
    StrategyBindings,
    StrategyContext,
    StrategyHandle,
    StrategyPlugin,
    validate_strategy_entrypoint,
    validate_strategy_plugin,
)


STRATEGY_ROOT = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "kehrnel"
    / "engine"
    / "strategies"
)


def test_sdk_and_runtime_share_one_plugin_abi():
    assert RuntimeStrategyPlugin is StrategyPlugin
    assert RuntimeStrategyContext is StrategyContext


def test_every_bundled_strategy_conforms_to_the_public_sdk():
    reports = []
    for path in iter_strategy_manifest_paths(STRATEGY_ROOT):
        manifest = load_strategy_manifest(path)
        reports.append(validate_strategy_entrypoint(manifest))

    failures = {
        report.strategy_id: [item.message for item in report.diagnostics]
        for report in reports
        if not report.is_conformant
    }
    assert failures == {}
    assert len(reports) == 8


def test_declared_capability_requires_its_runtime_hook():
    class MissingQueryStrategy(StrategyPlugin):
        pass

    manifest = load_strategy_manifest(
        STRATEGY_ROOT / "genomics" / "variant_first" / "manifest.json"
    )
    broken = validate_strategy_plugin(manifest, MissingQueryStrategy())

    assert not broken.is_conformant
    assert {item.code for item in broken.diagnostics} == {
        "CAPABILITY_HOOK_MISSING",
        "PLUGIN_MANIFEST_MISSING",
    }
    assert broken.as_dict()["isConformant"] is False


def test_declared_capability_rejects_an_incompatible_hook_signature():
    class InvalidQueryStrategy(StrategyPlugin):
        async def compile_query(self):
            return None

        async def execute_query(self):
            return None

    manifest = load_strategy_manifest(
        STRATEGY_ROOT / "genomics" / "variant_first" / "manifest.json"
    )
    strategy = InvalidQueryStrategy()
    strategy.manifest = manifest
    report = validate_strategy_plugin(manifest, strategy)

    assert {item.code for item in report.diagnostics} == {
        "CAPABILITY_HOOK_SIGNATURE_INVALID"
    }


def test_strategy_handle_validates_with_the_runtime_context():
    class ContextAwareStrategy(StrategyPlugin):
        seen_context = None

        async def validate_config(self, ctx):
            self.seen_context = ctx

    manifest = load_strategy_manifest(
        STRATEGY_ROOT / "genomics" / "variant_first" / "manifest.json"
    )
    strategy = ContextAwareStrategy()
    strategy.manifest = manifest
    handle = StrategyHandle(manifest, strategy)
    context = StrategyContext(environment_id="tenant-a", config={})

    asyncio.run(handle.activate({}, StrategyBindings(), context))

    assert strategy.seen_context is context
    assert handle.initialized is True
