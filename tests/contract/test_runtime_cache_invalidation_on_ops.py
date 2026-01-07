import asyncio
from pathlib import Path

from kehrnel.core.runtime import StrategyRuntime
from kehrnel.core.registry import FileActivationRegistry
from kehrnel.strategy_sdk import StrategyBindings


async def _activate(rt: StrategyRuntime, manifest, env_id: str):
    bindings = StrategyBindings(extras={"db": {"provider": "none"}})
    await rt.activate(env_id, manifest.id, manifest.version, manifest.default_config or {}, bindings, allow_plaintext_bindings=True)


def test_cache_invalidation_on_maintenance_op(tmp_path: Path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    from kehrnel.api.app import _load_manifests

    manifests, _ = _load_manifests()
    manifest = next(m for m in manifests if "rps_dual" in m.id)
    rt.register_manifest(manifest)
    env_id = "env-cache"
    asyncio.get_event_loop().run_until_complete(_activate(rt, manifest, env_id))
    rt._env_cache[env_id] = {"dict_cache": {"codes": {"a": 1}}, "adapters": {}}

    # simulate maintenance invalidation
    rt.invalidate_env_cache(env_id, dict_only=True)
    assert rt._env_cache[env_id]["dict_cache"] == {}
