import asyncio
from pathlib import Path
from typing import Dict, Any

from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.core.runtime import StrategyRuntime
from kehrnel.strategy_sdk import StrategyBindings
from tests.helpers.fixture_storage import FixtureStorage


def setup_test_app(registry_path: Path, fixture_dir: Path):
    app = create_app(str(registry_path))
    client = TestClient(app)
    runtime: StrategyRuntime = app.state.strategy_runtime
    # prefer openEHR strategy for contract helpers
    manifest = next((m for m in runtime.list_strategies() if "openehr" in m.id), runtime.list_strategies()[0])
    env_id = "env-contract"

    async def _activate():
        bindings = StrategyBindings(extras={"db": {"provider": "none"}})
        cfg = dict(manifest.default_config or {})
        # ensure search is enabled for cross-patient branch
        cfg.setdefault("collections", {}).setdefault("search", {}).setdefault("enabled", True)
        domain = getattr(manifest, "domain", "default")
        await runtime.activate(env_id, manifest.id, manifest.version, cfg, bindings, allow_plaintext_bindings=True, domain=domain)
        runtime._env_cache[env_id] = {
            "adapters": {"storage": FixtureStorage(fixture_dir), "index_admin": FixtureStorage(fixture_dir)},
            "dict_cache": {},
        }

    asyncio.get_event_loop().run_until_complete(_activate())
    return app, client, runtime, manifest, env_id
