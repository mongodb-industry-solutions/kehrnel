from __future__ import annotations

from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime


def test_invalidate_env_cache_dict_only_clears_query_compile_cache(tmp_path):
    runtime = StrategyRuntime(FileActivationRegistry(tmp_path / "registry.json"))
    runtime._env_cache["env-1"] = {
        "adapters": {"storage": object()},
        "dict_cache": {"shortcuts": "cached"},
        "query_compile_cache": {"raw_aql_ast": {"SELECT 1": {"select": {}}}},
    }

    runtime.invalidate_env_cache("env-1", dict_only=True)

    assert runtime._env_cache["env-1"]["dict_cache"] == {}
    assert runtime._env_cache["env-1"]["query_compile_cache"] == {}
    assert "adapters" in runtime._env_cache["env-1"]
