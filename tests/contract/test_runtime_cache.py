from kehrnel.core.runtime import StrategyRuntime
from kehrnel.core.registry import FileActivationRegistry
from pathlib import Path


def test_runtime_invalidate_env_cache_dict_only(tmp_path: Path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    env = "env1"
    rt._env_cache[env] = {"dict_cache": {"codes": {"a": 1}}, "adapters": {"x": 1}}
    rt.invalidate_env_cache(env, dict_only=True)
    assert rt._env_cache[env]["dict_cache"] == {}
    assert rt._env_cache[env]["adapters"] == {"x": 1}
