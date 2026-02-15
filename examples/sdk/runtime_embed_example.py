"""Minimal Kehrnel runtime embedding example (Python SDK style).

Run with:
  python3 examples/sdk/runtime_embed_example.py

Prerequisites:
  - strategies discovered by Kehrnel runtime
  - valid bindings_ref for your environment
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from kehrnel.core.runtime import StrategyRuntime
from kehrnel.core.registry import FileActivationRegistry
from kehrnel.strategy_sdk import StrategyBindings


async def main() -> None:
    registry = FileActivationRegistry(Path(".kehrnel_registry.json"))
    runtime = StrategyRuntime(registry)

    env_id = "dev"
    domain = "openehr"
    strategy_id = "openehr.rps_dual"

    # In production prefer bindings_ref and resolver-backed secrets.
    activation = await runtime.activate(
        env_id=env_id,
        strategy_id=strategy_id,
        version="latest",
        config={"database": "openehr_db"},
        bindings=StrategyBindings(),
        domain=domain,
        bindings_ref="env:dev",
    )
    print("activated:", activation.strategy_id, activation.domain, activation.env_id)

    compiled = await runtime.dispatch(
        env_id,
        "compile_query",
        {
            "domain": domain,
            "query": "SELECT c FROM EHR e CONTAINS COMPOSITION c LIMIT 5",
        },
    )
    print("compiled keys:", list((compiled or {}).keys()))

    executed = await runtime.dispatch(
        env_id,
        "query",
        {
            "domain": domain,
            "query": "SELECT c FROM EHR e CONTAINS COMPOSITION c LIMIT 5",
        },
    )
    print("query result keys:", list((executed or {}).keys()) if isinstance(executed, dict) else type(executed))


if __name__ == "__main__":
    asyncio.run(main())
