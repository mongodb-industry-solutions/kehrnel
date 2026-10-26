# Kehrnel Strategy SDK

The Strategy SDK is the Python authoring contract for executable Kehrnel
strategy packs. It is not a consumer API and it is not a remote runtime SDK.
Applications use the FHIR, openEHR, REST, event, MQL or MCP interfaces selected
by their interaction contracts.

The public SDK now imports the exact plug-in and execution types used by the
Kehrnel runtime. A pack cannot validate against one Python ABI and execute
against another.

## 1) Define the manifest

Each pack declares `id`, `name`, `version`, `domain`, `capabilities`, `ops`, an
`entrypoint` (`module:Class`), configuration schema/defaults, adapter
requirements, and optional presentation metadata. Bundled packs live under
`src/kehrnel/engine/strategies/**`.

Static pack validation checks layout, schemas and the entrypoint declaration
without importing pack code. Executable conformance is a separate trusted-code
step.

## 2) Implement the runtime hooks

Subclass `StrategyPlugin` and implement only the hooks advertised by the
manifest. Hooks receive one environment-scoped `StrategyContext`; resolved
adapters and bindings are on that context.

```python
from kehrnel.strategy_sdk import (
    ApplyPlan,
    ApplyResult,
    StrategyContext,
    StrategyManifest,
    StrategyPlugin,
)

MANIFEST = StrategyManifest(
    id="example.operational",
    name="My Strategy",
    version="1",
    domain="example",
    capabilities=["plan", "apply", "ingest"],
    entrypoint="example.strategy:MyStrategy",
)

class MyStrategy(StrategyPlugin):
    def __init__(self, manifest=MANIFEST):
        self.manifest = manifest

    async def validate_config(self, ctx: StrategyContext) -> None:
        ...

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        return ApplyPlan(artifacts={})

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        return ApplyResult()

    async def ingest(self, ctx: StrategyContext, payload: dict) -> dict:
        ...
```

Query-capable packs implement both `compile_query` and `execute_query`.
Manifest-specific operations implement `run_op`.

## 3) Run executable conformance

Only run executable conformance for built-in or reviewed code because it imports
the pack entrypoint:

```python
from kehrnel.strategy_sdk import validate_strategy_entrypoint

report = validate_strategy_entrypoint(manifest)
if not report.is_conformant:
    raise RuntimeError(report.as_dict())
```

Conformance verifies manifest identity, SDK inheritance, required capability
hooks, hook signatures, and operation dispatch. The bundled conformance suite
applies the same rules to every built-in pack.

## 4) Activate and operate

The Kehrnel Control API owns environment activation and operation. List packs
with `GET /v1/strategies`, activate one through the environment-scoped control
endpoint, then invoke its standard or manifest-specific operations through the
environment runtime. The Healthcare Data Lab Builder is a client of this API;
it is not part of the SDK.

Remote strategy isolation, a separately distributed cross-language SDK, and a
complete package/prove/promote lifecycle remain roadmap work and must not be
described as available.
