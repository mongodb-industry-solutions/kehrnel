# Examples

Integration examples for using Kehrnel as a reference modeling layer, which can be used to structure healthcare modeling patterns.

## Contents

- `examples/sdk/runtime_embed_example.py`
Python embedding pattern using `StrategyRuntime` (`activate` + `dispatch`).

- `examples/api/curl_flow.sh`
HTTP integration flow (`/strategies` -> `/activate` -> `/compile_query` -> `/query` -> strategy op).

- `examples/cli/pipeline.sh`
CLI-oriented automation skeleton for pack validation and command-driven workflows.

- `examples/cli/full_workflow_console.sh`
End-to-end workflow smoke test using unified CLI primitives (`context`, `resource`, `op`, `run`) plus local template flow.

## Quick Usage

```bash
# Python SDK embedding example
python3 examples/sdk/runtime_embed_example.py

# API flow example
BASE_URL=http://localhost:8000 examples/api/curl_flow.sh

# CLI flow skeleton
examples/cli/pipeline.sh

# Full workflow smoke test
RUNTIME_URL=http://localhost:8000 \
ENV_ID=dev \
DOMAIN=openehr \
STRATEGY_ID=openehr.rps_dual \
BINDINGS_REF=env://DB_BINDINGS \
examples/cli/full_workflow_console.sh
```

## Notes

- These examples are intentionally minimal and environment-agnostic.
- Replace `env`, `bindings_ref`, and strategy-specific payloads with your deployment values.
- For production, enable API auth and avoid plaintext bindings.
