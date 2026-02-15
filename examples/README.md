# Examples

Integration examples for using Kehrnel independently of Healthcare Data Lab.

## Contents

- `examples/sdk/runtime_embed_example.py`
Python embedding pattern using `StrategyRuntime` (`activate` + `dispatch`).

- `examples/api/curl_flow.sh`
HTTP integration flow (`/strategies` -> `/activate` -> `/compile_query` -> `/query` -> strategy op).

- `examples/cli/pipeline.sh`
CLI-oriented automation skeleton for pack validation and command-driven workflows.

## Quick Usage

```bash
# Python SDK embedding example
python3 examples/sdk/runtime_embed_example.py

# API flow example
BASE_URL=http://localhost:8000 examples/api/curl_flow.sh

# CLI flow skeleton
examples/cli/pipeline.sh
```

## Notes

- These examples are intentionally minimal and environment-agnostic.
- Replace `env`, `bindings_ref`, and strategy-specific payloads with your deployment values.
- For production, enable API auth and avoid plaintext bindings.
