---
sidebar_position: 3
---

# Core CLI Layer

`core` commands are runtime-kernel operations, independent of a specific strategy implementation.

## Commands

- `kehrnel core health` — check runtime health endpoint
- `kehrnel core api` — run the API server
- `kehrnel core env ...` — environment-scoped runtime operations (environment lifecycle, activation, query, ops)
- `kehrnel run ...` — preferred universal runtime executor (supports both runtime ops and strategy ops)

## Example

```bash
kehrnel core health
```

## Environment Operations

`kehrnel core env` is the CLI wrapper around the runtime admin endpoints under `/environments/{env_id}/...`.

Commands:

- `kehrnel core env list` — list environments
- `kehrnel core env show` — inspect one environment
- `kehrnel core env create` — create an environment shell
- `kehrnel core env update` — patch environment metadata or bindings reference
- `kehrnel core env delete` — delete an environment
- `kehrnel core env endpoints` — list which domains/strategies are active in an environment
- `kehrnel core env activate` — activate a strategy in an environment
- `kehrnel core env op` — run a strategy op (for example `ensure_dictionaries`)
- `kehrnel core env compile-query` — compile a query payload without executing it (openEHR AQL supported via `--aql` or `--aql-text`)
- `kehrnel core env query` — run a query payload (openEHR AQL supported via `--aql` or `--aql-text`)
- `kehrnel op capabilities --env <env>` — discover environment capabilities (`GET /environments/{env}/capabilities`)
- `kehrnel run <operation> ...` — execute via `POST /environments/{env}/run`

Typical flow:

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

# 1) Recommended: interactive setup (auth + context)
kehrnel setup --runtime-url "$RUNTIME_URL"
#
# Or, explicit primitives:
# kehrnel auth login --runtime-url "$RUNTIME_URL"
# kehrnel context set --env dev --domain openehr --strategy openehr.rps_dual

# 2) Create or inspect the environment
kehrnel core env create --env dev --name "Development"
kehrnel core env show --env dev

# 3) Activate
# Local dev/test:
cat > .kehrnel/bindings.mongo.yaml <<EOF
db:
  provider: mongodb
  uri: ${MONGODB_URI}
  database: openEHR_demo
EOF

kehrnel core env activate \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --allow-plaintext-bindings \
  --bindings .kehrnel/bindings.mongo.yaml

# Auth-enabled or resolver-backed deployments:
# kehrnel core env activate \
#   --env dev \
#   --domain openehr \
#   --strategy openehr.rps_dual \
#   --bindings-ref "<resolver-specific-ref>"

# 4) Run an op (optional)
kehrnel core env op ensure_dictionaries --env dev

# Preferred universal form:
# kehrnel run ensure_dictionaries --env dev --domain openehr
```

`kehrnel core env compile-query` and `kehrnel core env query` wrap the explicit runtime query endpoints. `compile-query --debug` asks the runtime to include extra compiler diagnostics in the response, but it still does not execute the query. For automation-heavy workflows, prefer `kehrnel run compile_query ...` and `kehrnel run query ...`, which keep the same environment contract as other runtime and strategy operations.

For a quick inline smoke test after activation:

```bash
kehrnel core env query \
  --env dev \
  --domain openehr \
  --aql-text "SELECT e/ehr_id/value AS ehr_id FROM EHR e LIMIT 5"
```

Run `kehrnel op capabilities --env dev` to see all standard runtime
capabilities and every operation contributed by the active strategy. The
output includes the exact CLI invocation for each capability.
