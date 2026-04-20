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
- `kehrnel core env compile-query` — compile a query payload (openEHR AQL supported via `--aql`)
- `kehrnel core env query` — run a query payload (openEHR AQL supported via `--aql`)
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

# 3) Activate (auth-enabled deployments require bindings_ref)
kehrnel core env activate --env dev --bindings-ref env://DB_BINDINGS

# 4) Run an op (optional)
kehrnel core env op ensure_dictionaries --env dev

# Preferred universal form:
# kehrnel run ensure_dictionaries --env dev --domain openehr
```

`kehrnel core env compile-query` and `kehrnel core env query` wrap the explicit runtime query endpoints. For automation-heavy workflows, prefer `kehrnel run compile_query ...` and `kehrnel run query ...`, which keep the same environment contract as other runtime and strategy operations.
