---
sidebar_position: 3
---

# Core CLI Layer

`core` commands are runtime-kernel operations, independent of a specific strategy implementation.

## Commands

- `kehrnel core health` — check runtime health endpoint
- `kehrnel core api` — run the API server
- `kehrnel core env ...` — environment-scoped runtime operations (activate, query, ops)
- `kehrnel run ...` — preferred universal runtime executor (supports both runtime ops and strategy ops)

## Example

```bash
kehrnel core health
```

## Environment Operations

`kehrnel core env` is the CLI wrapper around the runtime admin endpoints under `/environments/{env_id}/...`.

Commands:

- `kehrnel core env endpoints` — list which domains/strategies are active in an environment
- `kehrnel core env activate` — activate a strategy in an environment
- `kehrnel core env op` — run a strategy op (for example `ensure_dictionaries`)
- `kehrnel core env compile-query` — compile a query payload (openEHR AQL supported via `--aql`)
- `kehrnel core env query` — run a query payload (openEHR AQL supported via `--aql`)
- `kehrnel op capabilities --env <env>` — discover environment capabilities (`GET /environments/{env}/capabilities`)
- `kehrnel run <operation> ...` — execute via `POST /environments/{env}/run`

Typical flow:

```bash
# 1) Recommended: interactive setup (auth + context)
kehrnel setup --runtime-url http://localhost:8000
#
# Or, explicit primitives:
# kehrnel auth login --runtime-url http://localhost:8000
# kehrnel context set --env dev --domain openehr --strategy openehr.rps_dual

# 2) Activate (auth-enabled deployments require bindings_ref)
kehrnel core env activate --bindings-ref env://DB_BINDINGS

# 3) Run an op (optional)
kehrnel core env op ensure_dictionaries

# Preferred universal form:
# kehrnel run ensure_dictionaries --env dev --domain openehr
```
