---
sidebar_position: 1
---

# CLI Overview

\{kehrnel\} now exposes a unified command model through:

- `kehrnel`

## Command Layers

- `kehrnel auth`  
Authenticate once and persist API key/runtime URL.
- `kehrnel context`  
Set active environment, domain, strategy, and optional runtime URL override.
- `kehrnel core`  
Runtime-kernel operations (health/API server entrypoint, plus environment-scoped runtime operations under `kehrnel core env`).
- `kehrnel common`  
Cross-domain workflows (`transform`, `ingest`, `validate`, `generate`, `map`, `identify`, `bundles`, `validate-pack`) executed under selected context.
- `kehrnel domain`  
Domain-scoped operations (`domain list`, `domain openehr ...`).
- `kehrnel strategy`  
Strategy discovery and selection.

## Typical Workflow

```bash
kehrnel auth login --runtime-url http://localhost:8000
kehrnel strategy list --domain openehr
kehrnel strategy use openehr.rps_dual --domain openehr
kehrnel common transform flatten ./composition.json -o flattened.json
```

## Getting Help

```bash
kehrnel --help
kehrnel auth --help
kehrnel common --help
```

## Environment Variables

Most commands respect the following environment variables when applicable:

| Variable | Description |
|----------|-------------|
| `CORE_MONGODB_URL` | MongoDB connection string |
| `CORE_DATABASE_NAME` | Default database name |
| `KEHRNEL_API_KEY` | Runtime API key (fallback when not stored in CLI state) |
| `KEHRNEL_RUNTIME_URL` | Runtime base URL (fallback when not stored in CLI state) |
| `KEHRNEL_API_HOST` | API server bind host |
| `KEHRNEL_API_PORT` | API server bind port |

See [Configuration](/docs/getting-started/configuration) for the complete list.

## Canonical Inventory

For exhaustive command and endpoint inventory generated from `pyproject.toml` and OpenAPI, see:

- [Canonical CLI + API Inventory (generated)](https://github.com/mongodb-industry-solutions/kehrnel/blob/main/docs/cli-api-reference.md)
