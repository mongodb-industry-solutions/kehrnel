# kehrnel

`kehrnel` is a Python runtime for strategy packs, with:
- Strategy-pack API (`FastAPI`)
- Runtime/activation engine
- CLI tooling for mapping, validation, ingest, transform, and pack validation

## Active Scope

This repository is intentionally focused on:
- `src/kehrnel/api` (API surface)
  - includes `src/kehrnel/api/compatibility` compatibility modules still used by current domain routes
- `src/kehrnel/engine` (core/common/domains/strategies)
- `src/kehrnel/cli` (CLI commands)
- `samples/` and `tests/`

Removed from active scope:
- old standalone frontend
- old non-package API tree (`src/api`)
- old app entrypoint tree (`src/app`)

## Quick Start

```bash
git clone <repo>
cd kehrnel
python3 --version  # 3.10+ required
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[all]
# Build docs site (optional but recommended if you want /guide on port 8000)
cd docs/website && npm install && npm run build && cd ../..
uvicorn kehrnel.api.app:app --reload
```

API docs:
- `http://localhost:8000/docs`
- `http://localhost:8000/redoc`
- `http://localhost:8000/guide` (Docusaurus site, if built)

## Documentation Serving Model

Kehrnel serves all API/docs surfaces from the same API server port (default `8000`):

- Swagger UI: `/docs`
- ReDoc: `/redoc`
- Docusaurus site: `/guide` (served from `docs/website/build`)

Notes:
- If `docs/website/build` does not exist, `/guide` will show a “documentation is not built” message.
- During docs authoring you can also run the Docusaurus dev server separately on `8001`:

```bash
cd docs/website
npm start
```

In dev mode, API links are proxied to `KEHRNEL_API_ORIGIN` (default `http://localhost:8000`).

Full integration guide:
- `examples/README.md`
- `docs/cli-api-reference.md`

## Runtime Endpoints Used by HDL

- `GET /strategies`
- `GET /strategies/{id}`
- `POST /environments/{env}/activate`
- `GET /environments/{env}/capabilities`
- `POST /environments/{env}/run`
- `POST /environments/{env}/compile_query`
- `POST /environments/{env}/query`
- `POST /environments/{env}/activations/{domain}/ops/{op}`

Detailed contract docs:
- this README (standalone and integration model)

## Strategy Packs

Built-in strategy packs live under:
- `src/kehrnel/engine/strategies`

Additional packs can be discovered with:
- `KEHRNEL_STRATEGY_PATHS=/path/a:/path/b`

Validate a pack:

```bash
kehrnel common validate-pack /path/to/strategy-pack
```

## CLI

Primary CLI entrypoint:
- `kehrnel` (`auth`, `context`, `resource`, `op`, `run`, `core`, `common`, `domain`, `strategy`)
- `kehrnel-api` (API server launcher)

Complete CLI + endpoint inventory:
- `docs/cli-api-reference.md`

## Standalone Usage

Kehrnel can be used independently of Healthcare Data Lab as:
- a Python runtime library (embed in your backend),
- a CLI toolkit (scripts/CI),
- an HTTP API service (for external applications).

## Runtime Architecture

```mermaid
flowchart LR
  APP[External Application] -->|HTTP| API[Kehrnel API]
  APP -->|Python SDK| RT[StrategyRuntime]
  APP -->|CLI| CLI[Kehrnel CLI]

  API --> RT
  CLI --> RT

  RT --> REG[Activation Registry]
  RT --> DISC[Strategy Discovery]
  DISC --> PACKS[Strategy Packs]
  RT --> RES[Bindings Resolver]
  RES --> SECRETS[Secret Store]

  RT --> PLUG[Strategy Plugin]
  PLUG --> OPS[Ops / Transform / Ingest / Query]
  OPS --> MONGO[(MongoDB)]
```

Execution contract:
1. Discover strategy manifests.
2. Activate environment (`env_id + domain + strategy + config + bindings_ref`).
3. Dispatch capability (`compile_query`, `query`, `ingest`, `transform`, `op`, etc.).
4. Strategy plugin executes with resolved bindings and strategy config.

## API Integration Model

1. Discover strategies:
- `GET /strategies`
- `GET /strategies/{strategy_id}`

2. Activate an environment:
- `POST /environments/{env_id}/activate`

Activation binds:
- `strategy_id`
- `domain`
- strategy `config`
- secure `bindings_ref` (recommended)

3. Execute by environment:
- `POST /environments/{env_id}/compile_query`
- `POST /environments/{env_id}/query`
- `POST /environments/{env_id}/ingest`
- `POST /environments/{env_id}/transform`
- `POST /environments/{env_id}/apply`
- `GET /environments/{env_id}/capabilities`
- `POST /environments/{env_id}/run`
- `POST /environments/{env_id}/activations/{domain}/ops/{op}`

4. Strategy-specific APIs (example):
- `/api/strategies/openehr/rps_dual/*`

Clinical domain APIs:
- `/api/domains/openehr/*`

## Security Baseline

For public deployment, set these before exposure:
- `KEHRNEL_AUTH_ENABLED=true`
- `KEHRNEL_API_KEYS=<comma-separated-keys>`
- `KEHRNEL_CORS_ORIGINS=<explicit-origins>` (avoid `*` in production)
- `KEHRNEL_RATE_LIMIT=<requests/minute>`

For secure database binding resolution:
- `KEHRNEL_BINDINGS_RESOLVER=<module:function>`
- prefer `bindings_ref` over plaintext bindings

## Examples

- Python embedding: `examples/sdk/runtime_embed_example.py`
- HTTP flow: `examples/api/curl_flow.sh`
- CLI skeleton: `examples/cli/pipeline.sh`
- Full CLI workflow smoke: `examples/cli/full_workflow_console.sh`

## Tests

```bash
pytest tests/contract
```

Notes:
- Contract/golden tests target the active strategy runtime.
- Some tests still exercise compatibility routes while API/domain migration is completed.

## License

Code is Apache 2.0 (`LICENSE`).

Strategy data assets under `src/kehrnel/engine/strategies/` are CC BY 4.0 (see `src/kehrnel/engine/strategies/LICENSE`).
