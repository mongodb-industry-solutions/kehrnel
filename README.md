# kehrnel

`kehrnel` is a Python runtime for strategy packs, with:
- Strategy-pack API (`FastAPI`)
- Runtime/activation engine
- CLI tooling for mapping, validation, ingest, transform, and pack validation

## Active Scope

This repository is intentionally focused on:
- `src/kehrnel/api` (API surface)
- `src/kehrnel/engine` (core/common/domains/strategies)
- `src/kehrnel/legacy/cli` (CLI commands still in use)
- `samples/` and `tests/`

Removed from active scope:
- old standalone frontend
- old non-package API tree (`src/api`)
- old app entrypoint tree (`src/app`)

## Quick Start

```bash
git clone <repo>
cd kehrnel
python --version  # 3.10+ required
python -m venv .venv
source .venv/bin/activate
pip install -e .[all]
uvicorn kehrnel.api.app:app --reload
```

API docs:
- `http://localhost:8000/docs`
- `http://localhost:8000/redoc`

## Runtime Endpoints Used by HDL

- `GET /strategies`
- `GET /strategies/{id}`
- `POST /environments/{env}/activate`
- `POST /environments/{env}/compile_query`
- `POST /environments/{env}/query`
- `POST /environments/{env}/extensions/{strategy}/{op}`

Detailed contract docs:
- `../HealthcareDataLab/docs/kehrnel-contracts/hdl-contract.md`
- `../HealthcareDataLab/docs/kehrnel-contracts/hdl-kehrnel-synthetic-contract-v2.md`

## Strategy Packs

Built-in strategy packs live under:
- `src/kehrnel/engine/strategies`

Additional packs can be discovered with:
- `KEHRNEL_STRATEGY_PATHS=/path/a:/path/b`

Validate a pack:

```bash
kehrnel-validate-pack /path/to/strategy-pack
```

## CLI

Installed commands include:
- `kehrnel-api`
- `kehrnel-map`
- `kehrnel-generate`
- `kehrnel-validate`
- `kehrnel-ingest`
- `kehrnel-transform`
- `kehrnel-identify`
- bundle/pack helpers (`kehrnel-validate-pack`, `kehrnel-list-bundles`, etc.)

## Tests

```bash
pytest tests/contract
```

Notes:
- Contract/golden tests target the active strategy runtime.
- Legacy v1 tests are retained in the tree for historical coverage and may be skipped when legacy app modules are absent.

## License

Code is Apache 2.0 (`LICENSE`).

Strategy data assets under `src/kehrnel/engine/strategies/` are CC BY 4.0 (see `src/kehrnel/engine/strategies/LICENSE`).
