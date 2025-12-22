# Kehrnel Dev Runbook

## Run the new runtime API

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
uvicorn kehrnel.api.app:app --reload
# docs at http://localhost:8000/docs
```

### Strategy discovery paths
- Strategies discovered under `src/kehrnel/strategies` plus any paths in `KEHRNEL_STRATEGY_PATHS`.
- `KEHRNEL_STRATEGY_PATHS` supports `:` or `,` separators (e.g., `/path/a:/path/b`).
- Discovery fails fast on invalid JSON/duplicate IDs.
- Legacy FastAPI entrypoints under `src/api`/`src/app` are deprecated; do not run `kehrnel-api`/`python -m api.internal.api_server` for the new runtime.

## Tests

```bash
pytest tests/contract
```

- Contract/golden tests run without Mongo; they use fixtures/fake storage.
- Any e2e tests that need Mongo/Atlas must be guarded by env vars and should skip when missing.

## API endpoints HDL should call
- `GET /v1/strategies`, `GET /v1/strategies/{id}`
- `POST /v1/environments/{env}/activate`
- `POST /v1/environments/{env}/compile_query` (add `debug=true` for builder/scope/reason)
- `POST /v1/environments/{env}/query`
- `POST /v1/environments/{env}/extensions/{strategy}/{op}`

See `docs/hdl-contract.md` and `docs/hdl-migration-checklist.md` for details.
