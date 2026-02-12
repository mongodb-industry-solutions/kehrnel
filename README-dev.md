# Kehrnel Dev Runbook

## Run the new runtime API

```bash
python --version  # 3.10+ required
python -m venv .venv && source .venv/bin/activate
pip install -e .
uvicorn kehrnel.api.app:app --reload
# docs at http://localhost:8000/docs
```

### Strategy discovery paths
- Strategies discovered under `src/kehrnel/engine/strategies` plus any paths in `KEHRNEL_STRATEGY_PATHS`.
- `KEHRNEL_STRATEGY_PATHS` supports `:` or `,` separators (e.g., `/path/a:/path/b`).
- Discovery fails fast on invalid JSON/duplicate IDs.
- Use `kehrnel-api` or `uvicorn kehrnel.api.app:app`; legacy entrypoints are not used by the active runtime.

## Tests

```bash
pytest tests/contract
```

- Contract/golden tests run without Mongo; they use fixtures/fake storage.
- Any e2e tests that need Mongo/Atlas must be guarded by env vars and should skip when missing.

## API endpoints HDL should call
- `GET /strategies`, `GET /strategies/{id}`
- `POST /environments/{env}/activate`
- `POST /environments/{env}/compile_query` (add `debug=true` for builder/scope/reason)
- `POST /environments/{env}/query`
- `POST /environments/{env}/extensions/{strategy}/{op}`

See `../HealthcareDataLab/docs/kehrnel-contracts/hdl-contract.md` for details.
