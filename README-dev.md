# Kehrnel Dev Runbook

## Run the new runtime API

```bash
python3 --version  # 3.10+ required
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
uvicorn kehrnel.api.app:app --reload
# Swagger: http://localhost:8000/docs
# ReDoc:   http://localhost:8000/redoc
# Site:    http://localhost:8000/guide
```

## Run / Build Docusaurus Docs

The runtime serves the **built** Docusaurus site from `docs/website/build` at `/guide`.

- Live docs dev server (separate port):

```bash
cd docs/website
npm install
# runs on http://localhost:8001/guide/
# API links (/docs, /redoc, /api, ...) are proxied to KEHRNEL_API_ORIGIN (default http://localhost:8000)
npm start
```

- Rebuild the static site for the runtime:

```bash
cd docs/website
npm run build
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
