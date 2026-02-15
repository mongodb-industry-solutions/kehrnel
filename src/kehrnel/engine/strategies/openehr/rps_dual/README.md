# openEHR `rps_dual` Manual

This runbook is specific to strategy `openehr/rps_dual` and follows the multi-strategy API layout.

## 0) Runtime + Docs

Start runtime:

```bash
uvicorn kehrnel.api.app:app --reload --host 0.0.0.0 --port 8000
```

Strategy docs:

- Swagger UI: `/docs/strategies/openehr/rps_dual`
- ReDoc: `/redoc/strategies/openehr/rps_dual`
- OpenAPI JSON: `/openapi/strategies/openehr/rps_dual.json`

Domain docs:

- Swagger UI: `/docs/domains/openehr`
- ReDoc: `/redoc/domains/openehr`

Core docs:

- Swagger UI: `/docs/core`
- ReDoc: `/redoc/core`

## 1) Generate 1 composition from template and save to disk (CLI)

```bash
kehrnel-generate \
  -t samples/templates/T-IGR-TUMOUR-SUMMARY.opt \
  -o samples/out/tumour/composition.json \
  --random
```

Notes:

- `kehrnel-generate` now creates missing output folders automatically.
- Validation warnings may appear; file is still written unless command fails.

Optional validation:

```bash
kehrnel-validate \
  -c samples/out/tumour/composition.json \
  -t samples/templates/T-IGR-TUMOUR-SUMMARY.opt
```

## 2) CLI path for DB ingestion (current supported flow)

Current CLI supports:

- ingest flattened NDJSON from file, or
- read canonical compositions from Mongo and flatten+write to target Mongo (`mongo-catchup`).

Strategy-local templates:

- `src/kehrnel/engine/strategies/openehr/rps_dual/ingest/config/mongo_source.template.json`
- `src/kehrnel/engine/strategies/openehr/rps_dual/ingest/config/mongo_driver.template.json`

For current data, source collection is `hc_openEHRCDR.samples` (canonical docs).

Run catch-up:

```bash
URI=$(sed -n 's/^MONGODB_URI=//p' .env | head -n 1 | sed 's/^"//; s/"$//')

cat > /tmp/rps_dual.mongo_source.runtime.json <<EOF
{
  "connection_string": "$URI",
  "database_name": "hc_openEHRCDR",
  "source_collection": "samples"
}
EOF

cat > /tmp/rps_dual.mongo_driver.runtime.json <<EOF
{
  "driver": "mongo",
  "connection_string": "$URI",
  "database_name": "hdl_user_test",
  "compositions_collection": "compositions_rps",
  "search_collection": "compositions_search"
}
EOF

kehrnel-ingest mongo-catchup \
  --src-cfg /tmp/rps_dual.mongo_source.runtime.json \
  --driver-cfg /tmp/rps_dual.mongo_driver.runtime.json \
  --limit 10
```

Where to verify in MongoDB:

- Base docs: `hdl_user_test.compositions_rps`
- Search docs: `hdl_user_test.compositions_search`

Important gap:

- CLI does not yet fetch template definitions directly from DB to generate new canonical compositions.
- If needed, generate canonical first (step 1) then ingest through API (`/ingest/body`) or add a dedicated CLI command later.

## 3) API path with strategy-scoped endpoints

Base prefix:

- `/api/strategies/openehr/rps_dual`

Endpoints in this strategy:

- `POST /api/strategies/openehr/rps_dual/config`
- `POST /api/strategies/openehr/rps_dual/ingest/body`
- `POST /api/strategies/openehr/rps_dual/ingest/file`
- `POST /api/strategies/openehr/rps_dual/ingest/database`
- `POST /api/strategies/openehr/rps_dual/synthetic/generate`
- `GET /api/strategies/openehr/rps_dual/synthetic/stats`

Environment-scoped batch synthetic jobs (core API, strategy-agnostic orchestration):

- `POST /environments/{env_id}/synthetic/jobs`
- `GET /environments/{env_id}/synthetic/jobs`
- `GET /environments/{env_id}/synthetic/jobs/{job_id}`
- `POST /environments/{env_id}/synthetic/jobs/{job_id}/cancel`

For `openehr/rps_dual`, use `op=synthetic_generate_batch`.

Preferred payload (model catalog):

```json
{
  "patient_count": 1000,
  "generation_mode": "from_models",
  "store_canonical": true,
  "canonical_collection": "compositions_canonical_synthetic",
  "model_source": {
    "database_name": "hdl-team",
    "catalog_collection": "user-data-models",
    "links_collection": "semantic_links"
  },
  "models": [
    {
      "model_id": "opt.tumour_summary.v1",
      "min_per_patient": 1,
      "max_per_patient": 2,
      "sample_pool_size": 25
    }
  ],
  "links": [
    {
      "from": "opt.tumour_summary.v1",
      "to": "opt.followup.v1",
      "probability": 0.7,
      "min_to_per_patient": 1
    }
  ]
}
```

Compatibility payload still supported:

```json
{
  "patient_count": 1000,
  "source_collection": "samples",
  "templates": [
    { "template_id": "T-IGR-TUMOUR-SUMMARY", "min_per_patient": 1, "max_per_patient": 2 }
  ]
}
```

Source-instance payload (no model catalog; derive templates from existing data):

```json
{
  "patient_count": 1000,
  "generation_mode": "from_source",
  "source_collection": "samples",
  "source_database": "hdl-team-openehr",
  "source_sample_size": 200,
  "source_min_per_patient": 1,
  "source_max_per_patient": 2,
  "source_filter": { "domain": "openehr" }
}
```

Optional:
- `source_templates`: explicit template IDs to use instead of auto-discovery.

Optional flags:

- `dry_run=true`: run generation logic but skip inserts.
- `plan_only=true`: validate and return estimated plan without generating docs.
- `generation_mode=from_models`: generate canonical documents directly from model definitions (no `samples` collection required).
- `store_canonical=true`: also persist canonical generated compositions.
- `canonical_collection`: target collection for canonical docs when `store_canonical=true` (default: `compositions_canonical_synthetic`).
- `plan_only` also returns `estimated_canonical_bytes`, `estimated_base_bytes`, `estimated_search_bytes`, and `estimated_total_bytes` for capacity planning.
- generation result/progress includes `inserted_canonical` in addition to transformed document counters.

Security best practice:

- Use API key auth (`X-API-Key`) with `KEHRNEL_AUTH_ENABLED=true` and `KEHRNEL_API_KEYS=...`.
- Keep Mongo connection string server-side; do not send raw URIs from browser clients.
- If UI session auth is required, place auth in your gateway/backend and call these endpoints from that trusted backend.
- For HDL multi-environment (DEV/PROD): use `bindings_ref` in activation and configure `KEHRNEL_BINDINGS_RESOLVER=module:function` so Kehrnel resolves encrypted secrets server-side.

Production hardening (required):

- `KEHRNEL_AUTH_ENABLED=true`
- `KEHRNEL_API_KEYS` must be configured and non-empty
- `KEHRNEL_ALLOW_LOCAL_FILE_INPUTS=false`
- `KEHRNEL_ALLOW_ABSOLUTE_CONFIG_PATHS=false`

### HDL resolver setup (Kehrnel side)

Use built-in resolver:

```bash
export KEHRNEL_BINDINGS_RESOLVER="kehrnel.integrations.hdl.bindings_resolver:resolve_hdl_bindings"
export CORE_MONGODB_URL="mongodb+srv://..."
export CORE_DATABASE_NAME="hdl_core"
export ENV_SECRETS_KEY="base64_32_byte_key"
```

Supported `bindings_ref` formats:

- `hdl:env:<env_id>`
- `hdl:env:<env_id>:mongo`
- `hdl:env:<env_id>:mongo:<database_name>`

Notes:

- Resolver reads `environment_secrets` by `envId` and decrypts `sealedUri` (AES-256-GCM).
- If database name is not in the URI or ref, resolver tries environment metadata (`environments[].database`) in collections `teams,users,workspaces`.
