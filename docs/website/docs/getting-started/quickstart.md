---
sidebar_position: 2
---

# Quick Start

This quick start uses the unified CLI and the runtime universal workflow endpoints.

If you want the full openEHR `openehr.rps_dual` example with packaged sample
data, projection mappings, generated Atlas Search definitions, and runnable AQL
examples, follow [RPS Dual CLI Workflows](/docs/strategies/openehr/rps-dual/cli-workflows).

## Prerequisites

- `{kehrnel}` installed ([Installation Guide](/docs/getting-started/installation))
- API server running locally
- Runtime auth configured (or disabled for local dev)

For local development:

- `./startKehrnel` serves the runtime on `http://localhost:8080`
- `kehrnel-api` serves on `http://localhost:8000` unless you override `KEHRNEL_API_PORT`

## 1) Configure CLI Context

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

kehrnel setup \
  --runtime-url "$RUNTIME_URL" \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual
```

Check connectivity:

```bash
kehrnel core health
```

If the environment does not exist yet:

```bash
kehrnel core env create --env dev --name "Development"
```

## 2) Activate Strategy In Environment

```bash
kehrnel core env activate \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --bindings-ref env://DB_BINDINGS
```

## 3) Generate And Validate A Composition (Template Flow)

```bash
kehrnel common generate -- -t samples/templates/T-IGR-TUMOUR-SUMMARY.opt -o .kehrnel/quickstart/composition.json --random
kehrnel common validate -- -c .kehrnel/quickstart/composition.json -t samples/templates/T-IGR-TUMOUR-SUMMARY.opt --stats
kehrnel common transform -- flatten .kehrnel/quickstart/composition.json -o .kehrnel/quickstart/flattened.json
```

## 4) Discover Runtime Capabilities

```bash
kehrnel op capabilities --env dev
kehrnel op schema synthetic_generate_batch --strategy openehr.rps_dual
```

## 5) Run A Strategy Operation (Universal Runner)

```bash
kehrnel run ensure_dictionaries --env dev --domain openehr
```

Dry-run synthetic generation:

```bash
kehrnel run synthetic_generate_batch \
  --env dev \
  --domain openehr \
  --set patient_count=50 \
  --set generation_mode=from_source \
  --set source_database=hc_openEHRCDR \
  --set source_collection=samples \
  --dry-run
```

## 6) Run Query

```bash
cat > .kehrnel/quickstart/query.aql <<'AQL'
SELECT c/uid/value AS uid
FROM EHR e
CONTAINS COMPOSITION c
LIMIT 5
AQL

cat > .kehrnel/quickstart/query.payload.json <<'JSON'
{
  "domain": "openehr",
  "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c LIMIT 5"
}
JSON

kehrnel run compile_query --env dev --domain openehr --payload .kehrnel/quickstart/query.payload.json
kehrnel run query --env dev --domain openehr --payload .kehrnel/quickstart/query.payload.json
```

Generate the Atlas Search index definition that matches the active mappings:

```bash
kehrnel strategy build-search-index \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --out .kehrnel/quickstart/search-index.json
```

## Full Workflow Script

Use the complete smoke workflow script:

```bash
examples/cli/full_workflow_console.sh
```

See [Full Workflow Test](/docs/getting-started/full-workflow-test) for required environment variables and expected outputs.
