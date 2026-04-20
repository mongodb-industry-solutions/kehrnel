---
sidebar_position: 2
---

# openEHR RPS Dual CLI Workflows

Use this page as the end-to-end example path for `openehr.rps_dual`.

It mirrors the practical build flow behind the solution:

1. start the runtime
2. create an environment
3. activate the strategy
4. inspect the packaged sample assets
5. ingest canonical sample compositions
6. generate the Atlas Search definition from the active mappings
7. compile representative AQL
8. run the same queries
9. inspect the generated base and search-side documents

If you are writing a high-level guide such as a Solution Library Section 4,
this is the best Docusaurus page to deep-link.

## Prerequisites

Start Kehrnel with the recommended local entrypoint:

```bash
./startKehrnel
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"
```

The strategy-owned neutral sample pack lives under:

```bash
SAMPLES_ROOT="src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference"
```

It includes:

- canonical masked compositions under `canonical/`
- ingest-ready NDJSON envelopes under `envelopes/`
- neutral OPTs under `templates/`
- projection mappings in `projection_mappings.json`
- generated Atlas Search seed in `search_index.definition.json`
- runnable sample AQL files under `queries/`

## 1. Configure The CLI Context

```bash
kehrnel setup \
  --runtime-url "$RUNTIME_URL" \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual

kehrnel core health
kehrnel core env list
```

## 2. Create And Inspect The Environment

The environment is the unit of activation in Kehrnel. It isolates strategy
configuration, generated artifacts, and operational state.

```bash
kehrnel core env create --env dev --name "Development"
kehrnel core env show --env dev
kehrnel strategy list --domain openehr
```

## 3. Prepare Atlas Or MongoDB Target Collections

Create the main semi-flattened collection first. Add the optional
search-side collection when you want the dual-collection workflow.

Generate the Atlas Search definition from Kehrnel later instead of maintaining
it by hand.

```bash
export MONGODB_URI="<your-atlas-connection-string>"

mongosh "$MONGODB_URI" <<'MONGOSH'
use openEHR_demo

db.createCollection("compositions_rps")
db.createCollection("compositions_search")

db.compositions_rps.createIndex({ ehr_id: 1, tid: 1, time_c: 1 })
db.compositions_rps.createIndex({ "cn.p": 1 })
MONGOSH
```

## 4. Activate The Strategy

While multiple strategies may be available, select `openehr.rps_dual` for this
workflow.

```bash
kehrnel core env activate \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --bindings-ref env://DB_BINDINGS

kehrnel run ensure_dictionaries --env dev --domain openehr
```

Generate the Atlas Search definition that matches the current mappings:

```bash
kehrnel strategy build-search-index \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --out .kehrnel/search-index.json
```

Optional sanity check against the packaged sample seed:

```bash
diff -u "$SAMPLES_ROOT/search_index.definition.json" .kehrnel/search-index.json || true
```

## 5. Explore Templates, Mappings, And Packaged Samples

Use OPTs and canonical compositions as the source inputs. Use the projection
mapping file when you want the search-side collection to materialize only the
configured analytics fields.

```bash
ls "$SAMPLES_ROOT/templates"
ls "$SAMPLES_ROOT/queries"
ls "$SAMPLES_ROOT/projection_mappings.json" "$SAMPLES_ROOT/search_index.definition.json"
```

If you want to create your own canonical composition and mapping skeleton:

```bash
kehrnel common generate -- -- \
  -t "$SAMPLES_ROOT/templates/sample_laboratory_v0_4.opt" \
  -o .kehrnel/composition.json \
  --random

kehrnel common validate -- -- \
  -c .kehrnel/composition.json \
  -t "$SAMPLES_ROOT/templates/sample_laboratory_v0_4.opt" \
  --stats

kehrnel common map-skeleton -- -- \
  "$SAMPLES_ROOT/templates/sample_laboratory_v0_4.opt" \
  -o .kehrnel/mapping.skeleton.yaml \
  --macros
```

## 6. Ingest Canonical Compositions

Ingest through the strategy instead of writing flattened documents directly.

The NDJSON envelopes are ingest-ready wrappers. Each line contains the masked
canonical composition plus `ehr_id`, `template_id`, `composition_version`, and
`time_committed`.

```bash
python -m kehrnel.cli.ingest init-driver \
  --db openEHR_demo \
  --out .kehrnel/driver.mongo.yaml

kehrnel common ingest --strategy openehr.rps_dual --domain openehr -- \
  file "$SAMPLES_ROOT/envelopes/all.ndjson" \
  -d .kehrnel/driver.mongo.yaml \
  --workers 4
```

When mappings exist for a template, Kehrnel also creates the optional
search-side document in `compositions_search`. If no mappings exist, it skips
that sidecar instead of creating empty arrays.

## 7. Compile Representative AQL

Compile the packaged AQL examples before you execute them.

Patient-scoped example:

```bash
kehrnel core env compile-query \
  --env dev \
  --domain openehr \
  --aql "$SAMPLES_ROOT/queries/patient_laboratory_by_ehr.aql" \
  --debug
```

Cross-patient example:

```bash
kehrnel core env compile-query \
  --env dev \
  --domain openehr \
  --aql "$SAMPLES_ROOT/queries/cross_patient_laboratory_by_performing_centre.aql" \
  --debug
```

You can also compile through the universal runner if you prefer payload files:

```bash
cat > .kehrnel/query.payload.json <<'JSON'
{
  "domain": "openehr",
  "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c LIMIT 5"
}
JSON

kehrnel run compile_query \
  --env dev \
  --domain openehr \
  --payload .kehrnel/query.payload.json \
  --debug
```

## 8. Run Patient And Cross-Patient Queries

Execute both query families against the same environment.

```bash
kehrnel core env query \
  --env dev \
  --domain openehr \
  --aql "$SAMPLES_ROOT/queries/patient_laboratory_by_ehr.aql"

kehrnel core env query \
  --env dev \
  --domain openehr \
  --aql "$SAMPLES_ROOT/queries/cross_patient_immunization_by_publishing_centre.aql"
```

This is the core contract of the pattern: applications stay on AQL, while the
runtime selects the right physical execution path.

## 9. Inspect The Generated Artifacts

Look directly at the resulting MongoDB documents.

```bash
mongosh "$MONGODB_URI/openEHR_demo" <<'MONGOSH'
db.compositions_rps.findOne({}, { ehr_id: 1, tid: 1, time_c: 1, cn: { $slice: 3 } })
db.compositions_search.findOne({}, { ehr_id: 1, tid: 1, sort_time: 1, sn: 1 })
MONGOSH
```

This is where the document-first design becomes tangible:

- canonical compositions remain the source input
- the semi-flattened base document becomes the operational unit
- the search-side projection appears only when mappings require it
- the Atlas Search definition is derived from mappings instead of being managed separately

## 10. Continue Through The CLI

Once the base flow works, stay on the CLI for strategy operations rather than
jumping straight into custom API wiring.

Useful next commands:

```bash
kehrnel op capabilities --env dev
kehrnel op schema synthetic_generate_batch --strategy openehr.rps_dual
kehrnel run rebuild_codes --env dev --domain openehr
kehrnel run rebuild_shortcuts --env dev --domain openehr
kehrnel run build_search_index_definition --env dev --domain openehr --strategy openehr.rps_dual
```

## Related Pages

- [Quick Start](/docs/getting-started/quickstart)
- [RPS Dual Configuration](/docs/strategies/openehr/rps-dual/configuration)
- [RPS Dual Data Model](/docs/strategies/openehr/rps-dual/data-model)
- [RPS Dual Query Translation](/docs/strategies/openehr/rps-dual/query-translation)
