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

Kehrnel can keep multiple environments, domains, and strategies in its CLI
context. This walkthrough targets `dev` + `openehr` + `openehr.rps_dual`,
which is the strategy demonstrated by the packaged openEHR sample flow today.

The unified CLI is a client of the running Kehrnel runtime rather than a
standalone offline executor. That is why `kehrnel setup` needs
`--runtime-url`: it tells the CLI which runtime API to call.

```bash
kehrnel setup \
  --runtime-url "$RUNTIME_URL" \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual

kehrnel core health
kehrnel strategy list --domain openehr
kehrnel core env list
```

## 2. Create And Inspect The Environment

The environment is the unit of activation in Kehrnel. It isolates strategy
configuration, generated artifacts, and operational state.

Use `kehrnel core env show` to inspect the environment record and confirm which
metadata or `bindings_ref` values are currently stored for that environment.

```bash
kehrnel core env create --env dev --name "Development"
kehrnel core env show --env dev
```

## 3. Prepare Atlas Or MongoDB Target Collections

If your environment activation has working database bindings, Kehrnel creates
the required collections and standard B-tree indexes during activation. You can
skip manual shell setup unless you want to inspect or pre-create them yourself.

Generate the Atlas Search definition from Kehrnel later instead of maintaining
it by hand.

```bash
export MONGODB_URI="<your-atlas-connection-string>"
```

If you prefer to create them manually and `mongosh` is installed:

```javascript
use openEHR_demo

db.createCollection("compositions_rps")
db.createCollection("compositions_search")

db.compositions_rps.createIndex({ ehr_id: 1, v: 1 })
db.compositions_rps.createIndex({ ehr_id: 1, tid: 1, time_c: 1, comp_id: 1 })
db.compositions_rps.createIndex({ ehr_id: 1, "cn.p": 1, time_c: 1 })
db.compositions_search.createIndex({ ehr_id: 1, sort_time: 1 })
```

If `zsh` reports `command not found: mongosh`, use Atlas UI or MongoDB Compass,
or continue directly to activation and let Kehrnel materialize the artifacts.

## 4. Activate The Strategy

While multiple strategies may be available, select `openehr.rps_dual` for this
workflow.

When database adapters are available, activation is also the step that
materializes the configured collections and B-tree indexes for the strategy.

Activation needs database bindings. There are two supported patterns:

- local dev/test: pass a plaintext bindings file with
  `--allow-plaintext-bindings`
- auth-enabled or resolver-backed deployments: pass `--bindings-ref`

The strategy-owned bundle references come from
`src/kehrnel/engine/strategies/openehr/rps_dual/defaults.json` and resolve to:

- `bundles/dictionaries/_codes.json`
- `bundles/shortcuts/shortcuts.json`
- `bundles/searchIndex/searchIndex.json`

For the full packaged dual example, keep the defaults and add one config
overlay for the reference projection mappings. Without `transform.mappings`,
activation still works, but the slim `compositions_search` sidecar will not be
materialized with meaningful fields during ingest.

Local development example using `MONGODB_URI` directly:

```bash
mkdir -p .kehrnel

cat > .kehrnel/bindings.mongo.yaml <<EOF
db:
  provider: mongodb
  uri: ${MONGODB_URI}
  database: openEHR_demo
EOF

cat > .kehrnel/rps-dual.config.json <<EOF
{
  "transform": {
    "mappings": "file://samples/reference/projection_mappings.json"
  }
}
EOF

kehrnel core env activate \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --config .kehrnel/rps-dual.config.json \
  --allow-plaintext-bindings \
  --bindings .kehrnel/bindings.mongo.yaml
```

Resolver-backed deployment example:

```bash
kehrnel core env activate \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --bindings-ref "<resolver-specific-ref>"
```

For the built-in HDL resolver, valid examples include `env:dev`,
`hdl:env:dev`, and `hdl:env:dev:mongo:openEHR_demo`.

After activation, seed the code and shortcut dictionaries from the packaged
strategy bundles:

```bash
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

If you edit the strategy pack after activating the environment, later
`compile-query` or `query` calls may fail with
`ACTIVATION_STRATEGY_MISMATCH`. That means the environment is still activated
against an older manifest digest. Re-run activation for that environment before
continuing.

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

Use `kehrnel run ingest` here, not `kehrnel common ingest`. The `common ingest`
command is a low-level loader for documents that are already in the target
persistence shape, while this walkthrough starts from canonical openEHR
composition envelopes and needs the strategy to produce:

- the semi-flattened base document in `compositions_rps`
- the optional slim search projection in `compositions_search`

The local-file flags below are only a safety guard for server-side file access.
The CLI expands the packaged sample NDJSON from your working tree into
`documents=[...]` before posting to the runtime, so you do not need to enable
server-side local file access for this workflow.

```bash
kehrnel run ingest \
  --env dev \
  --domain openehr \
  --strategy openehr.rps_dual \
  --set file_path="$SAMPLES_ROOT/envelopes/all.ndjson"
```

When mappings exist for a template, Kehrnel also creates the optional
search-side document in `compositions_search`. If no mappings exist for a
template, Kehrnel skips that sidecar instead of creating empty arrays.

## 7. Compile Representative AQL

Compile the packaged AQL examples before you execute them.

This step is inspection-only: it compiles AQL into the runtime query plan and
returns the generated execution shape, but it does not run the query against
MongoDB.

Use `--debug` only when you want the compile response to include extra details
such as the bound AST, raw AQL, parameters, and compiler explain metadata.

Patient-scoped example:

```bash
kehrnel core env compile-query \
  --env dev \
  --domain openehr \
  --aql "$SAMPLES_ROOT/queries/patient_laboratory_by_ehr.aql"
```

Cross-patient example:

```bash
kehrnel core env compile-query \
  --env dev \
  --domain openehr \
  --aql "$SAMPLES_ROOT/queries/cross_patient_laboratory_by_performing_centre.aql"
```

Optional debug compile:

```bash
kehrnel core env compile-query \
  --env dev \
  --domain openehr \
  --aql "$SAMPLES_ROOT/queries/patient_laboratory_by_ehr.aql" \
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
  --payload .kehrnel/query.payload.json
```

## 8. Run Patient And Cross-Patient Queries

Use `query` when you want the runtime to both compile and execute the AQL and
return rows.

Execute both query families against the same environment:

```bash
kehrnel core env query \
  --env dev \
  --domain openehr \
  --aql "$SAMPLES_ROOT/queries/patient_laboratory_by_ehr.aql"

kehrnel core env query \
  --env dev \
  --domain openehr \
  --aql "$SAMPLES_ROOT/queries/cross_patient_laboratory_by_performing_centre.aql"
```

This is the core contract of the pattern: applications stay on AQL, while the
runtime selects the right physical execution path.

## 9. Inspect The Generated Artifacts

Look directly at the resulting MongoDB documents. Use `mongosh` if it is
installed, otherwise inspect the same collections in Atlas UI or MongoDB
Compass.

```bash
mongosh "$MONGODB_URI/openEHR_demo" <<'MONGOSH'
db.compositions_rps.findOne({}, { ehr_id: 1, tid: 1, time_c: 1, cn: { $slice: 3 } })
db.compositions_search.findOne({}, { ehr_id: 1, comp_id: 1, tid: 1, sort_time: 1, sn: 1 })
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
