---
sidebar_position: 3
---

# SNOMED CT Workflows and API

End-to-end path for `snomedct.mongodb`.

1. Activate the strategy on an environment.
2. Obtain the licensed official JSON release through the appropriate SNOMED CT licensing channel.
3. Place the JSON release file in the configured local folder.
4. Inspect the file.
5. Diff it against the current MongoDB canonical collection.
6. Ingest canonical concepts.
7. Rebuild the term sidecar.
8. Ensure indexes.
9. Query through the domain API or universal runtime.

## Activate

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

curl -sS -X POST "${RUNTIME_URL}/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": "snomedct.mongodb",
    "version": "0.5.0",
    "domain": "snomedct",
    "config": {
      "database": "snomedct",
      "release": { "id": "20260601" },
      "source": {
        "local_dir": ".kehrnel/snomedct/releases",
        "file_name": "edicion_20260601.json"
      }
    },
    "bindings": {
      "db": {
        "provider": "mongodb",
        "uri": "mongodb://localhost:27017",
        "database": "snomedct"
      }
    },
    "allow_plaintext_bindings": true
  }'
```

## Stage and List the Licensed Release

Kehrnel does not distribute SNOMED CT content. The customer obtains the official JSON release through their licensed channel and places the file in `source.local_dir`.

```bash
mkdir -p .kehrnel/snomedct/releases
# Place edicion_20260601.json in .kehrnel/snomedct/releases/
```

List staged release files:

```bash
curl -sS "${RUNTIME_URL}/api/domains/snomedct/releases" \
  -H "x-active-env: dev"

kehrnel terminology snomed-releases --env dev
```

Only files inside the activated `source.local_dir` are visible to these
operations. Runtime requests cannot override or escape that staging root.

## Inspect

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/releases/inspect" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{"file_name":"edicion_20260601.json","limit":1000}'

kehrnel terminology inspect-snomed-release \
  --env dev --file-name edicion_20260601.json --limit 1000
```

Remove `limit` for the full file.

## Diff

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/releases/diff" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{"file_name":"edicion_20260601.json","release_id":"20260601","sample_limit":20}'

kehrnel terminology diff-snomed-release \
  --env dev --release-id 20260601 --file-name edicion_20260601.json
```

This streams the official file and compares canonical hashes against MongoDB.

## Ingest and Rebuild Sidecar

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/releases/ingest" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{"file_name":"edicion_20260601.json","release_id":"20260601","rebuild_sidecar":true,"dry_run":false,"license_acknowledged":true}'

# Safe default is a dry run. Add --execute to persist.
kehrnel terminology ingest-snomed-release \
  --env dev --release-id 20260601 --file-name edicion_20260601.json

# Persistence additionally requires both flags.
kehrnel terminology ingest-snomed-release \
  --env dev --release-id 20260601 --file-name edicion_20260601.json \
  --execute --license-acknowledged
```

For a first local smoke test, add `"limit": 1000`.

## Ensure Indexes

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/indexes/ensure" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{"dry_run":false}'

# Safe default previews the plan. Add --execute to create indexes.
kehrnel terminology ensure-snomed-indexes --env dev
```

## Domain API

Pass the active environment through `x-active-env`.

Search:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/search" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "q": "diabetes mellitus",
    "language": "en",
    "release_id": "20260601",
    "offset": 0,
    "limit": 20
  }'
```

Lookup:

```bash
curl -sS "${RUNTIME_URL}/api/domains/snomedct/concepts/73211009?release_id=20260601" \
  -H "x-active-env: dev"
```

Typeahead and cross-release history:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/suggest" \
  -H "Content-Type: application/json" -H "x-active-env: dev" \
  -d '{"q":"myocard","language":"en","limit":8}'

curl -sS "${RUNTIME_URL}/api/domains/snomedct/concepts/73211009/history?language=en&offset=0&limit=20" \
  -H "x-active-env: dev"
```

Validate a code and display:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/validate-code" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "code": "73211009",
    "display": "Diabetes mellitus",
    "language": "en"
  }'
```

Check subsumption using the materialized inferred hierarchy:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/subsumes" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "code_a": "404684003",
    "code_b": "73211009"
  }'
```

Basic ECL:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/ecl" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "expression": "<< 73211009",
    "release_id": "20260601",
    "limit": 50
  }'
```

Compile ECL without executing:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/ecl/compile" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "expression": "< 404684003 : 363698007 = 39057004",
    "release_id": "20260601",
    "limit": 50
  }'
```

Expand a value set:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/expand" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "expression": "<< 73211009",
    "release_id": "20260601",
    "limit": 50
  }'
```

Register a reusable, immutable ValueSet version and then expand it by canonical
URL. The definition is tenant-owned metadata; the licensed concepts remain in
the canonical release collection.

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/value-sets" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "url": "https://example.org/ValueSet/diabetes",
    "version": "1.0.0",
    "name": "Diabetes disorders",
    "release_id": "20260601",
    "expression": "<< 73211009"
  }'

curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/expand" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "url": "https://example.org/ValueSet/diabetes",
    "value_set_version": "1.0.0",
    "limit": 50
  }'
```

Navigate hierarchy:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/concepts/73211009/descendants" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "include_self": true,
    "release_id": "20260601",
    "limit": 50
  }'
```

Find concepts by exact relationship:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/relationships/search" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "type_id": "363698007",
    "destination_id": "39057004",
    "release_id": "20260601",
    "limit": 50
  }'
```

Facet terminology search:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/semantic-facets" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "q": "diabetes",
    "language": "en",
    "release_id": "20260601"
  }'
```

Ground extracted mentions:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/ground" \
  -H "Content-Type: application/json" \
  -H "x-active-env: dev" \
  -d '{
    "mentions": ["type 2 diabetes mellitus", "diabetic nephropathy"],
    "language": "en",
    "release_id": "20260601",
    "limit_per_mention": 5
  }'
```

Persist reviewer decisions and query the reviewed corpus. The source text is
hashed and discarded unless the activation explicitly enables retention.

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/ground/reviews" \
  -H "Content-Type: application/json" -H "x-active-env: dev" \
  -d '{
    "source_ref":"note-001",
    "text":"Patient has diabetes mellitus",
    "language":"en",
    "codings":[{"concept_id":"73211009","status":"accepted","evidence":"diabetes mellitus"}]
  }'

curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/ground/corpus/search" \
  -H "Content-Type: application/json" -H "x-active-env: dev" \
  -d '{"concept_id":"404684003","offset":0,"limit":20}'
```

Run a bounded retrieval benchmark:

```bash
curl -sS -X POST "${RUNTIME_URL}/api/domains/snomedct/benchmarks/retrieval" \
  -H "Content-Type: application/json" -H "x-active-env: dev" \
  -d '{
    "language":"en",
    "top_k":10,
    "cases":[{"id":"diabetes","query":"diabetes mellitus","expected_concept_ids":["73211009"]}]
  }'
```

Readiness:

```bash
curl -sS "${RUNTIME_URL}/api/domains/snomedct/readiness?release_id=20260601" \
  -H "x-active-env: dev"
```
