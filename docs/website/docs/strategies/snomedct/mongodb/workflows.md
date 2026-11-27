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
    "version": "0.1.0",
    "domain": "snomedct",
    "config": {
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
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "snomedct",
    "operation": "op",
    "payload": {
      "op": "snomed_list_releases",
      "payload": {}
    }
  }'
```

## Inspect

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "snomedct",
    "operation": "op",
    "payload": {
      "op": "snomed_inspect_release",
      "payload": {
        "limit": 1000
      }
    }
  }'
```

Remove `limit` for the full file.

## Diff

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "snomedct",
    "operation": "op",
    "payload": {
      "op": "snomed_diff_release",
      "payload": {
        "release_id": "20260601",
        "sample_limit": 20
      }
    }
  }'
```

This streams the official file and compares canonical hashes against MongoDB.

## Ingest and Rebuild Sidecar

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "snomedct",
    "operation": "op",
    "payload": {
      "op": "snomed_ingest_release",
      "payload": {
        "release_id": "20260601",
        "rebuild_sidecar": true
      }
    }
  }'
```

For a first local smoke test, add `"limit": 1000`.

## Ensure Indexes

```bash
curl -sS -X POST "${RUNTIME_URL}/environments/dev/run" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "snomedct",
    "operation": "op",
    "payload": {
      "op": "snomed_ensure_indexes",
      "payload": {}
    }
  }'
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
    "limit": 20
  }'
```

Lookup:

```bash
curl -sS "${RUNTIME_URL}/api/domains/snomedct/concepts/73211009?release_id=20260601" \
  -H "x-active-env: dev"
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

Readiness:

```bash
curl -sS "${RUNTIME_URL}/api/domains/snomedct/readiness?release_id=20260601" \
  -H "x-active-env: dev"
```
