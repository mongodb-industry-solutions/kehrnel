---
sidebar_position: 4
---

# RPS Dual Configuration

Complete configuration reference for the openEHR RPS Dual strategy.

## Configuration Structure

```json
{
  "database": "kehrnel_db",
  "collections": {
    "compositions": {
      "name": "compositions_rps",
      "encodingProfile": "profile.codedpath"
    },
    "search": {
      "name": "compositions_search",
      "encodingProfile": "profile.search_shortcuts",
      "enabled": true,
      "atlasIndex": {
        "name": "search_nodes_index",
        "definition": "file://bundles/searchIndex/searchIndex.json"
      }
    },
    "codes": {
      "name": "_codes",
      "seed": "file://bundles/dictionaries/_codes.json"
    },
    "shortcuts": {
      "name": "_shortcuts",
      "seed": "file://bundles/shortcuts/shortcuts.json"
    }
  },
  "fields": {
    "document": {
      "ehr_id": "ehr_id",
      "comp_id": "comp_id",
      "tid": "tid",
      "v": "v",
      "time_committed": "time_c",
      "sort_time": "sort_time",
      "cn": "cn",
      "sn": "sn"
    }
  },
  "transform": {
    "apply_shortcuts": true,
    "coding": {
      "arcodes": {
        "strategy": "sequential"
      },
      "atcodes": {
        "strategy": "negative_int"
      }
    }
  }
}
```

## Configuration Reference

### Database

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database` | string | `kehrnel_db` | MongoDB database name |

### Collections

#### compositions

Primary storage for flattened compositions.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `compositions_rps` | Collection name |
| `encodingProfile` | string | `profile.codedpath` | Encoding profile to use |

#### search

Search-optimized projection collection.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `compositions_search` | Collection name |
| `enabled` | boolean | `true` | Enable search collection |
| `atlasIndex.name` | string | `search_nodes_index` | Atlas Search index name |

#### codes

Code dictionary collection.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `_codes` | Collection name |
| `seed` | string\|object | none | Optional seed payload (supports `file://...` URIs relative to the strategy pack) |

#### shortcuts

Shortcuts dictionary collection (used to compress common strings in documents).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `_shortcuts` | Collection name |
| `seed` | string\|object | none | Optional seed payload (supports `file://...` URIs relative to the strategy pack) |

### Transform

#### coding.arcodes

Archetype ID encoding strategy.

| Strategy | Description |
|----------|-------------|
| `sequential` | Assign sequential integers (1, 2, 3...) |
| `literal` | Keep original archetype ID strings |

#### coding.atcodes

AT-code encoding strategy.

| Strategy | Description |
|----------|-------------|
| `negative_int` | Encode as negative integers (at0004 → -4) |
| `literal` | Keep original at-code strings |

## Encoding Profiles

### profile.codedpath

The default encoding profile with full path encoding:

- Reversed paths with numeric segments
- AT-codes as negative integers
- Archetype IDs as sequential integers

### profile.literal

Debug profile with human-readable paths:

- Original path structure
- String codes preserved
- Larger storage footprint

## Atlas Search Index

The Atlas Search definition for `compositions_search` is generated from the active analytics mappings and strategy field configuration.

Recommended workflow:

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

kehrnel setup --runtime-url "$RUNTIME_URL" --env dev --domain openehr --strategy openehr.rps_dual
kehrnel strategy build-search-index --env dev --domain openehr --strategy openehr.rps_dual --out .kehrnel/search-index.json
```

The generated definition is aligned with the active mappings and currently includes:

- root metadata such as `ehr_id`, `tid`, and `sort_time`
- `sn.p` as the indexed path field
- only the mapped `sn.data.*` fields needed for search/analytics

The packaged sample dataset also includes a self-contained pair of example
artifacts under `samples/reference/`:

- `projection_mappings.json`
- `search_index.definition.json`

`collections.search.atlasIndex.definition` may still be provided as a `file://...` URI (relative to the strategy pack), but it should be treated as a seed artifact derived from the same mappings-driven workflow.

## Example Configurations

### Development

```json
{
  "database": "dev_cdr",
  "collections": {
    "compositions": { "name": "compositions" },
    "search": { "name": "search", "encodingProfile": "profile.search_shortcuts", "enabled": true },
    "codes": { "name": "_codes" },
    "shortcuts": { "name": "_shortcuts" }
  },
  "transform": {
    "apply_shortcuts": true,
    "coding": {
      "arcodes": { "strategy": "literal" },
      "atcodes": { "strategy": "literal" }
    }
  }
}
```

### Production

```json
{
  "database": "prod_cdr",
  "collections": {
    "compositions": {
      "name": "compositions_rps",
      "encodingProfile": "profile.codedpath"
    },
    "search": {
      "name": "compositions_search",
      "encodingProfile": "profile.search_shortcuts",
      "enabled": true,
      "atlasIndex": {
        "name": "search_nodes_index",
        "definition": "file://bundles/searchIndex/searchIndex.json"
      }
    },
    "codes": { "name": "_codes", "seed": "file://bundles/dictionaries/_codes.json" },
    "shortcuts": { "name": "_shortcuts", "seed": "file://bundles/shortcuts/shortcuts.json" }
  },
  "transform": {
    "apply_shortcuts": true,
    "coding": {
      "arcodes": { "strategy": "sequential" },
      "atcodes": { "strategy": "negative_int" }
    }
  }
}
```

### Search Disabled

For patient-only workloads without cross-patient queries:

```json
{
  "database": "patient_cdr",
  "collections": {
    "compositions": { "name": "compositions_rps" },
    "search": { "enabled": false }
  }
}
```

## Activation Example

```bash
RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

curl -X POST "${RUNTIME_URL}/environments/production/activate" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: admin-key" \
  -d '{
    "strategy_id": "openehr.rps_dual",
    "version": "0.2.0",
    "domain": "openehr",
    "config": {
      "database": "prod_cdr",
      "collections": {
        "compositions": { "name": "compositions_rps" },
        "search": { "name": "compositions_search", "enabled": true }
      }
    },
    "bindings_ref": "env://PROD_BINDINGS"
  }'
```

## Notes On Seeds And Bundles

RPS Dual uses `file://...` URIs in config for local pack assets, for example:

- `file://bundles/dictionaries/_codes.json` (seed documents for `_codes`)
- `file://bundles/shortcuts/shortcuts.json` (seed document for `_shortcuts`)
- `file://bundles/searchIndex/searchIndex.json` (Atlas Search index definition)
- `file://ingest/config/flattener_mappings_f.jsonc` (flattening/search projection mapping rules)

These are **strategy-pack assets** resolved from disk. They are not the same as “slim search definition bundles” managed by the `kehrnel common bundles` CLI (those bundles have a different schema and are stored in `.kehrnel/bundles`).

## Related

- [Introduction](/docs/strategies/openehr/rps-dual/introduction) - Strategy overview
- [Data Model](/docs/strategies/openehr/rps-dual/data-model) - Storage structure
- [Configuration (Global)](/docs/getting-started/configuration) - Environment variables
