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
      "enabled": true,
      "atlasIndex": {
        "name": "search_nodes_index"
      }
    },
    "codes": {
      "name": "_codes",
      "mode": "extend"
    },
    "ehr": {
      "name": "ehr"
    },
    "contributions": {
      "name": "contributions"
    }
  },
  "transform": {
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
| `mode` | string | `extend` | `extend` or `fixed` |

#### ehr

EHR metadata collection.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `ehr` | Collection name |

#### contributions

Audit trail collection.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | `contributions` | Collection name |

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

Required index configuration for `compositions_search`:

```json
{
  "mappings": {
    "dynamic": false,
    "fields": {
      "ehr_id": {
        "type": "string"
      },
      "tid": {
        "type": "number"
      },
      "sn": {
        "type": "embeddedDocuments",
        "fields": {
          "p": {
            "type": "string",
            "analyzer": "keyword"
          },
          "data": {
            "type": "document",
            "dynamic": true
          }
        }
      }
    }
  }
}
```

## Example Configurations

### Development

```json
{
  "database": "dev_cdr",
  "collections": {
    "compositions": { "name": "compositions" },
    "search": { "name": "search", "enabled": true },
    "codes": { "name": "_codes" }
  },
  "transform": {
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
      "enabled": true,
      "atlasIndex": { "name": "search_nodes_index" }
    },
    "codes": { "name": "_codes", "mode": "extend" }
  },
  "transform": {
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
curl -X POST "http://localhost:8000/environments/production/activate" \
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

## Related

- [Introduction](/docs/strategies/openehr-rps-dual/introduction) - Strategy overview
- [Data Model](/docs/strategies/openehr-rps-dual/data-model) - Storage structure
- [Configuration (Global)](/docs/getting-started/configuration) - Environment variables
