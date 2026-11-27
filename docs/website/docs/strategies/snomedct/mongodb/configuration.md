---
sidebar_position: 2
---

# SNOMED CT Configuration

Use `src/kehrnel/engine/strategies/snomedct/mongodb/defaults.json` as the activation baseline for `snomedct.mongodb`.

The strategy is multi-tenant. Each tenant activates the same pack with its own MongoDB binding and its own licensed SNOMED CT release.

## Pack Layout

| Path | Purpose |
|------|---------|
| `manifest.json` | Strategy identity, capabilities, use-case metadata, ops, docs link |
| `defaults.json` | Default activation config |
| `schema.json` | JSON Schema validation for activation config |
| `spec.json` | Machine-readable metamodel, stores, indexes, query modes, transformations |
| `strategy.py` | Runtime implementation for transform/query/ops |

## Activation Baseline

```json
{
  "database": "snomedct",
  "release": {
    "id": "20260601",
    "label": "SNOMED CT International + Spain + Medicines extension"
  },
  "source": {
    "local_dir": ".kehrnel/snomedct/releases",
    "file_name": "",
    "file_pattern": "*.json"
  },
  "collections": {
    "concepts": "snomed_concepts",
    "terms": "snomed_terms",
    "sidecar_enabled": true
  },
  "languages": ["es", "en"],
  "ingest": {
    "batch_size": 1000,
    "include_descendants": false,
    "drop_before_ingest": false,
    "rebuild_sidecar": true,
    "create_indexes_before_ingest": true
  },
  "indexes": {
    "canonical": true,
    "sidecar": true,
    "text": false,
    "atlas_search": false
  },
  "search": {
    "default_limit": 20,
    "max_limit": 100,
    "default_language": "es",
    "use_sidecar": true,
    "lexical_strategy": "normalized_regex"
  }
}
```

## What Should Be Configurable

| Field | Why configurable |
|-------|------------------|
| `database` | Logical name shown in pack metadata; binding selects the actual MongoDB database |
| `release.id`, `release.label` | Each tenant may ingest a different licensed publication |
| `source.*` | Local folder and filename pattern where the customer places the licensed JSON release |
| `collections.*` | Tenants may use naming conventions or multiple release sets |
| `languages` | Sidecar generation can target Spanish, English, or local language subsets |
| `ingest.batch_size` | Tune for laptop, CI, or production clusters |
| `ingest.include_descendants` | Default false to avoid redundant large descendant arrays |
| `collections.sidecar_enabled` | Canonical-only deployments are valid but cannot search or ground mentions |
| `indexes.*` | Enable only the index families required by the deployment |
| `search.default_language`, limits | API defaults without hardcoding client behavior |

## What Should Not Be Configurable

| Fixed behavior | Reason |
|----------------|--------|
| Canonical identity: `releaseId + conceptId` | Stable release-aware lookup |
| Sidecar identity: `releaseId + conceptId + descriptionId + languageCode` | Deterministic rebuilds |
| Distribution credentials | Kehrnel does not acquire or distribute SNOMED CT content in this pack |
| Baseline lexical strategy | Keeps MongoDB Atlas and non-Atlas parity |
| Canonical source ownership | Kehrnel does not ship licensed SNOMED CT content |

## Bindings

MongoDB connectivity comes from activation bindings, not from the strategy config:

```json
{
  "strategy_id": "snomedct.mongodb",
  "version": "0.1.0",
  "domain": "snomedct",
  "config": {
    "release": { "id": "20260601" },
    "collections": {
      "concepts": "snomed_concepts",
      "terms": "snomed_terms",
      "sidecar_enabled": true
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
}
```

For production, prefer `bindings_ref` with `KEHRNEL_BINDINGS_RESOLVER`.

## Dependencies

Install the runtime with API and MongoDB support:

```bash
pip install -e ".[api,mongo]"
```
