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
    "value_sets": "snomed_value_sets",
    "grounding_reviews": "snomed_grounding_reviews",
    "gold_notes": "snomed_gold_notes",
    "usage_events": "snomed_usage_events",
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
    "lexical_strategy": "normalized_regex",
    "scope_max_ids": 20000,
    "scope_cache_ttl_seconds": 86400,
    "scope_cache_max_entries": 5000
  },
  "retrieval": {
    "vector_index": "",
    "vector_path": "embedText",
    "candidate_pool": 40,
    "rrf_k": 60,
    "rerank_pool": 40
  },
  "clinical": {
    "max_phrases": 20,
    "per_phrase_limit": 5,
    "llm_extraction": true,
    "accept_confidence": 0.68
  },
  "grounding": {
    "store_source_text": false,
    "max_codings_per_review": 50
  }
}
```

## Activation, Seed and Licensed Releases

The pack declares a manifest-borne `activation` block; the runtime enforces it and consumers (for example Healthcare Data Lab's Strategy Studio) render it.

```jsonc
"activation": {
  "requires_database": true,
  "readiness_op": "snomed_readiness",
  "database": { "naming_hint": "<environment database>_<domain>", "example": "hdl_demo_snomedct" },
  "conditions": [ /* required license attestation + notices, with reference links */ ],
  "seed": {
    "kind": "collection_copy",
    "source_database": "snomedct_seed",
    "license_class": "sample",
    "collections": [ { "name": "snomed_concepts", "from": "snomed_concepts", "expected_documents": 25000 } ]
  }
}
```

- **Attestation is runtime authority.** A required `kind: "attestation"` condition must be explicitly accepted in the activation request; activation without it fails with a typed 422 (`SNOMED_LICENSE_ATTESTATION_REQUIRED`). Acceptances are recorded on the activation with the condition id, statement version, accepting user, timestamp, and the manifest digest, so the evidence is replayable.
- **Seed provisioning** copies the declared collections from `activation.seed.source_database` into the activation-owned tenant database, using the configured collection names. It is idempotent (a non-empty target is skipped unless forced), runs only after the attestation is recorded, and reports per-collection copied/expected counts in the activation result. **Sample-class content only** — the seed source is the redistributable demo subset, never a full release.
- **Readiness gate.** After a declared seed the runtime calls `activation.readiness_op` so an empty or partial seed leaves a visibly not-ready activation instead of a green one.
- **Licensed releases** arrive through the release path: stage a file with `snomed_upload_release` (inline base64, ≤ 5 MB) or the signed-URL pair `snomed_initiate_release_upload` / `snomed_finalize_release_upload`, then run list → inspect → diff → ingest. Persisting an ingest still requires `license_acknowledged: true` (422 `SNOMED_LICENSE_ACKNOWLEDGEMENT_REQUIRED` otherwise), and every path stays inside the configured `source.local_dir` staging jail.

### Collection naming

Pack defaults and the published guide use different names for the same two collections. Both work (the activation config overrides collection names); the mapping is:

| Pack default | Published guide / Spanish IRBD | Purpose |
|---|---|---|
| `snomed_concepts` | `snomed-irbd` | Canonical concept documents |
| `snomed_terms` | `snomed-term-search` | Term sidecar |

## What Should Be Configurable

| Field | Why configurable |
|-------|------------------|
| `database` | Logical name shown in pack metadata; binding selects the actual MongoDB database |
| `release.id`, `release.label` | Each tenant may ingest a different licensed publication |
| `source.*` | Local folder and filename pattern where the customer places the licensed JSON release |
| `collections.*` | Tenants may name canonical concepts, term sidecar, ValueSet registry, and reviewed-grounding collections |
| `languages` | Sidecar generation can target Spanish, English, or local language subsets |
| `ingest.batch_size` | Tune for laptop, CI, or production clusters |
| `ingest.include_descendants` | Default false to avoid redundant large descendant arrays |
| `collections.sidecar_enabled` | Canonical-only deployments are valid but cannot search or ground mentions |
| `indexes.*` | Enable only the index families required by the deployment |
| `search.default_language`, limits | API defaults without hardcoding client behavior |
| `search.scope_max_ids` | Caps how many concept ids an ECL/area scope may apply to a query |
| `search.scope_cache_*` | Tune scope-expansion caching for release size and query mix |
| `retrieval.vector_index`, `vector_path` | Opt in to the Atlas vector branch; empty means lexical only |
| `retrieval.candidate_pool`, `rrf_k`, `rerank_pool` | Tune fusion and reranking cost against recall |
| `clinical.max_phrases`, `per_phrase_limit` | Bound note-mapping cost per request |
| `clinical.llm_extraction` | Turn LLM-assisted extraction off for strictly deterministic deployments |
| `clinical.accept_confidence` | Tenant-specific acceptance bar for auto-accepted groundings |
| `grounding.store_source_text` | Defaults false; enables explicit tenant retention of clinical source text |
| `grounding.max_codings_per_review` | Bounds reviewer submissions |

## Optional Adapters

Vector retrieval, reranking, and LLM extraction are bound per environment, not configured in the strategy pack. Each is optional and independent:

| Binding | Provider | Effect when absent |
|---------|----------|--------------------|
| `llm` | `http` | Note mapping uses deterministic extraction; `/llm-ground` returns `503 SNOMED_LLM_UNAVAILABLE` |
| `rerank` | `http` | Fused search returns reciprocal-rank order and reports `reranked: false` |
| `embedding` | `http` | Unused by this pack; the vector branch relies on Atlas auto-embedding |

```json
{
  "bindings": {
    "db": { "provider": "mongodb", "uri": "mongodb://localhost:27017", "database": "snomedct" },
    "extras": {
      "llm": {
        "provider": "http",
        "endpoint": "https://gateway.example/v1/chat/completions",
        "model": "gpt-5.5",
        "max_tokens": 4096,
        "timeout_seconds": 60
      },
      "rerank": {
        "provider": "http",
        "endpoint": "https://api.voyageai.com/v1",
        "model": "rerank-2.5",
        "timeout_seconds": 15
      }
    }
  }
}
```

The `llm` and `rerank` endpoints must be absolute `http(s)` URLs. `api_key` is optional and is read verbatim from the resolved binding, so supply it through `bindings_ref` and a bindings resolver rather than inline, exactly as you would for a database URI. LLM extraction returns mentions and their context only; it never returns a SNOMED CT code.

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
  "version": "0.5.0",
  "domain": "snomedct",
  "config": {
    "release": { "id": "20260601" },
    "collections": {
      "concepts": "snomed_concepts",
      "terms": "snomed_terms",
      "value_sets": "snomed_value_sets",
      "grounding_reviews": "snomed_grounding_reviews",
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

