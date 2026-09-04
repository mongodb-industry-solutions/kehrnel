---
sidebar_position: 5
---

# Semantic projections

FHIR semantic projections are optional derived views of canonical resources.
They make clinical narrative usable through MongoDB text, vector, hybrid, or
partner-specific APIs without changing the FHIR representation returned to a
FHIR client.

## Lifecycle

1. Define a named pipeline in the strategy activation.
2. Preview its field extraction and chunk boundaries.
3. Explicitly materialize selected stored resources.
4. Query the derived sidecar collection through Atlas Vector Search.
5. Rebuild when the pipeline, model, source resource, or profile scope changes.

Activation validates and versions the declaration. It does **not** generate
vectors, contact an embedding provider, or modify clinical resources.

## Example activation configuration

```json
{
  "semantic": {
    "enabled": true,
    "pipelines": [
      {
        "id": "clinical-notes-v1",
        "enabled": true,
        "resource_types": ["DiagnosticReport", "Composition", "DocumentReference"],
        "profiles": [],
        "fields": [
          { "path": "DiagnosticReport.conclusion", "label": "Conclusion" },
          { "path": "Composition.section.text.div", "label": "Narrative" },
          { "path": "DocumentReference.description", "label": "Description" },
          { "path": "DocumentReference.content.attachment.title", "label": "Attachment" }
        ],
        "chunking": { "max_chars": 3000, "overlap_chars": 300 },
        "embedding": {
          "binding": "embedding",
          "model": "clinical-embedding-model",
          "dimensions": 1536
        },
        "trigger": "manual",
        "storage": {
          "mode": "sidecar",
          "collection": "fhir_semantic_chunks",
          "vector_index": "clinical-notes-v1-vector"
        }
      }
    ]
  }
}
```

Fields use a deliberately small dotted FHIRPath subset. A pipeline combines all
matching values in declared order. Profile filters are optional; when provided,
their canonical URLs must be selected in
`implementation_guides.active_profiles`.

Raw attachment base64 is not interpreted as text. A future attachment-extraction
adapter must apply MIME allowlists, size limits, malware controls, and provenance
before extracted text can enter a semantic pipeline.

## Preview

The preview operation is available consistently through the runtime API and CLI:

```bash
kehrnel core env run fhir_semantic_preview \
  --env dev --domain fhir \
  --payload semantic-preview.json
```

Or call:

```text
POST /api/domains/fhir/semantic/preview
```

with:

```json
{
  "pipeline_id": "clinical-notes-v1",
  "resource": {
    "resourceType": "DiagnosticReport",
    "id": "report-1",
    "status": "final",
    "code": { "text": "Radiology report" },
    "conclusion": "No acute cardiopulmonary abnormality."
  }
}
```

The response includes selected values, rendered text, chunks, source hash and a
deterministic projection version. It never returns a vector and never writes.

## Materialize and search

Materialization is explicit and requires configured `embedding`, `storage`,
`index_admin`, and—when available—`atlas_search` adapters:

```http
POST /api/domains/fhir/semantic/materialize
Content-Type: application/json

{
  "pipeline_id": "clinical-notes-v1",
  "targets": [
    {"resource_type": "DiagnosticReport", "id": "report-1"}
  ]
}
```

Kehrnel reads the canonical resource from the activated strategy database,
extracts and embeds the configured fields, upserts deterministic sidecar chunks,
removes stale chunks for the same source, and ensures the configured Atlas vector
index. Embedding credentials and endpoints come only from environment bindings.

```http
POST /api/domains/fhir/semantic/search
Content-Type: application/json

{
  "pipeline_id": "clinical-notes-v1",
  "query": "recent evidence of metastatic disease",
  "limit": 10,
  "resource_types": ["DiagnosticReport"]
}
```

Search embeds the query and executes `$vectorSearch`. It returns source pointers,
scores, and chunks, never raw vectors. Deployments without an embedding provider
or Atlas Vector Search fail explicitly; there is no misleading local fallback.

## Storage and versioning

The default target is a polymorphic `fhir_semantic_chunks` sidecar collection.
One vector-index family can then support cross-resource retrieval while the
canonical per-resource collections remain optimized for FHIR search.

Each stored chunk includes environment, resource type/id, profiles,
pipeline id, pipeline projection version, source hash, chunk ordinal, model,
dimensions and compartment filters. `_enrichments` on the source document may
hold only the rebuild receipt/status and must remain absent from FHIR responses.

Embedding credentials and endpoints belong in environment bindings, not in the
strategy configuration. Sending protected health information outside the tenant
must require an explicitly configured provider and deployment policy.

## Current boundary

Preview, explicit materialization, sidecar persistence and vector retrieval are
implemented. Bulk asynchronous rebuild jobs, attachment extraction, and hybrid
FHIR-filter/vector ranking remain outside the current accelerator boundary.
