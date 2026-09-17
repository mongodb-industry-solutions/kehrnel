---
sidebar_position: 4
---

# FHIR Resource Store Data Model

`fhir.resource_store` persists **native FHIR JSON** in a FHIR resource collection
family: one MongoDB collection for each resource type enabled by the active
capability contract. A concrete collection and its indexes are materialized only
when that resource type is first written. Search uses mandatory **in-place
projections** on the same documents; there is no separate search sidecar
collection.

## Collection layout

| Contract | Value |
|----------|-------|
| Physical model | One collection per resource type (fixed strategy invariant) |
| Collection name | `{collection_prefix}{ResourceType}` (e.g. `Patient`, `dev_Patient`) |
| Database | From activation `config.database` + bindings |

Example database `fhir_synthetic` after generating Patient and Observation:

| Collection | Contents |
|------------|----------|
| `Patient` | Canonical Patient resources |
| `Observation` | Canonical Observation resources |
| `Schedule`, `Slot`, `Appointment` | Scheduling corpora when generated |

Documents retain FHIR fields (`resourceType`, `id`, `meta`, …). Kehrnel synthetic watermarking (when enabled) adds canonical `meta.tag` and extension metadata before projection and persistence.

## Canonical document shape

Representative Patient (abbreviated):

```json
{
  "_id": "patient-uuid",
  "resourceType": "Patient",
  "id": "patient-uuid",
  "name": [{ "family": "Smith", "given": ["John"] }],
  "meta": { "profile": ["https://example.org/StructureDefinition/patient"] },
  "_search": { "familyName_lower": ["smith"] },
  "_compartments": { "Patient": ["patient-uuid"] },
  "_kehrnel": {
    "storage_schema_version": "2",
    "projection_contract_version": "v1:<sha256>",
    "resource_projection_version": "v1:<sha256>",
    "fhir_release": "R5",
    "projected_at": "2026-09-03T12:00:00Z",
    "stored_at": "2026-09-03T12:00:00Z"
  },
  "_custom": { "customerScore": 7 },
  "_enrichments": { "cohort": "example" }
}
```

FHIR `meta` remains canonical and is never used for persistence schema versioning.
MongoDB `_id`, `_search`, `_compartments`, `_kehrnel`, `_custom`, and
`_enrichments` are removed from FHIR responses. Valid primitive extensions such
as `_birthDate` are preserved. Canonical API updates atomically preserve the two
customer-owned namespaces so application-side annotations are not erased.

## Search denormalization (`_search`)

Before every stored write, fhir-mql adds flattened search fields on the **same** document:

```json
{
  "resourceType": "Patient",
  "id": "patient-uuid",
  "name": [{ "family": "Smith", "given": ["John"] }],
  "_search": {
    "family": "smith",
    "given": "john"
  },
  "_compartments": {
    "Patient": ["patient-uuid"]
  }
}
```

Field names under `_search` follow per-resource YAML in fhir-mql (`Patient.yaml`, `Slot.yaml`, …). Modifiers and token types are encoded per fhir-mql rules.

FHIR search against MongoDB targets these denormalized paths via compiled MQL filters—not full-document scans of nested arrays for every parameter.

Standard FHIR Search uses ordinary MongoDB filters and indexes; it does **not**
depend on Atlas Search. Atlas Vector Search is used only by the separate,
optional semantic-sidecar workflow when an embedding provider, sidecar
collection, and vector index are configured.

## Compartments (`_compartments`)

Compartment membership supports chained and compartment-scoped searches (for example Patient-linked resources). Definitions come from fhir-mql compartment JSON unless overridden by `search.compartment_definitions_dir`.

Run `fhir_denormalize` for affected resource types when compartment definitions
or search YAML change. The resource projection version identifies targeted work;
a shared compartment-definition change invalidates all affected types.

## Indexes

**`fhir_index_manifest`** generates the deterministic per-resource contract,
including the unique logical-id index, deduplicated search indexes, a digest, and
the configured budget. **`fhir_ensure_indexes`** creates MongoDB indexes declared in fhir-mql resource
configs for `_search.*`, `_compartments.*`, and related paths. These indexes are
mandatory and are ensured lazily before normal writes and after reprojection.
Unexpected indexes are reported as unmanaged-or-stale candidates and are never
deleted automatically.

Inspect index presence via the **`fhir_stats`** op (counts, `_search` coverage, generation vs search gaps).

## Generation vs search coverage

FHIR Core schemas, fhir-mql search mappings, fhir-gen generation schemas, and
curated recipes are independent capability sets. The Resource Store fails closed
instead of storing a resource without its mandatory search projection contract.
Recipe membership does not restrict import or write support. **`fhir_stats`**
reports projection/version coverage and package coverage gaps.

Known generation types are exposed from the strategy bridge (`known_generation_resource_types()`).

## Optional migration orchestration

The primary data model does not depend on migration collections. They are
created only when a resumable migration is started. `_kehrnel_fhir_migration_runs`
records tenant-scoped run status and checkpoints, while
`_kehrnel_fhir_migration_chunks` records ordered digests and bounded reports.
An exact retry is replayed without another write; different content at the same
checkpoint fails closed.

The clinical Bundle or NDJSON source is not retained in these collections or in
the core job database. Healthcare Data Lab keeps the chosen file in the browser
and streams it in bounded chunks. `_kehrnel.provenance.migration_run_id` on stored
resources links canonical data to the audit run without mixing run mechanics
into FHIR `meta`.

## Per-type collections only

`fhir.resource_store` stores canonical FHIR resources in one MongoDB collection per resource type (for example `Patient`, `Observation`). Do not use monolithic `fhir_resources` bag layouts in the same database without migration.
