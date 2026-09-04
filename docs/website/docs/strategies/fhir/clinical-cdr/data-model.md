---
sidebar_position: 4
---

# FHIR Clinical CDR Data Model

`fhir.clinical_cdr` persists **native FHIR JSON** in MongoDB with **one collection per resource type**. Search uses mandatory **in-place projections** on the same documents; there is no separate search sidecar collection.

## Collection layout

| Setting | Value |
|---------|--------|
| `collections.mode` | `per_resource_type` |
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
    "storage_schema_version": "1",
    "projection_contract_version": "v1:<sha256>",
    "resource_projection_version": "v1:<sha256>",
    "fhir_release": "R5",
    "projected_at": "2026-09-03T12:00:00Z",
    "stored_at": "2026-09-03T12:00:00Z"
  }
}
```

FHIR `meta` remains canonical and is never used for persistence schema versioning.
MongoDB `_id`, `_search`, `_compartments`, and `_kehrnel` are removed from FHIR
responses. Valid primitive extensions such as `_birthDate` are preserved.

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

## Compartments (`_compartments`)

Compartment membership supports chained and compartment-scoped searches (for example Patient-linked resources). Definitions come from fhir-mql compartment JSON unless overridden by `search.compartment_definitions_dir`.

Run `fhir_denormalize` for affected resource types when compartment definitions
or search YAML change. The resource projection version identifies targeted work;
a shared compartment-definition change invalidates all affected types.

## Indexes

**`fhir_ensure_indexes`** creates MongoDB indexes declared in fhir-mql resource
configs for `_search.*`, `_compartments.*`, and related paths. These indexes are
mandatory and are ensured before normal writes and after reprojection.

Inspect index presence via the **`fhir_stats`** op (counts, `_search` coverage, generation vs search gaps).

## Generation vs search coverage

fhir-gen can emit many resource types; fhir-mql only provides search configs for
a subset. The Clinical CDR fails closed instead of storing generated resource
types outside its configured and searchable scope. **`fhir_stats`** reports
projection/version coverage and any package coverage gaps.

Known generation types are exposed from the strategy bridge (`known_generation_resource_types()`).

## Per-type collections only

`fhir.clinical_cdr` stores canonical FHIR resources in one MongoDB collection per resource type (for example `Patient`, `Observation`). Do not use monolithic `fhir_resources` bag layouts in the same database without migration.
