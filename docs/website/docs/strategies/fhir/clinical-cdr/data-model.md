---
sidebar_position: 4
---

# FHIR Clinical CDR Data Model

`fhir.clinical_cdr` persists **native FHIR JSON** in MongoDB with **one collection per resource type**. Search uses **in-place denormalization** on the same documents—unlike openEHR RPS Dual, there is no separate search sidecar collection.

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

Documents retain FHIR fields (`resourceType`, `id`, `meta`, …). Kehrnel synthetic watermarking (when enabled) adds `meta.tag` and extension metadata before insert.

## Canonical document shape

Representative Patient (abbreviated):

```json
{
  "_id": "patient-uuid",
  "resourceType": "Patient",
  "id": "patient-uuid",
  "name": [{ "family": "Smith", "given": ["John"] }],
  "meta": {
    "versionId": "1",
    "lastUpdated": "2026-05-28T12:00:00Z"
  }
}
```

MongoDB `_id` typically aligns with FHIR logical `id` for generated resources.

## Search denormalization (`_search`)

After **`fhir_denormalize`**, fhir-mql adds flattened search fields on the **same** document:

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

Re-run denormalize when compartment definitions or search YAML change.

## Indexes

**`fhir_ensure_indexes`** creates MongoDB indexes declared in fhir-mql resource configs for `_search.*` (and related paths). With `search.auto_index: true` (default), index creation runs after denormalize maintenance.

Inspect index presence via the **`fhir_stats`** op (counts, `_search` coverage, generation vs search gaps).

## Generation vs search coverage

fhir-gen can emit many resource types; fhir-mql only provides search configs for a subset. **`fhir_stats`** reports types with documents but missing search YAML—generate is not enough for search until denormalize + config exist for that type.

Known generation types are exposed from the strategy bridge (`known_generation_resource_types()`).

## Per-type collections only

`fhir.clinical_cdr` stores canonical FHIR resources in one MongoDB collection per resource type (for example `Patient`, `Observation`). Do not use monolithic `fhir_resources` bag layouts in the same database without migration.
