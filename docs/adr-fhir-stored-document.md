# ADR: FHIR Resource Store — stored-document model (`fhir.clinical_cdr`)

**Status:** Accepted and implemented · 2026-09-03  
**Contract:** stored-document schema `1`

## Context

The Clinical CDR stores FHIR R5 or R6 resources in one MongoDB collection per
resource type. The first implementation mixed `_stored_at` and
`_fhir_resource_type` into the canonical resource and allowed generation without
search projection. That produced documents which looked stored but were not
reliably searchable or usable through compartments.

FHIR `meta.versionId` cannot solve this problem: it is part of the clinical FHIR
resource and describes resource history, not Kehrnel's MongoDB shape or derived
search configuration.

## Decision

### 1. Current document shape

```javascript
{
  // Canonical FHIR resource, kept together at the root
  resourceType: "Observation",
  id: "obs-123",
  meta: { /* canonical FHIR Meta */ },
  // ...remaining canonical Observation fields...

  // Mandatory, regenerated operational projections
  _search: { /* typed fhir-mql fields */ },
  _compartments: { Patient: ["patient-123"] },

  // Kehrnel-owned persistence metadata
  _kehrnel: {
    storage_schema_version: "1",
    projection_contract_version: "v1:<sha256>",
    resource_projection_version: "v1:<sha256>",
    fhir_release: "R5",
    projected_at: ISODate("..."),
    stored_at: ISODate("...")
  }
}
```

Canonical fields remain together and readable at the root. `_search` and
`_compartments` remain top-level because fhir-mql filters and MongoDB indexes use
those paths directly. All other persistence mechanics are concentrated under
the reserved `_kehrnel` object. FHIR serialization removes MongoDB `_id`, both
projection buckets, `_kehrnel`, and the two legacy fields while preserving valid
FHIR primitive extensions such as `_birthDate`.

### 2. Projection and indexing are mandatory

Every import, FHIR create/update, and stored synthetic-generation path uses this
order:

1. validate the canonical resource;
2. calculate `_search` and `_compartments` (empty objects are materialized when
   the resource has no values for either bucket);
3. stamp current contract versions;
4. ensure configured indexes;
5. persist the complete document atomically.

Activation values that try to disable search, generation projection, or automatic
indexing are rejected. Runtime merging also coerces legacy stored activations to
the mandatory behavior. Payloads cannot opt out through `denormalize_after`,
`ensure_indexes`, or `skip_auto_index`.

`fhir_denormalize` is a repair/reprojection operation. It clears stale projections,
rebuilds both buckets, stamps current versions, verifies every targeted document,
and ensures indexes.

### 3. Version model: both global and per resource

- `storage_schema_version` is a manually bumped version for a breaking change to
  the stored MongoDB document shape.
- `projection_contract_version` is a deterministic digest of the active FHIR
  release, every loaded fhir-mql resource configuration, and the compartment
  definitions. It detects any change to the overall projection contract.
- `resource_projection_version` is a deterministic digest of the selected
  resource configuration plus shared compartment definitions. If only one
  resource YAML changes, this version identifies the collection that needs
  reprojection; a compartment-definition change safely invalidates all affected
  resource types.

The catalog and statistics operations expose these versions. They are never
written into canonical FHIR `meta`.

### 4. Identity, collections, and tenancy

- A resource is identified by `(resourceType, id)`.
- Each resource type has its own collection; a unique ascending `id` index is
  enforced before writes.
- The strategy database is mandatory and must differ from the database embedded
  in the HDL environment connection URI. The environment database is reserved
  for transversal tenant data.

### 5. Future history envelope

Version history, `vread`, ETags, and optimistic concurrency remain future work.
If those require a nested `{resource, search, compartments, control}` envelope,
the storage adapter remains the migration seam. That future change must bump
`storage_schema_version`; it must not reuse `meta.versionId` as a database schema
marker.

## Consequences

- No newly stored resource can lack `_search`, `_compartments`, contract metadata,
  or the configured indexes.
- A projection error fails the write instead of leaving a partially searchable
  corpus.
- The whole search contract can be audited, while resource-level changes can be
  reprojected selectively.
- Canonical FHIR responses remain free of MongoDB implementation fields.
- Existing pre-contract data must be reprojected or discarded. The initial HDL
  FHIR corpus was intentionally discarded so the new dedicated database can be
  populated from scratch.
