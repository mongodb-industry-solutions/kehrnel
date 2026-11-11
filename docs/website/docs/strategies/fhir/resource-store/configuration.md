---
sidebar_position: 2
---

# FHIR Resource Store Configuration

Use `src/kehrnel/engine/strategies/fhir/resource_store/specification/defaults.json` as the authoritative activation baseline for `fhir.resource_store`.

That file is what the runtime merges into an environment when the strategy is activated. Apply small, explicit overlays for your deployment rather than copying the entire pack into client code.

## Strategy identity migration

The strategy identity is now `fhir.resource_store` and the Python pack is
`kehrnel.engine.strategies.fhir.resource_store`. The rename is deliberately not
implemented as a permanent alias: an environment activated with the previous
identity must be reviewed and reactivated with the new id and manifest version.

Reactivation does not require moving clinical documents. Keep the existing
strategy database name when it already contains data, verify its binding, take a
backup, and point the new activation at that same database. The renamed
`fhir_synthetic_resource_store` database in the development example applies only
to clean development environments; Kehrnel does not rename MongoDB databases.

## Strategy pack layout

| Path | Purpose |
|------|---------|
| `specification/manifest.json` | Strategy identity, capabilities, ops, `ui.docs` link |
| `specification/defaults.json` | Default activation config |
| `specification/schema.json` | User-facing validation for activation overrides |
| `specification/spec.json` | Machine-readable storage and index specification |
| `specification/*.json` | Sample activation / synthetic-job payloads |
| `scripts/strategy.py` | Runtime: `compile_query`, `execute_query`, `run_op` |
| `scripts/bridge.py` | fhir-gen / fhir-mql adapters, Mongo resolution |
| `scripts/generation.py`, `denormalize.py`, `indexes.py`, `query.py` | Feature modules |

Install optional FHIR libraries (vendored under `src/kehrnel/engine/domains/fhir/libs/`):

```bash
pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql
pip install -e ".[api,mongo,fhir]"
```

## Activation baseline

Healthcare Data Lab deliberately exposes only project-level choices during
activation: the target database, FHIR release, optional implementation guides
and profiles, terminology validation, and optional semantic projections.
Mandatory search/index projections and internal filesystem paths are enforced by
the strategy and are not presented as controls. Synthetic cohort design belongs
in Data Factory rather than strategy activation.

```json
{
  "database": "fhir_cdr",
  "schema_version": "R5",
  "implementation_guides": { "packages": [], "active_profiles": [] },
  "terminology_validation": {
    "mode": "disabled",
    "binding": "terminology",
    "systems": [],
    "max_codings_per_resource": 100
  },
  "semantic": { "enabled": false, "pipelines": [] }
}
```

### Customer-facing fields

| Field | Description |
|-------|-------------|
| `database` | Required strategy-owned MongoDB database, distinct from the environment/core database |
| `schema_version` | FHIR release (`R4`, `R5`, or `R6`); R4 is currently a minimal Patient/Observation tier |
| `implementation_guides.packages` | Optional local FHIR NPM packages; an empty list is normal FHIR Core mode |
| `implementation_guides.active_profiles` | Optional canonical profile URLs selected from enabled packages; empty means no profile constraint |
| `implementation_guides.profile_validation` | Optional `disabled` or fail-closed `required` enforcement through a validation adapter |
| `terminology_validation` | Optional `advisory` or fail-closed `required` Coding validation through the tenant terminology gateway, restricted to an explicit system allowlist |
| `semantic.enabled` | Enables configured semantic projection definitions; activation itself never generates embeddings |
| `semantic.pipelines` | Optional named field-selection, chunking, model, trigger and sidecar-storage contracts |

The packaged defaults retain internal safety limits, search projections, index
governance, and synthetic-generation defaults. These are documented for
operators and library users but are not presented as activation choices in HDL.

The one-collection-per-resource-type model is a fixed invariant of
`fhir.resource_store`, not an activation option. A different physical collection
model belongs in a different strategy pack.

## Bindings

The environment binding supplies MongoDB connectivity and credentials. The
strategy-owned database is selected only by the reviewed `config.database`:

```json
{
  "strategy_id": "fhir.resource_store",
  "version": "0.1.0",
  "domain": "fhir",
  "config": {
    "database": "fhir_cdr",
    "schema_version": "R5"
  },
  "bindings": {
    "db": {
      "provider": "mongodb",
      "uri": "mongodb://localhost:27017",
      "database": "fhir_cdr"
    }
  },
  "allow_plaintext_bindings": true
}
```

Packaged example: `src/kehrnel/engine/strategies/fhir/resource_store/specification/activate_dev.json`.

For production, prefer `bindings_ref` with `KEHRNEL_BINDINGS_RESOLVER` instead
of inline URIs. A database embedded in the URI or environment metadata does not
override `config.database`. The resolver also rejects an activation whose
strategy database equals that environment database, preventing FHIR collections
from being written to the tenant's core/transversal database.

## Environment activation (HTTP)

```bash
export RUNTIME_URL="${RUNTIME_URL:-http://localhost:8080}"

curl -sS -X POST "${RUNTIME_URL}/environments/dev/activate" \
  -H "Content-Type: application/json" \
  -d @src/kehrnel/engine/strategies/fhir/resource_store/specification/activate_dev.json
```

Confirm activation:

```bash
curl -sS "${RUNTIME_URL}/environments/dev/activations/fhir"
```

## CLI context (optional)

```bash
kehrnel setup \
  --runtime-url "$RUNTIME_URL" \
  --env dev \
  --domain fhir \
  --strategy fhir.resource_store

kehrnel core env show --env dev
kehrnel strategy list --domain fhir
```

## fhir-mql config overrides

When `search.config_dir` and `search.compartment_definitions_dir` are null, the strategy uses fhir-mql defaults shipped with the installed package. Set these paths when you maintain custom resource search YAML or compartment definitions in your own repo.

After changing YAML, run **`fhir_denormalize`** for affected resource types. It
rebuilds projections, stamps new versions, and ensures indexes automatically.

## Optional implementation-guide overlays

Customers do not need an IG to start. The selected Core tier and the implemented
Kehrnel capability matrix form the default. R4 is currently minimal; R5 and R6
are package-backed. To inspect customer
constraints, configure one or more checksum-pinned package directories or
archives:

```json
{
  "implementation_guides": {
    "compiled_root": "/var/lib/kehrnel/fhir-ig-cache",
    "packages": [
      {
        "enabled": true,
        "source": "/config/fhir/packages/customer.fhir.ig-1.0.0.tgz",
        "sha256": "<64 hexadecimal characters>"
      }
    ],
    "active_profiles": [
      "https://example.org/fhir/StructureDefinition/customer-observation"
    ],
    "profile_validation": {
      "mode": "required",
      "binding": "validation_engine",
      "fail_on_warning": false
    }
  }
}
```

`source` and `compiled_root` are operator-controlled paths on the Kehrnel host
or mounted container volumes. For portal users, configure an allowlisted staging
root and quotas:

```bash
export KEHRNEL_FHIR_IG_STAGING_ROOT=/var/lib/kehrnel/fhir-ig-staging
export KEHRNEL_FHIR_IG_UPLOAD_MAX_BYTES=33554432
export KEHRNEL_FHIR_IG_STAGING_MAX_BYTES=536870912
```

Healthcare Data Lab can then upload a `.tgz` from the FHIR Configuration Center,
or an operator can use the equivalent CLI workflow:

```bash
kehrnel fhir stage-ig ./customer.fhir.ig-1.0.0.tgz \
  --env dev --release R5 --out ./customer-ig-stage.json
```

`--release` is required when no FHIR strategy is active; an existing activation
provides the release automatically. The runtime validates archive paths, file
count, expanded size, package JSON, checksum, and release compatibility before returning an activation patch. That patch
contains the checksum-pinned package entry and a Kehrnel-owned `compiled_root`
inside the environment's staging area; the portal does not invent server paths.
Staging is not activation: an authorized user must still review the patch in
Strategy Studio or merge it into a CLI activation file, activate the package,
and voluntarily select any profiles. Use `kehrnel core env activate --config`
for that reviewed step.

Activation writes an immutable package lock, resource/profile catalog, search
plan, per-package terminology inventory, and an activation-level terminology
lock. The lock records the exact active profiles, required/extensible bindings,
invariants, package checksums, FHIR release, enforcement modes, and a stable
digest. Inventory still does not pretend that every invariant was enforced. Simple
FHIRPath expressions become candidate search paths; complex expressions are
marked for a reviewed override. Package discovery does not expand the REST
capability statement beyond interactions implemented by Kehrnel.

Profile selection and enforcement are separate. The default
`profile_validation.mode: disabled` catalogs selected profiles without claiming
conformance. `mode: required` makes writes fail closed unless a configured
`validation_engine` adapter validates resources against the selected profiles.
The adapter can wrap the official HL7 validator or HAPI validator and receives a
bounded `kehrnel-validation/v1` JSON envelope containing the release, package
sources, active profiles, and resources. Kehrnel does not implement a partial
FHIRPath validator internally.

Every layer is voluntary: no packages means FHIR Core mode, an empty
`active_profiles` array means no profile constraint, and selecting profiles does
not force enforcement unless the customer chooses `mode: required`. Multiple
packages and profiles can be selected simultaneously.

## Optional terminology enforcement

Terminology enforcement is configured separately from profile validation. This
keeps FHIR Core usable with no external dependency and prevents Kehrnel from
silently sending every code system to a default server.

```json
{
  "terminology_validation": {
    "mode": "required",
    "binding": "terminology",
    "systems": ["http://snomed.info/sct"],
    "max_codings_per_resource": 100
  }
}
```

On import and REST writes, Kehrnel walks canonical resources for Coding-shaped
objects whose `system` is explicitly listed. For selected profiles it also
applies the compiled element binding to the bounded coding group and asks the
gateway to validate membership in the canonical ValueSet/version. A required
binding succeeds when at least one coding at that element is a member; optional
binding failures remain warnings. Each result records the provider and release
used. `advisory` emits warnings without rejecting the resource; `required`
rejects failed required bindings and invalid configured-system codings. Codes
from unlisted systems outside active binding paths are not claimed as
validated.

## Stored-document versions

FHIR `meta.versionId` remains canonical clinical metadata. Kehrnel persistence
metadata is isolated under `_kehrnel`:

- `storage_schema_version` versions the MongoDB document shape.
- `projection_contract_version` fingerprints the complete active search and compartment contract.
- `resource_projection_version` fingerprints one resource configuration plus the shared compartment definitions, allowing targeted reprojection.
- `fhir_release`, `projected_at`, and `stored_at` provide operational context.

`_search` and `_compartments` remain top-level because the MQL and index contract
queries them directly. `_custom` and `_enrichments` are reserved for customer and
partner data and survive canonical FHIR updates. `_kehrnel`, both projection
buckets, both extension namespaces, and MongoDB `_id` are removed from every
FHIR-facing response.

See [Semantic projections](./semantic-projections.md) for the optional enrichment
configuration and its separate execution lifecycle.
