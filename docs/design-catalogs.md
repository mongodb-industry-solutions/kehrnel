# Authorized source-profile catalog

`GET /v1/source-profiles` returns `source-profile-catalog/v1` with whole, integrity-checked profiles whose server-reviewed required packages are currently installed, admitted and readable through the existing strategy access policy. `GET /v1/strategies` remains the separate authorized installed-package discovery endpoint.

The initial descriptor is a customer-independent **design** profile:

- `source-profile:exchange.openehr-json.rps-dual-design@0.1.0`
- Content digest: `sha256:0f114ef59dc24eb964cf7eb862645eae53cdea44fa6f93c5e2ba12781eaf8061`
- Required package: `openehr.rps_dual@0.2.0`, package digest `sha256:eb37f11855cf428abc21d620fad8219c0fb16239af0819e31b36b522121815f8`
- Maturity `specified`; mapping `proposed`. No target, template, clinical query, cohort workload, preparation or runtime qualification is claimed.

## Authority and visibility

A standalone installation key uses its configured owner, action and exact package ceiling. An HDL-delegator key additionally requires a valid short-lived delegated-principal assertion with effective `strategy:read`, live server-derived owner scope and nonempty exact package references. Kehrnel intersects the claims with the key ceiling and existing policy. Missing/invalid/mis-scoped assertions fail closed. `blueprint:validate` with empty references grants no catalog access; successful catalog read grants no compilation or runtime action.

The descriptor's required package association is server-owned and keyed by its complete `{kind,id,version,digest}`. Public `strategyIds`, labels, filenames and browser metadata are hints, not admission, readiness or identity evidence. Package version/content changes invalidate the association until a new exact qualification is reviewed. Existing private-default and explicitly approved public-read rules remain unchanged; shipping this catalog grants no owner access and does not publish any package. IBM remains governed by its existing private/customer entitlement and is absent from this design descriptor.

Names, profile counts and bodies are filtered on every request. Responses and bounded errors use `Cache-Control: private, no-store` and `Vary: X-API-Key, X-HDL-Principal-Assertion`. No filtered result is cached across callers. If no eligible profile remains, the endpoint returns HTTP 503 with `SOURCE_PROFILE_CATALOG_UNAVAILABLE` and no private identity or path details. The v1 catalog requires a nonempty `profiles` array; an empty installed strategy list remains valid on its own endpoint.

## Library and wire contracts

`kehrnel.engine.core.source_profiles.load_source_profile_catalog` is a pure Python library loader independent of HDL, HTTP, JWT, registry activation and strategy entrypoints. It validates the bundled JSON and verifies each digest over normalized model wire, excluding only its self-referential `artifactRef.digest`. The API boundary separately filters whole profiles through the current exact-package policy. The standalone source-profile/catalog schemas are packaged with the descriptor.

Do not filter or rewrite an individual profile body under an old digest. A descriptor content change requires a new immutable version/digest and a reviewed server-owned package association. Resolve authored profile pins by all four reference fields; a same-ID current version or catalog ordering cannot replace the selected version. Saving a structurally valid draft preserves an unavailable authored pin; structural validation does not establish profile access or readiness.

## Increment boundary

This endpoint supports source selection and structural draft saving. Architecture compilation remains unavailable until a separately reviewed bounded implementation has genuine target, binding and workload definitions. Required unresolved sources prevent ready planning; source-free/custom/cohort drafts remain authorable and saveable. Workload behavior and semantic standards come from reviewed definitions and protocol/authority declarations, not identifiers or labels. Saved drafts remain unprepared.

HDL must supply live package-action delegation through its existing transport before a shared delegator can read private catalog data. Keep its validation-only signer narrow. Catalog GET never selects an environment, activates a strategy, prepares a target, persists a design or runs a query.

## Private architecture inputs

`GET /v1/architecture-resolution-inputs` returns `architecture-resolution-input-catalog/v1` using the same owner-scoped `strategy:read` authority. It supplies design-reference target and binding definitions only. It grants no compile, preparation or execution authority and makes no live capability observation. An operator-authored `unknown` capability stays `unknown`; empty bindings establish no readiness. There is no bundled qualification or automatically available target.

Configure `KEHRNEL_ARCHITECTURE_INPUTS_FILE` explicitly with a local JSON file outside the executable SDK and all installed strategy package roots. Symlinked paths are rejected. The HTTP caller cannot select this file. The pure SDK entrypoint is `kehrnel.engine.core.architecture_catalog.load_architecture_resolution_context(path, forbidden_roots=...)`; external library callers supply all external executable pack roots. The loader needs no HDL, HTTP, JWT, strategy entrypoint, architecture compiler or lifecycle modules.

The closed private envelope is `{contract: "architecture-resolution-context/v1", records: [...]}`. Every record contains exactly `artifactRef` (kind `architecture-input-record`), `owner`, nonempty `requiredPackageRefs`, nonempty `sourceProfileRefs`, `evidenceBasis: "design_reference"`, nonempty `evidenceRefs`, nonempty `targetCapabilityProfiles`, and `semanticExecutionBindings` (which may be empty). Each artifact reference contains kind/id/version/digest; package refs contain id/version/packageDigest. Binding strategy refs must match a required package; their semantic-authority and component refs must occur among the private evidence refs. Operator review of actual evidence and its bounded coverage is required before installing a record. A descriptive label, self-generated digest or claimed reviewed status does not establish capability truth. Observed runtime/tenant inventory is outside this version's unlabelled wire.

The loader normalizes contract bodies, checks full source-profile pins and content, and rejects duplicate or conflicting immutable kind/id/version identities. Target digests exclude top-level `artifactRef` and `contract`, preserving the maintained target-seed convention. Binding digests exclude only top-level `artifactRef`. Private record digests exclude only their own `artifactRef`, retaining owner, package/profile/evidence refs and complete normalized definitions. Use the pure content-digest helpers to mint new immutable definitions; unsupported canonical values are rejected. Source-profile and Blueprint digest rules remain unchanged.

Each request filters whole records by matching owner, effective read action, server-owned full source-profile/package association and current installed/admitted package/dependency policy. An explicitly public package does not publish its owner's private record. Private owner/evidence/config metadata is never returned. Package, grant, ceiling or dependency revocation is rechecked. Missing, invalid, inaccessible or ineligible inputs yield the same bounded HTTP503 `ARCHITECTURE_INPUT_CATALOG_UNAVAILABLE`, with private/no-store headers and no names, counts, paths or policy details. Empty targets remain invalid; missing inputs never fabricate a target. Structurally saved drafts may retain unavailable pins. This endpoint neither changes the Blueprint nor restores compiler/lifecycle graphs.
