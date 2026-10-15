# Data Lab Core foundation contracts

Kehrnel's shared contract authority is `src/kehrnel/contracts`. Runtime and SDK
manifest imports are compatibility aliases to the same class; their plugin
ABIs remain separate.

This includes the implemented **KDL-001a foundation slice** and the first
source-aware compilation contracts. No UI should infer contracts from Python
models directly; committed JSON Schemas and versioned API responses are the
integration boundary.

## Source-aware compilation

Kehrnel now distinguishes concrete source profiles from MongoDB target
strategies. Catalog metadata declares artifact role, industry navigation paths
and provenance (`mongodb_curated`, `community`, `tenant_generated` or
`tenant_extended`). A customer Source Profile therefore cannot be presented as
if it were the same artifact as a FHIR/openEHR persistence strategy.

The discovery catalog starts one level above products with
`source-category/v1`: Healthcare and Pharmaceutical categories describe
product-neutral functional interactions and semantic domains. Concrete
products are immutable reference Source Profiles attached to those categories,
not the primary navigation model.

The retired `legacy-source-blueprint/internal` prototype recorded source instances, their
optional profile-defined functional interactions, integration posture,
semantic/context requirements, intended consumptions and constraints.
`strategy-candidate/v1` is the deterministic, read-only compilation result.
It may select a supported strategy or describe a generated strategy candidate,
but it cannot install code or activate a runtime. See
[Source-aware strategy compilation](./source-aware-strategy-compilation.md).

`architecture-proposal/v1` combines that immutable Strategy Candidate with an
`access-plan/v1`. The access plan records the minimum access mechanisms
justified by each typed workload, reuse across consumers, rejected higher-cost
alternatives, estimated projection counts, and the rebuild and retirement
rules for every derived structure. It remains a proposal: no collection,
index, Search definition, projection, stream or export is created by the
compile endpoint.

Each selected consumption interaction is presented through the protocol-neutral
[`governed-data-interaction-pipeline/v1`](./governed-data-interaction-pipeline.md)
lifecycle. Its eight stages keep reference demonstrations, live execution
traces and future Builder-generated paths comparable without hiding policy,
semantic binding, canonical query, MongoDB compilation, physical readiness,
execution or lineage boundaries. The interaction pipeline is a projection of a
larger Solution Blueprint; it is not the whole project canvas.

## Reference Architecture hand-off

An exact `solution-blueprint-template/v1` is the only automatic bridge from a
portfolio Reference Architecture to a legacy internal source blueprint.
Templates contain complete source, ingestion, context, consumption, workload,
constraint, and preferred-strategy defaults plus assumptions that remain
visible to the customer. Category-only portfolio metadata is never expanded
into guessed implementation choices.

`POST /v1/reference-architectures/{architecture_id}/instantiate` accepts an
exact Reference Architecture reference, assumption decisions, and restricted
JSON-Pointer overrides. It returns a content-derived `derivationDigest`, a
valid Blueprint or structured blocking issues, and a review requirement. The
endpoint validates the resulting Blueprint through the normal source-aware
compiler but performs no persistence or activation. The initial executable
hand-off is the canonical openEHR CDR starter; architectures without an exact
template return `not_instantiable`.

## Mapping IR and safe simulation

`mapping-definition/v1` is now the canonical runtime representation for a
governed mapping. YAML can remain an expert authoring format, but the runtime
does not execute arbitrary YAML expressions or embedded code. A mapping pins:

- an exact Source Profile and bounded source interactions;
- the normalized input shape, structures and joins;
- an exact target model, schema and compatible strategies;
- typed source/constant-to-target rules, transforms, semantic bindings,
  terminology references and declared lossiness;
- batch/CDC support, idempotency inputs, ordering, delete behavior and
  error/dead-letter policy.

The first catalog artifact is
`mapping.openmrs-to-openehr.lab-observation@1.0.0`. It maps one normalized,
joined numeric OpenMRS laboratory observation into an openEHR
`sample_laboratory_v0.4` composition. Its maturity is `simulated`; it is not a
live connector and does not promote the broader OpenMRS → openEHR route from
`proposed` to `available`.

`POST /v1/mappings/{mapping_id}/simulate` is deterministic and side-effect
free. It accepts either batch records or CDC after-images, returns canonical
  documents, source document/subject routing keys, stable idempotency keys,
  per-leaf source/transform lineage and typed record errors. The subject key is
  deliberately not relabelled as an openEHR EHR ID; identity resolution owns
  that transition. Deletes fail closed until reconciliation semantics exist.
The same after-image has byte-equivalent output in batch and CDC modes.

## Strategy portfolio policy

`src/kehrnel/engine/strategies/portfolio.json` is the product-level authority
for delivery priority. It is deliberately separate from package manifests:
package maturity describes the package, while the portfolio says what blocks a
Kehrnel release.

- **Primary:** `openehr.rps_dual`, `openehr.rps_dual_ibm`
- **Supported:** `fhir.resource_store`, `cdisc.sdr`
- **Preview:** ContextObjects, genomics, standalone SNOMED and X12 packs

Primary and supported entries are release-blocking. Preview entries must remain
safe and discoverable but do not imply supported runtime behavior. The policy
is available at `GET /v1/strategy-portfolio` and has a published
`strategy-portfolio/v1` JSON Schema. The standalone SNOMED pack being preview
does not demote the provider-neutral terminology gateway used by supported
strategies.

CI enforces this release promise with `scripts/test-supported-strategies`. The
gate exercises the primary openEHR dual and IBM profiles, supported FHIR and
CDISC packs, and the shared activation/API boundary. Preview packs remain
discoverable and demonstrable, but do not block a supported release.

## Published v1 schemas

The committed schemas live in `src/kehrnel/contracts/schemas/v1`. Their
`manifest.json` pins the exact file bytes and a canonical `contractDigest`.
Runtime code reads and verifies that committed manifest; it never regenerates
the digest from whichever Pydantic version happens to be installed.

Regenerate after an intentional contract change:

```bash
PYTHONPATH=src .venv/bin/python scripts/generate_contract_schemas.py
```

The foundation test gate regenerates into a temporary directory and requires a
byte-for-byte match with the committed files.

Test and CI processes set `KEHRNEL_SKIP_DOTENV=true`. Mongo integration suites
run only when their dedicated `FHIR_TEST_*`, `CDISC_TEST_*` or
`KEHRNEL_TEST_*` variables are supplied; generic developer/runtime connection
variables are never an implicit test opt-in.

## Manifest compatibility

Existing manifests load through the tolerant `StrategyManifest` compatibility
reader, including qualified capability strings and extension properties. A new
strict source contract opts in with:

```json
{"manifestContract": "strategy-manifest/v1"}
```

Strict sources reject undeclared fields. Both paths reject authored
`packageDigest`, `contractDigest` and hydrated `pack_spec`; those identities are
runtime-owned. A strict source must declare an import-safe `module:attribute`
entrypoint and an `input_schema` for every operation. Compatibility packs may
retain extensions, but permissive placeholder schemas are reported separately
from constrained operation contracts in the readiness baseline.

## Digest boundaries

- `manifest_digest` remains the legacy activation compatibility fingerprint.
- `packageDigest` identifies the exact regular files in one strategy package
  directory. Presentation metadata is included; line endings are not changed.
- `contractDigest` identifies the committed public schema set.
- A Mapping IR `artifactRef.digest` identifies the exact governed mapping
  definition excluding its self-referential digest field.
- The source-aware compiler owns `candidateDigest`; a later composition
  compiler will own `compositionDigest`.
- The access planner owns `accessPlanDigest`, and the enclosing deterministic
  proposal owns `proposalDigest`.
- HTTP `ETag` identifies a response representation and is never substituted
  for an artifact digest.

Package hashing rejects symlinks, special files and secret-like environment
files. It ignores only explicit VCS, cache, build and temporary artifacts. A
wheel or container digest is separate and is not inferred from the source
package digest.

The present `strategy-directory/v1` boundary is a **source-pack identity**, not
an independently executable identity. Existing strategies import Core,
domain, persistence and vendored-library code outside their strategy
directories. A reproducible executable release must therefore bind the
`packageDigest` to an immutable Core/runtime artifact reference and all
external dependency locks. A later staged-package format may widen that
boundary; it must use a new scope identifier rather than changing v1 meaning.
