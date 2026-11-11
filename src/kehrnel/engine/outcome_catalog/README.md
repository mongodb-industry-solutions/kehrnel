# Industry outcome catalog seed

`catalog.json` is the first repo-native normalization of the Healthcare
Industry Architecture Matrix dated 27 September 2026. It preserves all 121
workbook application IDs as `outcome.appNNN` artifacts, together with the 30
portfolio domains, 26 logical data products, 12 reusable architecture
capabilities, and 16 visual patterns used to describe them. Those definitions
remain separate from a deliberately small set of nine reference architectures
and nine demo experiences. Portfolio domains, Data Products, architecture
capabilities and visual patterns are typed, content-addressed D/P/B/V
definitions rather than unchecked workbook codes.

The catalog is discovery and planning input. It is not an instruction to
deploy a source connector or to treat a storyboard as runtime proof. All
imported outcomes therefore remain `draft`, `evaluation_only`, and
`not_assessed`. Reference architectures raise their maturity only when an
exact current Kehrnel strategy or mapping artifact is attached. The openEHR
runtime is the primary executable proof; FHIR and CDISC are supported
follow-ups. Every demo experience remains `specified`: executable strategy or
mapping evidence beneath an architecture is not evidence that its visual demo
has been simulated. Operational and agentic lighthouse compositions without
runtime evidence also remain specified concepts.

The catalog publishes three exact, content-addressed gallery-to-Builder
hand-offs in the separate `solution-blueprint-template-catalog/v1`:

- `template.healthcare.openehr-cdr-starter@1.0.0` uses the exact openEHR
  exchange profile and `openehr.rps_dual` for canonical AQL and population
  access;
- `template.healthcare.fhir-repository-starter@1.0.0` uses the exact FHIR R4
  exchange profile and `fhir.resource_store` for CapabilityStatement-bounded
  create, update, read, search, and bounded indexed population filtering; and
- `template.pharmaceutical.cdisc-sdr-starter@1.0.0` uses the exact CDISC
  Dataset-JSON 1.1 exchange profile and `cdisc.sdr` for controlled intake,
  bounded `cdisc-query/v1` access, lineage inspection, and reviewed snapshot
  export.

Each template supplies explicit defaults and visible assumptions rather than
inventing customer facts. Instantiation preserves the exact
architecture/template references, assumption decisions, and allowed overrides
in a result with a deterministic derivation digest. The result also echoes the
requested Blueprint ID/revision and binds the complete canonical request
through `requestDigest`, including for `not_instantiable` outcomes. Kehrnel
validates the derived Blueprint through the source-aware compiler but does not
persist or activate it.

The seam fails closed: template integrity is rechecked at instantiation,
cached catalogs are isolated from caller mutation, override values use the
canonical JSON subset, assumption citations must match the paths being
changed, and preferred strategies remain within the Reference Architecture's
exact strategy references. Missing or incompatible template/compiler
dependencies return a stable `503` instead of a partial Blueprint.

Reference Architectures without one of these exact templates remain useful for
discovery but return `not_instantiable`; the runtime never invents a concrete
source, connector, mapping, ingestion mode, workload, or topology. The FHIR and
CDISC starters do not make their linked Demo Experiences executable: those
experiences remain separately labelled and require their own fixture and run
evidence.

The current public experience should expose two intentional doors:
`healthcare` and `pharmaceutical`. The `medtech` and `diagnostics` values in
the catalog preserve the matrix's future portfolio facets; a UI must not infer
that every `industryExperienceId` is already a public landing page.

The source workbook is editorial input, not runtime authority. Future edits
should happen through reviewed, versioned catalog artifacts (and eventually a
catalog authoring service), with spreadsheets generated as portfolio views.
Source fixture names from the workbook were intentionally not imported as
`fixturePackRefs`, because they describe files to create rather than existing
packaged fixtures. All nine demos therefore have empty `fixturePackRefs` and
`runEvidenceRefs`; the contract permits promotion to `simulated` only after
both a packaged fixture and completed-run evidence exist.

Every catalog artifact is versioned and content-addressed. Reverse mappings
from data products and capabilities use exact outcome references, scenario
stories explicitly name the interactions they exercise through required
`interactionIds`, and reference architectures use exact `sourceCategoryRefs`.
The top-level `artifactRef` identifies `industry-outcomes@0.1.0`, matches the
catalog ID and version, and content-addresses the complete read model so a
consumer can detect any unreviewed vocabulary or relationship change.
