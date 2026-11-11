# Governed data interaction pipeline

Status: design rule implemented by the Healthcare Data Lab clinical-meaning
experience; intended to become a versioned Kehrnel contract.

## Why this exists

Reference architectures, live demonstrations and customer-built projects must
tell the same architectural story. Protocols will differ, but a FHIR path
cannot expose eleven steps while an equivalent openEHR path hides most of its
work inside one MongoDB operation.

The stable abstraction is `governed-data-interaction-pipeline/v1`. It describes
one selected interaction from intent to evidence. It is not the whole project
canvas and it is not a replacement for Strategy manifests, Mapping IR,
Solution Blueprints or Access Plans.

## Invariant lifecycle

Every governed interaction exposes these eight ordered stages:

1. **Interpret the question** — convert the user or application request into a
   typed, bounded intent.
2. **Apply policy and select strategy** — authorize the purpose and result
   shape, then bind the request to an explicit interaction contract and
   strategy.
3. **Resolve terminology and bind the data model** — resolve external or local
   meaning and bind it to concrete model paths. FHIR binds resources and search
   parameters; openEHR binds OPT templates, archetypes and AQL paths.
4. **Express the canonical query** — produce the portable, domain-facing
   expression when one exists, such as FHIR Search or AQL.
5. **Compile the MongoDB execution** — deterministically translate the typed
   request into MQL, an aggregation pipeline or another bounded MongoDB plan.
6. **Verify projections and indexes** — prove that the physical structures
   selected by the plan exist, are current and match the model version.
7. **Execute query and assemble result** — execute the plan and assemble the
   bounded record or subject result required by the interaction contract.
8. **Cite evidence and form the answer** — preserve record lineage and return a
   bounded answer or typed application response.

The eight stages are grouped into four audience-friendly phases:

- Understand and govern
- Resolve and express meaning
- Compile and optimize for MongoDB
- Execute and answer

Each stage may report multiple internal operations. For example, governance
includes policy evaluation and strategy selection, while result assembly may
include document matching and subject roll-up. These operations remain visible
in the stage input, output and evidence; they are not presented as independent
top-level steps unless a future version identifies a new architectural
boundary.

## Representation rules

1. The linear trace is the source of truth for the execution path.
2. The execution graph is generated from that trace: one node per stage, the
   same identifiers, titles, order and runtime status.
3. A stage is never removed because an implementation is incomplete. It is
   labelled planned, unavailable, configured, executed or failed.
4. An executed stage has runtime evidence. UI inference cannot promote a stage
   to executed.
5. Protocol-specific detail belongs inside stage evidence, not in a different
   top-level journey.
6. The generated graph is not independently editable. Editing happens in the
   Builder's project model and recompilation produces a new pipeline.

## Relationship to the portal and Builder

The portal has three connected representations:

1. **Reference Architecture** — an industry story and reusable blueprint,
   showing sources, ingestion, mappings, target objects, context/enrichment,
   access structures and consumption endpoints.
2. **Builder Project Canvas** — an editable customer architecture created from
   a reference architecture or from scratch. It retains the full project graph
   and may contain many sources and consumption endpoints.
3. **Interaction Pipeline** — the eight-stage executable path generated for one
   selected endpoint or workflow in that project.

Therefore a project with FHIR Search, openEHR AQL, a cohort API and an agent
tool has one project canvas and at least four interaction pipelines. Shared
ingestion and physical structures can be reused, while each interaction keeps
its own policy, query, execution and evidence contract.

The intended user journey is:

`discover reference → open in Builder → configure project → compile → inspect
each interaction pipeline → simulate or execute → validate evidence → save as
a tenant pattern`

## Evolution rule

The stage keys and meanings are versioned product vocabulary. A future
architecture can add protocol-specific evidence and sub-operations without a
contract version change. Adding, removing, reordering or redefining a top-level
stage requires a new pipeline contract version and migration guidance.
