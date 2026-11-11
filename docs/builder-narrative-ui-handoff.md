# Builder narrative and UI hand-off

**Audience:** Claude and the Healthcare Data Lab UI team
**Purpose:** translate the Kehrnel product story into an exceptional visual experience
**Design authority:** this document defines the story and user outcomes; Claude retains creative control over the detailed interaction and MongoDB visual style
**Portfolio architecture:** [Outcome catalog and reference architectures](./outcome-catalog-and-reference-architectures.md)

## The one-sentence story

> **Keep the systems you already run. Connect their meaning. Build the MongoDB operational data foundation that your applications and AI need next.**

This is not primarily a standards browser, a terminology demonstrator, a
chatbot over FHIR, or a migration wizard. It is a guided way to turn a
fragmented operational estate into a purpose-built, explainable and scalable
data architecture.

## What the audience should feel

The first reaction should be: “MongoDB understands the complexity of my
industry and has a credible method for turning it into something usable.”

The second reaction should be: “I can start from my business need without
already knowing FHIR, openEHR, CDISC, MQL, vector search or index design.”

The final reaction should be: “This is ambitious, but I can see exactly which
parts work now, which decisions require my approval and what will be built
next.”

## The central product idea

The experience begins with categories, not vendors.

A hospital recognizes EHR, MPI, scheduling, laboratory, imaging, pharmacy,
billing, devices and patient engagement. A pharmaceutical organization
recognizes EDC, CTMS, eTMF, safety, regulatory, laboratory, manufacturing,
quality and supply chain. These are the stable concepts around which the story
is organized.

Products such as OpenMRS and Bahmni appear one level below. They are examples
from which Kehrnel learns real structures, identifiers, events and workflows.
They are not the architecture and should not dominate the first screen.

The user can say either:

- “I use a known implementation”; or
- “My system is different—help me describe or discover it.”

Both paths produce the same kind of Source Profile and enter the same design
journey.

## The experience as a story

### Act 1 — Begin with the outcome

Open with a calm, aspirational question:

> What do you want your data to make possible?

The user can describe the need naturally or start from a proven pattern:

- give an agent a governed longitudinal patient context;
- reduce emergency waiting time across several facilities;
- create a multi-patient clinical query service;
- expose canonical FHIR or openEHR without replacing source systems;
- create a clinical-trial operational view across EDC, CTMS and eTMF;
- connect safety cases, product knowledge and regulatory evidence.

Natural language and graphical selection are complementary. The agent should
translate the conversation into visible structured choices; it must never
hide the architecture behind chat.

### Act 2 — Show the operational landscape

Present the relevant industry categories as a living ecosystem. The user
selects the capabilities involved in the problem, then optionally identifies a
known product, standards-native feed, database or custom system behind each.

For every selected source, make one business choice explicit:

- leave it in place and access it through a governed connection;
- materialize only the context required by this use case;
- maintain an operational replica;
- make MongoDB authoritative for a new or migrated domain.

Selective materialization is the default. Never imply that MongoDB requires a
hospital or pharmaceutical company to replace its entire application estate.

### Act 3 — Add meaning and context

The user decides what the raw operational records need in order to become
useful and safe:

- identity resolution across systems, tenants and facilities;
- terminology and external value-set services;
- documents, protocols, policies and scientific knowledge;
- provenance and version history;
- relationships and graph context;
- vector representations for semantic retrieval;
- agent decision traces, conclusions and episodic or procedural memory.

Terminology is one context capability, not the centre of the entire
experience. SNOMED must not force every journey into “find a FHIR resource by
clinical meaning.”

### Act 4 — Choose how the result will be used

The user selects one or more consumption outcomes:

- operational applications and APIs;
- FHIR APIs and search;
- openEHR REST and AQL;
- CDISC study-data access;
- fast MQL and aggregation;
- cohorts and population queries;
- Search, Vector Search and graph traversal;
- governed agent tools and conversational access;
- events and analytical exports;
- rich role-specific applications.

Sources do not determine the MongoDB model by themselves. The required
consumptions, latency, scale, freshness, tenancy and residency determine the
physical architecture.

### Act 5 — The Data Modeler Agent proposes the architecture

The specialist agent now says, in effect:

> I understand the sources you have, the meanings that must survive and the
> experiences you want to enable. Here is the smallest MongoDB architecture I
> recommend—and here is why.

The proposal appears as an editable architecture, not as prose alone. It
should show:

- which source categories and concrete profiles contribute data;
- what remains authoritative outside MongoDB;
- ingestion or federation paths;
- identity, terminology, mappings and enrichment;
- authoritative MongoDB aggregates and any justified projections;
- indexes, Search, Vector Search and graph structures;
- canonical and custom interfaces;
- data architectures/workflows using those interfaces;
- applications that can be placed on top.

Every recommendation must visibly distinguish fact, inference and assumption.
The user can accept, reject or refine each important decision using direct
controls or natural language. The agent proposes; deterministic Core contracts
compile and validate; a human approves.

### Act 6 — Prove it before asking for commitment

Let the user run a scenario. Show familiar source interactions—register a
patient, book an appointment, place an order, publish a laboratory result,
complete a study visit or open a safety case—and animate how their events
become governed operational context.

Then show the same data serving several outcomes: an AQL query, a FHIR request,
a cohort, an agent answer with evidence, or a rich application screen. The
proof should explain why each MongoDB model and access structure exists.

This is the “wow” moment: one governed foundation preserving complex meaning
while serving very different operational and AI workloads efficiently.

### Act 7 — Turn the proposal into a project

Public visitors can explore categories, describe a need, produce a bounded
proposal and run approved simulations. Authentication is requested only when
they want to save the project, add private source details, create a tenant or
operate a deployment.

The transition should feel natural:

> Save this architecture to your organization and continue building.

## The four product layers

The Builder is the experience that composes all four layers; it is not another
layer itself.

### Layer 0 — Core platform

Kehrnel contracts, SDK/CLI, artifact registry, governance, versioning,
compilation, evidence, security and runtime lifecycle. This layer gives every
higher-level promise credibility, but it should remain mostly behind the
scenes in the public narrative.

### Layer 1 — Data strategies

Reusable technical knowledge for source profiling, semantics, mappings,
persistence, query, indexing and interfaces. This includes product-neutral
source categories, concrete reference profiles and target strategies such as
openEHR RPS Dual, FHIR Resource Store and CDISC SDR.

### Layer 2 — Data architectures and workflows

Compositions that solve business outcomes by orchestrating one or more
strategies. Examples include longitudinal patient context, semantic cohort
discovery, emergency-flow optimization, study operations or safety signal
investigation. A workflow may use an existing strategy or recommend a new
purpose-built strategy when repeated orchestration would not scale.

### Layer 3 — Rich applications

Role-specific experiences built on governed Data Architectures: patient-flow
command centres, cohort explorers, trial-operations workspaces, safety-case
navigators and agent workbenches. The application gallery remains valuable,
but applications are now the visible outcome of the architecture—not isolated
demos.

The discovery catalog above these layers is organized by **Outcome Pattern**,
not by a promise to deliver every application. An outcome leads to a scenario,
interaction requirements, Data Products, a Reference Architecture, and an
honestly labeled Demo Experience. `Open in Builder` creates a new Solution
Blueprint pinned to the selected Reference Architecture version; it never
edits or deploys the catalog record.

## Two industry doors, one platform

Healthcare and Pharmaceutical must have distinct entry points, examples,
language, category maps, patterns and application galleries. Do not introduce
them through a generic industry selector or blend them into “Health and Life
Sciences.”

They share the same Builder mechanics and Kehrnel backend. The shared platform
should become apparent after the audience understands its own industry story,
not before.

## Main interfaces Claude should invent

Claude has creative freedom over layout and motion, using a refined MongoDB
visual language. The experience should nevertheless provide these clear
moments:

1. **Industry landing** — outcome-led invitation, example patterns and the
   category landscape.
2. **Builder** — a light, spacious journey from Need → Sources → Context →
   Uses, with natural language always available.
3. **Modeler Agent** — conversational guidance paired with visible structured
   decisions, assumptions and approvals.
4. **Architecture Proposal** — a comprehensible canvas connecting source
   categories to MongoDB models, interfaces, workflows and applications.
5. **Simulation and Proof** — interactions in motion, live data, queries,
   agent evidence and architecture rationale.
6. **Outcome Gallery** — business outcomes and rich demo experiences made
   possible by linked Reference Architectures, with applications shown as the
   visible result rather than the catalog's unit of architecture.
7. **Manager / Operate** — authenticated, dark-theme operational view for
   connections, CDC, health, releases, evidence and lifecycle.

The Builder should be light, visual and inviting. Operate can retain the dark,
technical Healthcare Data Lab character. The visual shift should communicate
the transition from imagination to running infrastructure.

## Capability truth

The interface must make ambition credible by showing three understandable
states, derived from Core maturity rather than invented in the browser:

- **Available now** — backed by an executable strategy and evidence;
- **Guided proposal** — modeled or simulated, but still requires mapping,
  approval or validation;
- **Planned** — visible as part of the destination architecture, not presented
  as executable.

An unavailable or planned action can still be beautiful and informative, but
must not look clickable as if it already ran. The proposal should explain what
is missing and what would be required to promote it.

The first catalog endpoint exposes raw maturity, support, publication, and
evidence fields. It does not yet return these three summary labels, so the
browser must not calculate **Available now** by association. Server-derived
summaries with contributing and blocking claims are a next Core increment.

## Current Core truth to connect

Available now:

- category-first catalog covering 15 Healthcare and 16 Pharmaceutical source
  categories;
- reference profiles for OpenMRS, Bahmni, FHIR R4 exchange and openEHR
  exchange;
- supported target strategies for openEHR RPS Dual/IBM, FHIR Resource Store
  and CDISC SDR;
- deterministic source-aware Strategy Candidate compilation;
- deterministic consumption-aware access plans with selected and rejected
  alternatives, projection budgets, rebuild paths and retirement conditions;
- direct standards-native routes and explicit approval/blocking diagnostics;
- a governed Mapping IR catalog and a bounded OpenMRS laboratory-to-openEHR
  simulation with batch/CDC equivalence and field lineage;
- existing openEHR/AQL, FHIR, CDISC, terminology and simulation foundations;
- a content-addressed outcome catalog with 30 typed portfolio domains, 26 Data
  Products, 12 architecture capabilities, 16 visual patterns, 121 draft
  Outcome Patterns, nine Reference Architectures, and nine specified Demo
  Experiences.

The nine catalog Demo Experiences are stories, not runnable demonstrations:
all are `specified` and `evaluation_only`, and none currently has
`fixturePackRefs` or `runEvidenceRefs`. Executable or simulated evidence on a
linked strategy or mapping must not turn a gallery card into a “live demo.”

Guided proposal or planned:

- live customer-database discovery through Context Studio;
- complete executable OpenMRS/Bahmni mappings and production CDC connectors;
- the LLM-powered Data Modeler Agent;
- automatic collection, index, shard, Search/Vector Search and API generation;
- coordinated multi-source simulation;
- composite activation, migration, rollback and full multi-tenant Operate;
- automatically generated rich applications.

The UI should consume Core truth from:

```text
GET  /v1/source-categories
GET  /v1/source-profiles
GET  /v1/mappings
GET  /v1/strategy-portfolio
GET  /v1/outcome-catalog
POST /v1/mappings/{mapping_id}/simulate
POST /v1/strategy-candidates/compile
POST /v1/architecture-proposals/compile
```

Separate committed UI fixtures may illustrate planned experiences, but they
must retain their status and must not redefine support, maturity or compiler
decisions locally. The initial outcome catalog deliberately contains no demo
fixture or completed-run references; its storyboard steps must be rendered as
specified previews rather than proof-run controls.

The first complete Builder contract fixture is
`tests/fixtures/builder/healthcare-patient-context-proposal.json`. It shows one
openEHR source serving subject-level AQL and a population cohort. Core reuses
direct document access and one targeted index, introduces one justified
rebuildable projection for the population SLO, and marks benchmark evidence as
an approval item. Claude can use this fixture to design the proposal canvas
without hardcoding those decisions in the UI.

For the Mapping Studio narrative, the catalog exposes a real but deliberately
bounded proof. The UI may let a user inspect its joined OpenMRS inputs, target
openEHR paths, semantic bindings, lossiness notes and field lineage, and may
run batch or CDC-after-image simulation. It must label the artifact
`Simulated` and keep “connect live source” or “activate mapping” unavailable;
the encompassing OpenMRS route is still only proposed.

Source configuration can now select profile-defined `interactionIds`. For the
OpenMRS reference profile, choosing `laboratory-result-observed` makes the
compiler return the exact Mapping Artifact that can be simulated. This is the
intended progressive disclosure: choose a recognizable business interaction
first, then inspect tables, joins and field rules only if useful.

## A Healthcare demonstration story

A regional health system wants an agent and new operational applications to
understand a patient journey across its EHR, MPI, scheduling, laboratory,
imaging and billing systems without replacing them.

The user selects those categories, chooses selective materialization, adds
identity resolution, terminology, protocols and decision traces, then asks for
openEHR/AQL, FHIR, cohorts, agent tools and a patient-flow application. The
Modeler Agent proposes the architecture, marks the known and inferred mappings,
and asks for approval. A simulation shows appointment, arrival, laboratory and
imaging events forming a coherent patient context. The same governed data is
then queried through AQL, used by an evidence-bearing agent and rendered in a
patient-flow command centre.

## A Pharmaceutical demonstration story

A pharmaceutical company wants one operational view of study execution across
EDC, CTMS, eTMF, laboratory and safety systems while keeping each source
authoritative.

The user selects those categories, adds protocol and terminology context, and
asks for CDISC access, study cohorts, agent tools and a trial-operations
workspace. The Modeler Agent proposes how subject, site, visit, specimen,
document and safety context should be connected, highlights controlled
assumptions, and shows which elements are available versus planned. The result
uses the same Kehrnel platform as Healthcare while speaking an entirely
different industry language.

## What this experience must avoid

- Do not start with FHIR, openEHR, CDISC or SNOMED as mandatory choices.
- Do not reduce the proposition to natural-language querying.
- Do not make vendor products the primary catalog hierarchy.
- Do not imply replacement of the source application estate.
- Do not silently generate an opaque “perfect” architecture.
- Do not show planned integrations as executed.
- Do not collapse Healthcare and Pharmaceutical into one landing experience.
- Do not bury the final value: operational applications and governed AI.

## Creative mandate

The experience should feel like designing a living data system, not filling in
a configuration form. Use animation, progressive disclosure, architectural
storytelling and confident MongoDB design language. Keep the first steps
simple enough for an industry executive, then reveal enough evidence and
control for an architect or engineer to trust the result.
