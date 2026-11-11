# Clinical intent resolution architecture

Status: foundation contracts, terminology orchestration, native FHIR/openEHR
structural adapters, and deterministic native-query compilation are implemented.

## Decision

Kehrnel uses one governed resolution lifecycle with two independent binding
planes:

1. **Structural binding** proves where a semantic role is represented and how
   predicates must correlate. A known strategy resolves through its native
   compiled catalog. A custom source resolves through a published semantic pack
   compiled from Context Studio.
2. **Terminology grounding** proves what a coded mention means. It routes through
   the tenant terminology gateway to a pinned native or external provider.

Neither plane can substitute for the other. An openEHR archetype can prove that
a field is a diagnosis code inside one problem instance, but SNOMED proves what
`84114007` means and what its descendants are. The canonical query plan joins
both receipts before any AQL, FHIR Search, MQL, or aggregation can execute.

## Production lifecycle

```text
human/application request
  -> interaction-profile selection
  -> typed clinical-cohort intent
  -> policy and resolution-route selection
  -> terminology candidate recall and authoritative verification
  -> native-strategy or compiled-semantic-pack structural binding
  -> ambiguity decision: ready | clarify | unsupported
  -> canonical query plan
  -> strategy-native query compiler
  -> projection/index readiness
  -> execution, lineage, and bounded response
```

The first interaction profile is deliberately bounded to cross-patient cohort
selection. Its Boolean tree supports `all`, `any`, and unary `not`; leaves carry
semantic roles, unresolved concept mentions, scalar and temporal constraints,
and explicit correlation groups. This makes combinations and negation explicit
without allowing a model to generate query text.

The contract can describe more than the first compilers execute. Today the FHIR
and openEHR compiler profiles accept proven conjunctions and reject `any`/`not`.
This is deliberate: the stable intent does not need to be redesigned when a
strategy later proves those operators, while the current runtime cannot
silently simplify them.

## Deterministic and model-assisted resolution

The LLM is a proposal mechanism, not an execution mechanism:

1. A profile-specific parser first extracts the bounded operation, Boolean
   structure, temporal constraints, and unresolved concept phrases.
2. Deterministic terminology search, accepted aliases, ValueSets, and native
   strategy catalogs are tried first.
3. A model may be invoked when syntax is not covered, candidate recall is empty,
   or the top verified candidates remain ambiguous. It proposes a typed intent
   or additional candidates; it does not produce AQL, FHIR Search, MQL, or paths.
4. The same schema validation, provider lookup, score/margin policy, structural
   binding, and compiler gates apply regardless of how a proposal was produced.
5. Ambiguity produces a clarification result with alternatives. Provider or
   projection failure is an operational failure, not evidence that the user
   meant something else.

This is the seam where a future MongoDB Agentic Engine assistant plugs in. It
can coordinate parsers and candidate-generating tools, retain the conversation,
and explain a plan; the deterministic Kehrnel contracts remain the authority.

## Known strategies

Known strategies do not query a general semantic map to rediscover paths on the
hot path.

- `fhir.resource_store` binds cohort roles through its governed resource/search
  catalog and then invokes its existing FHIR-to-MQL compiler.
- `openehr.rps_dual` binds roles through an exact OPT semantic catalog, including
  archetype, RM type, queryable facet, AQL path, containment, and correlation.
- Other known strategies register equivalent native structural resolvers.

Context Studio remains valuable for discovery, polymorphism analysis,
reconciliation, authoring, evaluation, and human adjudication. Its accepted
artifacts compile into a versioned semantic pack. Kehrnel loads the pack; the
Studio application is not a runtime dependency.

## Terminology routing

Imported packages may contribute declared CodeSystems, ValueSets, ConceptMaps,
aliases, archetype terms, and external term bindings. Finite, redistributable,
versioned content may be compiled into a local terminology projection that
supports lookup, validation, and expansion.

Observed values never become an authoritative ValueSet automatically. Large,
licensed, intensional, or externally governed systems remain provider
dependencies. SNOMED CT therefore normally routes to the native Kehrnel SNOMED
strategy or a licensed external service with a pinned release.

The gateway adapter follows these rules:

- explicit code: authoritative lookup or ValueSet validation;
- free text with a ValueSet: filtered expansion, then code validation;
- free text with a searchable native system: candidate search, then lookup;
- candidate recall alone never sets `verified=true`;
- several close verified candidates require clarification;
- an LLM proposal cannot cross the execution boundary without provider
  verification and deterministic adjudication.

## Implemented contracts

- `clinical-cohort-intent/v1` is the protocol-neutral, bounded Boolean intent.
- `clinical-cohort-resolution/v1` combines the route, terminology receipts,
  structural receipt, decision, and reasons.
- confidence and ambiguity use integer basis points for deterministic,
  cross-language canonicalization.
- `GatewayTerminologyCandidateProvider` reuses Kehrnel's tenant terminology
  gateway, verifies recalled candidates, and emits a complete version-pinned
  expansion with a deterministic digest. Truncated expansions fail closed.
- `CohortResolutionOrchestrator` stops before structural binding when meaning is
  ambiguous, unverified, or unsupported.
- `ResolutionRegistry` requires an explicit structural resolver and performs no
  cross-strategy fallback.
- `FhirConditionCohortStructuralResolver` proves the current Condition plus
  onset-date profile against the native FHIR search contract.
- `OpenEhrOptCohortStructuralResolver` binds approved semantic roles to exact
  field and facet identifiers in one immutable OPT semantic catalog.
- `compile_fhir_condition_cohort` and `compile_openehr_cohort` translate only a
  ready resolution into the existing FHIR and OPT-to-AQL query contracts.

The first executable slice intentionally supports conjunctions only. The intent
contract can represent `any` and `not`, but a target strategy must explicitly
prove and implement those semantics before compilation. They are never silently
flattened into an `AND` query.

## Next integration slices

1. Expose the unified resolution and compilation service through a tenant-scoped
   API and connect it to the Data Lab trace.
2. Add clarification receipts and explicit human approval to the Data Lab trace.
3. Import the minimum Context Studio semantic-pack runtime contract for custom
   sources.
4. Add a MongoDB Agentic Engine parser only as a proposal lane behind the same
   validation and adjudication gates.
5. Add explicit, tested compiler profiles for disjunction, negation, multiple
   condition occurrences, and additional FHIR/openEHR data domains.
