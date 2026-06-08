# FHIR Search-to-MQL — Gap Resource Implementation Prompts

Master backlog for shipping **practical-use** FHIR R5 resources that were
missing from the original 54-config set. Execute **one resource per
agent session** using `.cursor/skills/fhir-resource-config/SKILL.md`
(local schema only — no external spec URLs).

**Status baseline:** **84 configs shipped** (54 original + 30 gap batch).
All phases below are **Done** except documenting deferred composites.

---

## Per-resource checklist (copy for each session)

```text
Resource: <Resource>
- [ ] resource_spec <Resource> + compartment grep
- [ ] src/fhir_search_to_mql/configs/<Resource>.yaml
- [ ] python -m fhir_search_to_mql.schema.build_indexes
- [ ] tests/integration/test_<snake>_comprehensive.py
- [ ] test_config_audit_regressions.py (sample + ALL_AUDITED_RESOURCES x2)
- [ ] test_legacy_shape_denormalization.py (if CodeableConcept token)
- [ ] CLI_COMMANDS.md (count, list, examples)
- [ ] fhir-data-generation: enricher + CORE_DEPENDENCIES (see PROMPTS_FHIR_MQL_GAP_DATA_GENERATION.md)
- [ ] pytest -k "<Resource>" --no-cov
```

---

## Phase 1 — Clinical & financial core

| Resource | Params | Status | Notes |
|----------|--------|--------|-------|
| Composition | 22 | **Done** | Defer `section-code-text` composite |
| MedicationStatement | 13 | **Done** | `effectiveTiming` not indexed for date overlap |
| Questionnaire | 23 | **Done** | Defer 3 context composites; `useContext` token workaround |
| ExplanationOfBenefit | 19 | **Done** | Large backbone; mirror Claim patterns |
| CoverageEligibilityRequest | 9 | **Done** | Enricher exists in fhir-gen |
| CoverageEligibilityResponse | 11 | **Done** | |
| DeviceRequest | 21 | **Done** | `event-date` in parameter_parser |
| AdverseEvent | 16 | **Done** | |
| ImmunizationRecommendation | 10 | **Done** | |
| Person | 25 | **Done** | Not Patient — demographics resource |
| BodyStructure | 7 | **Done** | |

---

## Phase 2 — Devices, supplies, derived products

| Resource | Params | Status | Notes |
|----------|--------|--------|-------|
| DeviceUsage | 6 | **Done** | |
| DeviceDispense | 7 | **Done** | |
| SupplyDelivery | 7 | **Done** | |
| SupplyRequest | 10 | **Done** | |
| BiologicallyDerivedProduct | 10 | **Done** | CodeableReference patterns |

---

## Phase 3 — Payer enrollment & interop

| Resource | Params | Status | Notes |
|----------|--------|--------|-------|
| OrganizationAffiliation | 16 | **Done** | |
| Endpoint | 8 | **Done** | URI search |
| Provenance | 15 | **Done** | |
| EnrollmentRequest | 6 | **Done** | |
| EnrollmentResponse | 5 | **Done** | |
| InsurancePlan | 16 | **Done** | |
| ChargeItemDefinition | 17 | **Done** | Defer 3 context composites |
| Basic | 8 | **Done** | |

---

## Phase 4 — Specialty & quality (add when product needs)

| Resource | Params | Status | Notes |
|----------|--------|--------|-------|
| VisionPrescription | 8 | **Done** | |
| NutritionIntake | 11 | **Done** | |
| RequestOrchestration | 17 | **Done** | |
| GenomicStudy | 7 | **Done** | |
| Measure | 24 | **Done** | Defer 3 context composites |
| MeasureReport | 12 | **Done** | |

---

## Explicitly out of scope (do not ship unless product asks)

Terminology/conformance: CodeSystem, ValueSet, StructureDefinition,
CapabilityStatement, ImplementationGuide, Subscription*, Binary, Bundle,
Parameters, SearchParameter, etc.

Definition/catalog resources: *Definition, Ingredient, FormularyItem, etc.
(158 schema types still generate via fhir-gen without MQL configs).

---

## Verification (after each phase)

```powershell
cd fhir-search-to-mql
$env:FHIR_SCHEMA_ROOT = "schema"; $env:PYTHONPATH = "src"
python -m fhir_search_to_mql.schema.build_indexes
python -m pytest tests/integration -q --no-cov --ignore=tests/integration/test_performance.py
```

Cross-repo smoke (fhir-data-generation):

```powershell
cd fhir-data-generation
pip install -e .
pytest tests/test_mql_gap_resources.py -v
```
