# fhir-data-generation ↔ fhir-search-to-mql alignment (84 resources)

When a resource is shipped in `fhir-search-to-mql` (`configs/<Resource>.yaml`),
**this repo** must list it in `MQL_SHIPPED_RESOURCES` and `CORE_DEPENDENCIES`, and
provide an enricher in the domain module below.

## Single source of truth

| Artifact | Path |
|----------|------|
| Shipped resource list (84) | `fhir_gen/resolvers/dependency.py` → `MQL_SHIPPED_RESOURCES` |
| Generation order / pre-deps | `fhir_gen/resolvers/dependency.py` → `CORE_DEPENDENCIES` |
| Enrichers | `fhir_gen/generators/resources/{clinical,medication,workflow,financial,specialized}.py` |
| Smoke tests | `tests/test_mql_shipped_resources.py` |

Run on change:

```powershell
cd fhir-data-generation
pip install -e .
pytest tests/test_mql_shipped_resources.py -v --no-cov
```

## Per-resource checklist (new MQL config)

- [ ] Add to `MQL_SHIPPED_RESOURCES`
- [ ] Extend `tests/mql_resource_checks.py` enriched-field map
- [ ] Covered by `tests/test_mql_shipped_resources.py` and `tests/test_mql_integration.py`
- [ ] Add `CORE_DEPENDENCIES[<Resource>]`
- [ ] Add `enrich_<Resource>` in the appropriate domain module (see map)
- [ ] Register in that module's `ENRICHERS` dict
- [ ] `pytest tests/test_mql_shipped_resources.py`

## Enricher module map (all 84 MQL resources)

| Module | Resources |
|--------|-----------|
| `clinical.py` | Patient, Practitioner, Organization, Location, Encounter, Condition, Observation, Procedure, DiagnosticReport, Immunization, AllergyIntolerance, FamilyMemberHistory, ClinicalImpression, RiskAssessment, Composition, AdverseEvent, BodyStructure, Person, ImmunizationRecommendation |
| `medication.py` | Medication, MedicationRequest, MedicationAdministration, MedicationDispense, MedicationStatement |
| `workflow.py` | Appointment, CarePlan, CareTeam, Goal, ServiceRequest, Task, Communication, DocumentReference, Schedule, Slot, Flag, Consent, Contract, NutritionOrder, Questionnaire, DeviceRequest, SupplyRequest, SupplyDelivery, RequestOrchestration, VisionPrescription, NutritionIntake, Basic, Provenance |
| `financial.py` | Coverage, Claim, ClaimResponse, Account, Invoice, ChargeItem, CoverageEligibilityRequest, CoverageEligibilityResponse, ExplanationOfBenefit, EnrollmentRequest, EnrollmentResponse, InsurancePlan, ChargeItemDefinition, PaymentNotice, PaymentReconciliation |
| `specialized.py` | Specimen, ImagingStudy, Device, DeviceUsage, DeviceDispense, ResearchStudy, ResearchSubject, QuestionnaireResponse, AuditEvent, EpisodeOfCare, HealthcareService, RelatedPerson, Group, DetectedIssue, Substance, BiologicallyDerivedProduct, OrganizationAffiliation, Endpoint, GenomicStudy, Measure, MeasureReport |
