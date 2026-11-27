# CLI & command reference

Practical commands for **fhir-mql**, Python APIs, schema tooling, and common
healthcare search workflows. All examples assume an activated venv and
`pip install -e ".[dev]"` from the repo root.

**Shell:** PowerShell on Windows; use `export VAR=value` instead of `$env:VAR` on bash.

---

## Table of contents

1. [Environment & connection](#environment--connection)
2. [Install & verify](#install--verify)
3. [Resource inventory (84 shipped)](#resource-inventory-84-shipped)
4. [Convert only (no MongoDB)](#convert-only-no-mongodb)
5. [Search (convert + execute)](#search-convert--execute)
6. [Bulk operations](#bulk-operations)
7. [Compartment-scoped queries](#compartment-scoped-queries)
8. [Healthcare workflow scenarios](#healthcare-workflow-scenarios)
9. [Industrial & enterprise scenarios](#industrial--enterprise-scenarios)
10. [Multi-database / project presets](#multi-database--project-presets)
11. [Python API one-liners](#python-api-one-liners)
12. [FHIR schema tooling](#fhir-schema-tooling)
13. [Testing commands](#testing-commands)
14. [Troubleshooting commands](#troubleshooting-commands)

---

## Environment & connection

```powershell
# Defaults used when flags are omitted
$env:MONGODB_URI = "mongodb://localhost:27017/"
$env:MONGODB_DB  = "fhir_synthetic"

# Shorthand for repeated commands
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_synthetic"
```

| Flag / variable | Purpose | Default |
|-----------------|---------|---------|
| `--uri` / `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `--db` / `MONGODB_DB` | Database name | `fhir_synthetic` |
| `--config-dir DIR` | Layer YAML configs (repeatable) | Bundled `src/fhir_search_to_mql/configs/` |
| `--compartment-definitions-dir DIR` | CompartmentDefinition JSON | Bundled definitions |
| `--collection-prefix PREFIX` | Collection name = `PREFIX` + resource type | *(none)* |
| `--format {json,table}` | Output shape | `table` |
| `--dry-run` | Plan only (bulk subcommands) | off |
| `--no-with-deps` | Bulk ops: only named types (no fhir-gen dependency expansion) | off |
| `--limit N` | Cap documents (search / denormalize) | unlimited |
| `--batch-size N` | Denormalize batch size | 500 |

**Docker MongoDB (local dev):**

```powershell
docker run -d --name mongo-fhir -p 27017:27017 mongo:7
python -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017/').server_info()['version'])"
```

---

## Install & verify

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -e ".[dev,docs]"

fhir-mql --version
fhir-mql resources
python -c "from fhir_search_to_mql import ConfigLoader; print(sorted(ConfigLoader().list_resources()))"
```

---

## Resource inventory (84 shipped)

```powershell
fhir-mql resources
fhir-mql resources --format json
python -c "from fhir_search_to_mql import ConfigLoader; print(len(ConfigLoader().list_resources()))"
```

Every resource below has a YAML config under `src/fhir_search_to_mql/configs/`.
Use `fhir-mql convert <Resource> "…"` to preview MQL; use `search` after denormalize.

### By healthcare / industrial domain

| Domain | Resources | Typical use |
|--------|-----------|-------------|
| **Identity & directory** | Patient, Person, Practitioner, PractitionerRole, RelatedPerson, Organization, OrganizationAffiliation, Location, Endpoint, HealthcareService, Group | MPI, provider directory, facility registry, network endpoints |
| **Scheduling & access** | Appointment, Schedule, Slot, Encounter, EpisodeOfCare | booking, capacity, visit management, care episodes |
| **Clinical record** | Condition, AllergyIntolerance, Observation, DiagnosticReport, ImagingStudy, Specimen, ClinicalImpression, FamilyMemberHistory, BodyStructure, Composition, DocumentReference | problem list, labs, imaging, assessments, notes, attachments |
| **Medications** | Medication, MedicationRequest, MedicationAdministration, MedicationDispense, MedicationStatement, Substance | e-prescribing, MAR, dispensing, home med list, formulary |
| **Orders & care delivery** | ServiceRequest, Procedure, DeviceRequest, RequestOrchestration, CarePlan, CareTeam, Goal, Task, NutritionOrder, NutritionIntake, VisionPrescription | orders, procedures, devices, care plans, workflows |
| **Devices & supplies** | Device, DeviceUsage, DeviceDispense, SupplyRequest, SupplyDelivery, BiologicallyDerivedProduct | asset tracking, utilization, inventory, blood/tissue |
| **Immunizations** | Immunization, ImmunizationRecommendation | IIS, forecast, clinic campaigns |
| **Safety & quality** | AdverseEvent, DetectedIssue, Flag, RiskAssessment, Measure, MeasureReport | pharmacovigilance, CDS alerts, HEDIS/eCQM |
| **Financial / RCM** | Coverage, CoverageEligibilityRequest, CoverageEligibilityResponse, Claim, ClaimResponse, ExplanationOfBenefit, Account, Invoice, ChargeItem, ChargeItemDefinition, PaymentNotice, PaymentReconciliation | eligibility, claims, EOB, patient accounting |
| **Payer & enrollment** | EnrollmentRequest, EnrollmentResponse, InsurancePlan | member enrollment, plan catalog |
| **Research & genomics** | ResearchStudy, ResearchSubject, GenomicStudy | trials, biobank, molecular results |
| **Forms & PROs** | Questionnaire, QuestionnaireResponse | assessments, PROMs, screening |
| **Privacy & legal** | Consent, Contract | consent directives, BAAs |
| **Interop & audit** | AuditEvent, Provenance, Communication, Basic | security audit, lineage, messaging, extensions |

### Alphabetical index (param count)

| Resource | Params | Resource | Params | Resource | Params |
|----------|--------|----------|--------|----------|--------|
| Account | 12 | AdverseEvent | 17 | AllergyIntolerance | 17 |
| Appointment | 23 | AuditEvent | 17 | Basic | 8 |
| BiologicallyDerivedProduct | 10 | BodyStructure | 7 | CarePlan | 20 |
| CareTeam | 10 | ChargeItem | 20 | ChargeItemDefinition | 14 |
| Claim | 19 | ClaimResponse | 13 | ClinicalImpression | 14 |
| Communication | 18 | Composition | 21 | Condition | 24 |
| Consent | 20 | Contract | 12 | Coverage | 15 |
| CoverageEligibilityRequest | 9 | CoverageEligibilityResponse | 11 | DetectedIssue | 11 |
| Device | 23 | DeviceDispense | 7 | DeviceRequest | 21 |
| DeviceUsage | 6 | DiagnosticReport | 19 | DocumentReference | 35 |
| Encounter | 29 | Endpoint | 8 | EnrollmentRequest | 6 |
| EnrollmentResponse | 5 | EpisodeOfCare | 14 | ExplanationOfBenefit | 19 |
| FamilyMemberHistory | 11 | Flag | 10 | GenomicStudy | 7 |
| Goal | 13 | Group | 12 | HealthcareService | 17 |
| ImagingStudy | 19 | Immunization | 18 | ImmunizationRecommendation | 10 |
| InsurancePlan | 16 | Invoice | 15 | Location | 17 |
| Measure | 21 | MeasureReport | 12 | Medication | 12 |
| MedicationAdministration | 17 | MedicationDispense | 19 | MedicationRequest | 18 |
| MedicationStatement | 13 | NutritionIntake | 11 | NutritionOrder | 14 |
| Observation | 44 | Organization | 15 | OrganizationAffiliation | 16 |
| Patient | 25 | PaymentNotice | 9 | PaymentReconciliation | 12 |
| Person | 25 | Practitioner | 22 | PractitionerRole | 17 |
| Procedure | 19 | Provenance | 15 | Questionnaire | 20 |
| QuestionnaireResponse | 14 | RelatedPerson | 21 | RequestOrchestration | 17 |
| ResearchStudy | 27 | ResearchSubject | 9 | RiskAssessment | 12 |
| Schedule | 11 | ServiceRequest | 25 | Slot | 11 |
| Specimen | 14 | Substance | 10 | SupplyDelivery | 7 |
| SupplyRequest | 10 | Task | 24 | VisionPrescription | 8 |

**Deferred search params (documented in YAML):** Composition `section-code-text`;
Questionnaire / ChargeItemDefinition / Measure — three `useContext` composite/quantity params.

---

## Convert only (no MongoDB)

Pure FHIR search → MQL JSON. Use for query review, CI, or app integration without DB.

**Convention:** `patient=…` is the Patient/* shortcut where the spec defines it; otherwise use
`subject=…`. Dates support prefixes `eq`, `ne`, `gt`, `lt`, `ge`, `le`, `sa`, `eb`, `ap`.
Token parameters accept `system|code` or bare code when system is implicit in data.

### Common parameters (all 84 resources)

```powershell
# _id / _lastUpdated (all resources)
fhir-mql convert Patient "_id=p1"
fhir-mql convert Observation "_lastUpdated=ge2024-01-01"

# Modifiers
fhir-mql convert Patient "name:exact=Smith"
fhir-mql convert Patient "identifier:missing=false"
fhir-mql convert Appointment "status:not=cancelled"
```

### ResearchSubject — trial participants

```powershell
fhir-mql convert ResearchSubject "patient=pat-1&study=rs-1&status=active"
fhir-mql convert ResearchSubject "subject=pat-1&subject_state=on-study"
fhir-mql convert ResearchSubject "identifier=RSUB-001&date=ge2024-01-01"
```

### Composition — clinical documents

```powershell
fhir-mql convert Composition "patient=pat-1&status=final&type=18842-5"
fhir-mql convert Composition "author=prac-1&encounter=enc-1&section=48767-8"
fhir-mql convert Composition "entry=obs-1&event-code=admission&period=ge2024-07-01"
fhir-mql convert Composition "identifier=COMP-001&title=Discharge"
```

### Questionnaire — form definitions

```powershell
fhir-mql convert Questionnaire "status=active&questionnaire-code=44249-1"
fhir-mql convert Questionnaire "combo-code=item-code-1&context=ambulatory"
fhir-mql convert Questionnaire "effective=ge2024-01-01&jurisdiction=US"
fhir-mql convert Questionnaire "url=http://example.org/Questionnaire/phq9&version=1.0"
```

### ExplanationOfBenefit — adjudicated claims

```powershell
fhir-mql convert ExplanationOfBenefit "patient=pat-1&status=active&claim=clm-1"
fhir-mql convert ExplanationOfBenefit "coverage=cov-1&provider=prac-1&encounter=enc-1"
fhir-mql convert ExplanationOfBenefit "payee=prac-1&created=ge2024-07-01"
fhir-mql convert ExplanationOfBenefit "identifier=EOB-001&disposition=processed"
```

### CoverageEligibilityRequest — eligibility inquiries

```powershell
fhir-mql convert CoverageEligibilityRequest "patient=pat-1&status=active"
fhir-mql convert CoverageEligibilityRequest "provider=prac-1&enterer=prac-2&facility=loc-1"
fhir-mql convert CoverageEligibilityRequest "identifier=CER-001&created=ge2024-07-01"
```

### CoverageEligibilityResponse — eligibility outcomes

```powershell
fhir-mql convert CoverageEligibilityResponse "patient=pat-1&request=cer-1&outcome=complete"
fhir-mql convert CoverageEligibilityResponse "insurer=org-1&requestor=prac-1&status=active"
fhir-mql convert CoverageEligibilityResponse "identifier=CERES-001&disposition=eligible"
```

### OrganizationAffiliation — org network links

```powershell
fhir-mql convert OrganizationAffiliation "primary-organization=org-1&participating-organization=org-2"
fhir-mql convert OrganizationAffiliation "role=provider&specialty=cardio&active=true"
fhir-mql convert OrganizationAffiliation "service=hs-1&location=loc-1&date=ge2024-01-01"
fhir-mql convert OrganizationAffiliation "identifier=OAF-001&email=affil@example.org"
```

### Endpoint — technical service endpoints

```powershell
fhir-mql convert Endpoint "status=active&connection-type=hl7-fhir-rest"
fhir-mql convert Endpoint "organization=org-1&name=fhir&payload-type=application/fhir+json"
fhir-mql convert Endpoint "identifier=EP-001"
```

### Provenance — resource lineage

```powershell
fhir-mql convert Provenance "patient=pat-1&target=obs-1&agent=prac-1"
fhir-mql convert Provenance "activity=CREATE&recorded=ge2024-07-01&when=ge2024-07-01"
fhir-mql convert Provenance "encounter=enc-1&entity=dev-1&signature-type=ProofOfOrigin"
```

### EnrollmentRequest / EnrollmentResponse — payer enrollment

```powershell
fhir-mql convert EnrollmentRequest "patient=pat-1&status=active&identifier=ENR-001"
fhir-mql convert EnrollmentResponse "request=enr-1&status=active&identifier=ENRES-001"
```

### InsurancePlan — plan catalog

```powershell
fhir-mql convert InsurancePlan "status=active&name=gold&type=medical"
fhir-mql convert InsurancePlan "owned-by=org-1&administered-by=org-2&address-city=boston"
fhir-mql convert InsurancePlan "identifier=IP-001&phonetic=G530"
```

### ChargeItemDefinition — billing code definitions

```powershell
fhir-mql convert ChargeItemDefinition "status=active&url=http://example.org/ChargeItemDefinition/lab-panel"
fhir-mql convert ChargeItemDefinition "context=ambulatory&jurisdiction=US&effective=ge2024-01-01"
fhir-mql convert ChargeItemDefinition "title=Lab Panel&publisher=Acme&version=1.0"
```

### Basic — custom/extension resources

```powershell
fhir-mql convert Basic "patient=pat-1&code=referral&author=prac-1"
fhir-mql convert Basic "subject=pat-1&created=ge2024-07-01&identifier=BASIC-001"
```

### VisionPrescription — eyewear authorization (8 params)

```powershell
fhir-mql convert VisionPrescription "patient=pat-1&prescriber=prac-1&status=active"
fhir-mql convert VisionPrescription "encounter=enc-1&datewritten=ge2024-07-01&identifier=VP-001"
```

### NutritionIntake — food/fluid consumption (11 params)

```powershell
fhir-mql convert NutritionIntake "patient=pat-1&status=completed&nutrition=apple"
fhir-mql convert NutritionIntake "source=prac-1&encounter=enc-1&date=ge2024-07-01"
```

### RequestOrchestration — dependent request sets (17 params)

```powershell
fhir-mql convert RequestOrchestration "patient=pat-1&status=active&intent=order"
fhir-mql convert RequestOrchestration "author=prac-1&participant=part-1&authored=ge2024-07-01"
```

### GenomicStudy — genomic analysis (7 params)

```powershell
fhir-mql convert GenomicStudy "patient=pat-1&status=registered&focus=cond-1"
fhir-mql convert GenomicStudy "subject=pat-1&identifier=GS-001"
```

### Measure — quality measure definitions (21 shipped / 24 index; defer 3 context composites)

```powershell
fhir-mql convert Measure "status=active&title=Diabetes&context=ambulatory"
fhir-mql convert Measure "depends-on=lib-1&url=http://example.org/Measure/diabetes&version=1.0"
```

### MeasureReport — measure calculation results (12 params)

```powershell
fhir-mql convert MeasureReport "patient=pat-1&status=complete&measure=diabetes"
fhir-mql convert MeasureReport "reporter=prac-1&period=ge2024-07-01&evaluated-resource=obs-1"
```

### ResearchStudy — clinical trials

```powershell
fhir-mql convert ResearchStudy "status=active&phase=phase-2&condition=38341003"
fhir-mql convert ResearchStudy "title=Trial&site=loc-1&protocol=pd-1"
fhir-mql convert ResearchStudy "focus-code=med-focus&focus-reference=med-1"
fhir-mql convert ResearchStudy "recruitment-actual=42&recruitment-target=100"
fhir-mql convert ResearchStudy "progress-status-state-actual=recruiting$true"
fhir-mql convert ResearchStudy "date=ge2024-01-01&identifier=RS-001"
```

### Invoice — billing invoices

```powershell
fhir-mql convert Invoice "patient=pat-1&status=issued&type=invoice"
fhir-mql convert Invoice "account=acct-1&issuer=org-1&recipient=rp-1"
fhir-mql convert Invoice "participant=prac-1&participant-role=author"
fhir-mql convert Invoice "totalgross=500&totalnet=450&date=ge2024-07-01"
fhir-mql convert Invoice "identifier=INV-001&subject=pat-1"
```

### ChargeItem — billing charges

```powershell
fhir-mql convert ChargeItem "patient=pat-1&status=billable&code=99213"
fhir-mql convert ChargeItem "encounter=enc-1&enterer=prac-1&performer-actor=prac-perf"
fhir-mql convert ChargeItem "service=proc-1&account=acct-1&occurrence=ge2024-07-01"
fhir-mql convert ChargeItem "factor-override=1.5&price-override=120&quantity=1"
fhir-mql convert ChargeItem "performer-function=performer&identifier=CI-001"
```

### Account — financial accounts

```powershell
fhir-mql convert Account "patient=pat-1&status=active"
fhir-mql convert Account "subject=pat-1&type=PBILLACCT&owner=org-1"
fhir-mql convert Account "guarantor=rp-1&relatedaccount=acct-parent&identifier=ACCT-001"
fhir-mql convert Account "period=ge2024-07-01&name:exact=Inpatient Account"
```

### PaymentReconciliation — payment allocation

```powershell
fhir-mql convert PaymentReconciliation "status=active&outcome=complete"
fhir-mql convert PaymentReconciliation "requestor=prac-1&request=task-1&payment-issuer=org-1"
fhir-mql convert PaymentReconciliation "allocation-account=acct-1&allocation-encounter=enc-1"
fhir-mql convert PaymentReconciliation "created=ge2024-07-01&disposition:exact=Payment processed"
fhir-mql convert PaymentReconciliation "identifier=PR-001"
```

### PaymentNotice — payment status notices

```powershell
fhir-mql convert PaymentNotice "status=active&payment-status=paid"
fhir-mql convert PaymentNotice "reporter=prac-1&request=claim-1&response=cr-1"
fhir-mql convert PaymentNotice "created=ge2024-07-01&identifier=PN-001"
```

### QuestionnaireResponse — completed questionnaires

```powershell
fhir-mql convert QuestionnaireResponse "patient=pat-1&status=completed"
fhir-mql convert QuestionnaireResponse "questionnaire=Questionnaire/quest-1&authored=ge2024-07-01"
fhir-mql convert QuestionnaireResponse "author=prac-1&source=prac-2&encounter=enc-1"
fhir-mql convert QuestionnaireResponse "based-on=cp-1&part-of=obs-1&item-subject=pat-subj"
fhir-mql convert QuestionnaireResponse "identifier=QR-001&subject=pat-1"
```

### DetectedIssue — clinical issues and alerts

```powershell
fhir-mql convert DetectedIssue "patient=pat-1&status=final"
fhir-mql convert DetectedIssue "author=prac-1&code=DRG&category=drug-drug"
fhir-mql convert DetectedIssue "implicated=mr-1&identified=ge2024-07-01"
fhir-mql convert DetectedIssue "subject=pat-1&identifier=DI-001"
```

### ClinicalImpression — clinical assessments

```powershell
fhir-mql convert ClinicalImpression "patient=pat-1&status=completed"
fhir-mql convert ClinicalImpression "encounter=enc-1&performer=prac-1&date=ge2024-07-01"
fhir-mql convert ClinicalImpression "finding-code=386661006&finding-ref=obs-finding"
fhir-mql convert ClinicalImpression "problem=cond-1&previous=ci-prev&identifier=CI-001"
fhir-mql convert ClinicalImpression "supporting-info=obs-1"
```

### FamilyMemberHistory — family health history

```powershell
fhir-mql convert FamilyMemberHistory "patient=pat-1&status=completed"
fhir-mql convert FamilyMemberHistory "relationship=FTH&sex=male&code=44054006"
fhir-mql convert FamilyMemberHistory "date=ge2024-07-01&identifier=FMH-001"
fhir-mql convert FamilyMemberHistory "instantiates-uri=http://example.org/protocols/fmh"
```

### ImagingStudy — DICOM imaging studies

```powershell
fhir-mql convert ImagingStudy "patient=pat-1&status=available"
fhir-mql convert ImagingStudy "encounter=enc-1&started=ge2024-07-01&modality=CT"
fhir-mql convert ImagingStudy "series=1.2.3.4.5&instance=1.2.3.4.5.6&dicom-class=1.2.840.10008.5.1.4.1.1.2"
fhir-mql convert ImagingStudy "performer=dev-1&referrer=prac-1&based-on=sr-1"
fhir-mql convert ImagingStudy "body-site=body-site-1&body-structure=bs-1&reason=reason-1"
```

### Specimen — laboratory samples

```powershell
fhir-mql convert Specimen "patient=pat-1&status=available"
fhir-mql convert Specimen "collector=prac-1&procedure=proc-1&collected=ge2024-07-01"
fhir-mql convert Specimen "type=119297000&accession=ACC-001&identifier=SP-001"
fhir-mql convert Specimen "bodysite=bs-1&container-device=dev-1&parent=spec-parent"
```

### NutritionOrder — diet, enteral formula, and supplements

```powershell
fhir-mql convert NutritionOrder "patient=pat-1&status=active"
fhir-mql convert NutritionOrder "provider=prac-1&encounter=enc-1"
fhir-mql convert NutritionOrder "datetime=ge2024-07-01&oraldiet=226211001"
fhir-mql convert NutritionOrder "formula=226783000&additive=226789001&supplement=226352002"
fhir-mql convert NutritionOrder "group-identifier=GRP-NO-001&identifier=NO-001"
```

### Contract — legal agreements and policies

```powershell
fhir-mql convert Contract "patient=pat-1&status=executed"
fhir-mql convert Contract "signer=prac-1&authority=org-1&domain=loc-1"
fhir-mql convert Contract "issued=ge2024-07-01&identifier=CTR-001"
fhir-mql convert Contract "url=http://example.org/contracts/ctr-1"
```

### Consent — privacy and data-sharing agreements

```powershell
fhir-mql convert Consent "patient=pat-1&status=active"
fhir-mql convert Consent "grantee=prac-1&purpose=PATRQT&action=access"
fhir-mql convert Consent "period=ge2024-07-01&verified=true"
fhir-mql convert Consent "category=idscl&security-label=R&identifier=CONSENT-001"
```

### AuditEvent — security and audit trail

```powershell
fhir-mql convert AuditEvent "patient=pat-1&action=R"
fhir-mql convert AuditEvent "agent=prac-1&source=dev-1&encounter=enc-1"
fhir-mql convert AuditEvent "code=110100&category=rest&outcome=0"
fhir-mql convert AuditEvent "date=ge2024-07-01&purpose=PATADMIN"
fhir-mql convert AuditEvent "policy=http://example.org/policy/audit"
```

### Flag — clinical alerts and warnings

```powershell
fhir-mql convert Flag "patient=pat-1&status=active"
fhir-mql convert Flag "author=prac-1&encounter=enc-1&category=safety"
fhir-mql convert Flag "date=ge2024-07-01&identifier=FLAG-001"
fhir-mql convert Flag "subject=pat-1"
```

### Communication — messages and notifications

```powershell
fhir-mql convert Communication "patient=pat-1&status=completed"
fhir-mql convert Communication "sender=prac-1&recipient=prac-2&encounter=enc-1"
fhir-mql convert Communication "category=notification&medium=WRITTEN"
fhir-mql convert Communication "sent=ge2024-07-01&received=ge2024-07-01"
fhir-mql convert Communication "topic=371535009&identifier=COMM-001"
```

### Task — workflow tasks

```powershell
fhir-mql convert Task "patient=pat-1&status=in-progress"
fhir-mql convert Task "encounter=enc-1&owner=prac-1&requester=prac-2"
fhir-mql convert Task "code=103693007&intent=order&priority=routine"
fhir-mql convert Task "performer=performer&requestedperformer-reference=prac-3"
fhir-mql convert Task "authored-on=ge2024-07-01&period=ge2024-07-01"
```

### RiskAssessment — clinical risk predictions

```powershell
fhir-mql convert RiskAssessment "patient=pat-1&risk=moderate"
fhir-mql convert RiskAssessment "condition=cond-1&encounter=enc-1"
fhir-mql convert RiskAssessment "probability=gt0.4&date=ge2024-07-01"
fhir-mql convert RiskAssessment "method=clinical&identifier=RA-001"
```

### HealthcareService — service catalog

```powershell
fhir-mql convert HealthcareService "name=cardio&active=true"
fhir-mql convert HealthcareService "organization=org-1&location=loc-1"
fhir-mql convert HealthcareService "service-category=17&service-type=11429006"
fhir-mql convert HealthcareService "specialty=394579002&identifier=HS-001"
```

### EpisodeOfCare — patient care episodes

```powershell
fhir-mql convert EpisodeOfCare "patient=pat-1&status=active"
fhir-mql convert EpisodeOfCare "organization=org-1&care-manager=prac-1"
fhir-mql convert EpisodeOfCare "type=hacc&date=ge2024-01-01"
fhir-mql convert EpisodeOfCare "diagnosis-reference=cond-1&incoming-referral=sr-1"
```

### RelatedPerson — patient contacts & next-of-kin

```powershell
fhir-mql convert RelatedPerson "patient=pat-1&relationship=WIFE"
fhir-mql convert RelatedPerson "name=Smith&active=true"
fhir-mql convert RelatedPerson "identifier=http://hospital.org/rp|RP-001"
fhir-mql convert RelatedPerson "birthdate=ge1980-01-01&phone=555-0199"
```

### Patient — identity & demographics

```powershell
fhir-mql convert Patient "name=Smith&gender=male"
fhir-mql convert Patient "birthdate=ge1980-01-01&birthdate=le1990-12-31"
fhir-mql convert Patient "identifier=http://hospital.org/mrn|MRN-1001"
fhir-mql convert Patient "active=true&address-city=Springfield"
fhir-mql convert Patient "telecom=555-0100"
fhir-mql convert Patient "deceased=false"
fhir-mql convert Patient "language=en-US"
fhir-mql convert Patient "organization=org-1"
```

### Practitioner & PractitionerRole — workforce

```powershell
fhir-mql convert Practitioner "name=Jones&active=true"
fhir-mql convert Practitioner "identifier=http://npi|1234567890"
fhir-mql convert PractitionerRole "practitioner=pr-1&organization=org-1"
fhir-mql convert PractitionerRole "location=loc-er&service=hs-cardiology"
fhir-mql convert PractitionerRole "specialty=394814009"
```

### Organization & Location — facilities

```powershell
fhir-mql convert Organization "name=General Hospital&active=true"
fhir-mql convert Organization "identifier=urn:oid:2.16.840.1.113883.4.6|123"
fhir-mql convert Location "name=ER&status=active"
fhir-mql convert Location "address-city=Boston&organization=org-1"
```

### Observation — vitals, labs, clinical data

```powershell
fhir-mql convert Observation "code=http://loinc.org|8480-6"
fhir-mql convert Observation "patient=p1&status=final"
fhir-mql convert Observation "date=ge2024-06-01&code=8480-6"
fhir-mql convert Observation "value-quantity=120"
fhir-mql convert Observation "category=vital-signs"
fhir-mql convert Observation "encounter=enc-1"
```

### Appointment, Schedule, Slot — scheduling

```powershell
fhir-mql convert Appointment "status=booked&patient=p1"
fhir-mql convert Appointment "date=ge2024-07-01&actor=Practitioner/pr-1"
fhir-mql convert Appointment "reason-code=185345009"
fhir-mql convert Appointment "reason-reference=Condition/cond-1"
fhir-mql convert Schedule "active=true&actor=Practitioner/pr-1"
fhir-mql convert Schedule "service-type=11429006"
fhir-mql convert Schedule "service-type-reference=HealthcareService/hs-1"
fhir-mql convert Schedule "date=ge2024-07-01"
fhir-mql convert Slot "status=free&schedule=sched-1&start=ge2024-07-15"
```

### Encounter — visits & episodes

```powershell
fhir-mql convert Encounter "status=in-progress&patient=p1"
fhir-mql convert Encounter "class=AMB&type=185349003"
fhir-mql convert Encounter "date=ge2024-07-01&practitioner=pr-1"
fhir-mql convert Encounter "date-start=ge2024-07-01&end-date=le2024-07-31"
fhir-mql convert Encounter "location=loc-1&service-provider=org-1"
fhir-mql convert Encounter "diagnosis-code=44054006"
fhir-mql convert Encounter "part-of=enc-parent"
```

### Condition — problem list & diagnoses

```powershell
fhir-mql convert Condition "clinical-status=active&patient=p1"
fhir-mql convert Condition "code=44054006&verification-status=confirmed"
fhir-mql convert Condition "encounter=enc-1&onset-date=ge2020-01-01"
fhir-mql convert Condition "category=problem-list-item"
```

### ServiceRequest — orders & referrals

```powershell
fhir-mql convert ServiceRequest "status=active&patient=p1"
fhir-mql convert ServiceRequest "code-concept=103693007&intent=order"
fhir-mql convert ServiceRequest "requester=pr-1&performer=pr-2"
fhir-mql convert ServiceRequest "occurrence=ge2024-07-15&encounter=enc-1"
```

### Procedure — performed actions

```powershell
fhir-mql convert Procedure "status=completed&patient=p1"
fhir-mql convert Procedure "code=80146002&category=103693007"
fhir-mql convert Procedure "date=ge2024-07-01&encounter=enc-1"
fhir-mql convert Procedure "performer=pr-1&location=loc-1"
fhir-mql convert Procedure "reason-code=109006&reason-reference=Condition/cond-1"
fhir-mql convert Procedure "based-on=ServiceRequest/sr-1&part-of=Observation/obs-1"
```

### Medication — medication definitions

```powershell
fhir-mql convert Medication "status=active&code=319785009"
fhir-mql convert Medication "form=385055001&identifier=MED-001"
fhir-mql convert Medication "ingredient-code=387517004&ingredient=Substance/sub-1"
fhir-mql convert Medication "lot-number=LOT-42&expiration-date=ge2026-01-01"
fhir-mql convert Medication "marketingauthorizationholder=org-1"
```

### Substance — materials / ingredients

```powershell
fhir-mql convert Substance "status=active&code=387517004"
fhir-mql convert Substance "category=chemical&identifier=SUB-001"
fhir-mql convert Substance "code-reference=sd-aspirin&substance-reference=sub-salicylic"
fhir-mql convert Substance "expiry=le2025-12-31&quantity=500|http://unitsofmeasure.org|mg"
```

### MedicationRequest — prescriptions & orders

```powershell
fhir-mql convert MedicationRequest "status=active&patient=p1"
fhir-mql convert MedicationRequest "code=319785009&medication=Medication/med-1"
fhir-mql convert MedicationRequest "intent=order&priority=stat"
fhir-mql convert MedicationRequest "requester=pr-1&intended-performer=pr-2"
fhir-mql convert MedicationRequest "authoredon=ge2024-07-01&encounter=enc-1"
fhir-mql convert MedicationRequest "intended-dispenser=org-pharm&group-identifier=GRP-99"
```

### MedicationAdministration — administrations given

```powershell
fhir-mql convert MedicationAdministration "status=completed&patient=p1"
fhir-mql convert MedicationAdministration "code=319785009&medication=Medication/med-1"
fhir-mql convert MedicationAdministration "date=ge2024-07-15&encounter=enc-1"
fhir-mql convert MedicationAdministration "performer=pr-1&device=pump-1"
fhir-mql convert MedicationAdministration "request=MedicationRequest/mr-1"
fhir-mql convert MedicationAdministration "reason-given-code=386661006&reason-not-given=182849000"
```

### Claim — billing / financial claims

```powershell
fhir-mql convert Claim "patient=p1&status=active&use=claim"
fhir-mql convert Claim "provider=pr-1&enterer=pr-2&insurer=org-ins"
fhir-mql convert Claim "created=ge2024-07-15&priority=normal"
fhir-mql convert Claim "encounter=enc-1&care-team=pr-ct&payee=pr-payee"
fhir-mql convert Claim "item-udi=dev-1&detail-udi=dev-2&procedure-udi=dev-3"
fhir-mql convert Claim "identifier=CLM-001&facility=loc-1"
```

### DocumentReference — documents / attachments

```powershell
fhir-mql convert DocumentReference "patient=p1&status=current&type=34117-2"
fhir-mql convert DocumentReference "author=pr-1&attester=pr-attest&custodian=org-cust"
fhir-mql convert DocumentReference "context=enc-1&category=clinical-note&doc-status=final"
fhir-mql convert DocumentReference "contenttype=application/pdf&location=example.org/docs"
fhir-mql convert DocumentReference "date=ge2024-07-15&creation=ge2024-07-14&period=ge2024-07-01"
fhir-mql convert DocumentReference "relatesto=doc-old&relation=replaces&identifier=DOC-001"
```

### ClaimResponse — adjudication / payer response

```powershell
fhir-mql convert ClaimResponse "patient=p1&status=active&outcome=complete"
fhir-mql convert ClaimResponse "request=claim-1&requestor=pr-1&insurer=org-ins"
fhir-mql convert ClaimResponse "created=ge2024-07-15&payment-date=ge2024-08-01"
fhir-mql convert ClaimResponse "identifier=CR-001&use=claim&disposition=processed"
```

### Coverage — insurance / benefits

```powershell
fhir-mql convert Coverage "patient=p1&status=active"
fhir-mql convert Coverage "beneficiary=pat-1&insurer=org-ins"
fhir-mql convert Coverage "policy-holder=p-holder&subscriber=p-sub"
fhir-mql convert Coverage "paymentby-party=pat-1&type=EHCPOL"
fhir-mql convert Coverage "identifier=COV-001&subscriberid=SUB-001"
fhir-mql convert Coverage "class-type=group&class-value=GRP-100&dependent=01"
```

### Immunization — vaccination records

```powershell
fhir-mql convert Immunization "patient=p1&status=completed"
fhir-mql convert Immunization "vaccine-code=140&date=ge2024-07-15"
fhir-mql convert Immunization "performer=pr-1&location=loc-1&manufacturer=org-mfr"
fhir-mql convert Immunization "lot-number=LOT-2024&series=Standard"
fhir-mql convert Immunization "target-disease=6142004&reason-code=429060002"
fhir-mql convert Immunization "reaction=obs-1&reaction-date=ge2024-07-16"
```

### CarePlan — care plans

```powershell
fhir-mql convert CarePlan "patient=p1&status=active&intent=plan"
fhir-mql convert CarePlan "category=assess-plan&condition=cond-1"
fhir-mql convert CarePlan "date=ge2024-07-01&encounter=enc-1"
fhir-mql convert CarePlan "care-team=ct-1&goal=goal-1&activity-reference=sr-1"
fhir-mql convert CarePlan "custodian=pr-1&based-on=sr-1"
```

### Goal — care goals

```powershell
fhir-mql convert Goal "patient=p1&lifecycle-status=active"
fhir-mql convert Goal "description=406156006&achievement-status=in-progress"
fhir-mql convert Goal "category=dietary&target-measure=29463-7"
fhir-mql convert Goal "start-date=ge2024-07-01&target-date=le2024-12-31"
fhir-mql convert Goal "addresses=cond-1&identifier=GOAL-001"
```

### CareTeam — care coordination teams

```powershell
fhir-mql convert CareTeam "patient=p1&status=active"
fhir-mql convert CareTeam "name=Crisis&category=LA27976-2"
fhir-mql convert CareTeam "participant=pr-1&subject=Patient/p1"
fhir-mql convert CareTeam "date=ge2024-07-01&identifier=CT-001"
```

### DiagnosticReport — lab & imaging reports

```powershell
fhir-mql convert DiagnosticReport "patient=p1&status=final"
fhir-mql convert DiagnosticReport "code=11502-2&category=LAB&conclusion=10828004"
fhir-mql convert DiagnosticReport "date=ge2024-07-10&issued=ge2024-07-10"
fhir-mql convert DiagnosticReport "encounter=enc-1&performer=pr-1&result=obs-1"
fhir-mql convert DiagnosticReport "specimen=spec-1&study=img-1&media=doc-1"
fhir-mql convert DiagnosticReport "based-on=sr-1&results-interpreter=pr-2"
```

### AllergyIntolerance — allergies & intolerances

```powershell
fhir-mql convert AllergyIntolerance "patient=p1&clinical-status=active"
fhir-mql convert AllergyIntolerance "code=91935009&verification-status=confirmed"
fhir-mql convert AllergyIntolerance "category=food&criticality=high&type=allergy"
fhir-mql convert AllergyIntolerance "date=ge2024-07-01&last-date=ge2024-06-01"
fhir-mql convert AllergyIntolerance "severity=severe&route=26643006"
fhir-mql convert AllergyIntolerance "manifestation-code=39579001&participant=pr-1"
```

### MedicationDispense — dispensing events

```powershell
fhir-mql convert MedicationDispense "status=completed&patient=p1"
fhir-mql convert MedicationDispense "code=319785009&medication=Medication/med-1"
fhir-mql convert MedicationDispense "whenprepared=ge2024-07-14&whenhandedover=ge2024-07-15"
fhir-mql convert MedicationDispense "prescription=MedicationRequest/mr-1&encounter=enc-1"
fhir-mql convert MedicationDispense "performer=pr-1&location=loc-1&destination=loc-dest"
fhir-mql convert MedicationDispense "responsibleparty=pr-2&type=FF"
```

### MedicationStatement — home / reconciled medication list

```powershell
fhir-mql convert MedicationStatement "patient=p1&status=recorded&code=313782"
fhir-mql convert MedicationStatement "medication=Medication/med-1&encounter=enc-1"
fhir-mql convert MedicationStatement "source=prac-1&adherence=taking&category=inpatient"
fhir-mql convert MedicationStatement "effective=ge2024-06-01&identifier=MS-001"
fhir-mql convert MedicationStatement "subject=pat-1"
```

### DeviceRequest — equipment & implant orders

```powershell
fhir-mql convert DeviceRequest "patient=p1&status=active&intent=order"
fhir-mql convert DeviceRequest "code=706172005&device=Device/dev-1&requester=pr-1"
fhir-mql convert DeviceRequest "encounter=enc-1&authored-on=ge2024-07-01&event-date=ge2024-07-15"
fhir-mql convert DeviceRequest "performer=pr-2&performer-code=performer&priority=stat"
fhir-mql convert DeviceRequest "based-on=sr-1&prior-request=dr-prev&group-identifier=GRP-DR"
```

### AdverseEvent — safety & pharmacovigilance

```powershell
fhir-mql convert AdverseEvent "patient=p1&status=completed&actuality=actual"
fhir-mql convert AdverseEvent "code=404684003&category=product-use&seriousness=serious"
fhir-mql convert AdverseEvent "encounter=enc-1&recorder=pr-1&date=ge2024-07-01"
fhir-mql convert AdverseEvent "study=rs-1&substance=sub-1&resultingeffect=cond-1"
fhir-mql convert AdverseEvent "identifier=AE-001&location=loc-1"
```

### ImmunizationRecommendation — vaccine forecast

```powershell
fhir-mql convert ImmunizationRecommendation "patient=p1&date=ge2024-09-01"
fhir-mql convert ImmunizationRecommendation "identifier=IR-001&status=due"
fhir-mql convert ImmunizationRecommendation "target-disease=6142004&vaccine-type=208"
fhir-mql convert ImmunizationRecommendation "information=obs-1&support=imm-1"
```

### Person — person master (linked to Patient)

```powershell
fhir-mql convert Person "name=Smith&gender=male"
fhir-mql convert Person "birthdate=ge1980-01-01&phone=555-0100&email=user@example.org"
fhir-mql convert Person "address-city=Boston&identifier=PERSON-001"
fhir-mql convert Person "link=Patient/p1&organization=org-1"
```

### BodyStructure — anatomical location / marking

```powershell
fhir-mql convert BodyStructure "patient=p1"
fhir-mql convert BodyStructure "morphology=368208006&identifier=BS-001"
fhir-mql convert BodyStructure "included_structure=368208006&excluded_structure=113257007"
```

### DeviceUsage — device utilization on patient

```powershell
fhir-mql convert DeviceUsage "patient=p1&status=active"
fhir-mql convert DeviceUsage "device=Device/dev-1&identifier=DU-001"
```

### DeviceDispense — device fulfillment

```powershell
fhir-mql convert DeviceDispense "patient=p1&status=completed"
fhir-mql convert DeviceDispense "code=706172005&subject=pat-1&identifier=DD-001"
```

### SupplyRequest — supply requisitions

```powershell
fhir-mql convert SupplyRequest "patient=p1&status=active&category=central"
fhir-mql convert SupplyRequest "requester=pr-1&supplier=org-supply&date=ge2024-07-01"
fhir-mql convert SupplyRequest "subject=pat-1&identifier=SR-001"
```

### SupplyDelivery — supply shipments

```powershell
fhir-mql convert SupplyDelivery "patient=p1&status=completed"
fhir-mql convert SupplyDelivery "supplier=org-supply&receiver=loc-1&identifier=SD-001"
```

### BiologicallyDerivedProduct — blood, tissue, cellular products

```powershell
fhir-mql convert BiologicallyDerivedProduct "code=119297000&product-category=organ"
fhir-mql convert BiologicallyDerivedProduct "product-status=available&serial-number=SN-42"
fhir-mql convert BiologicallyDerivedProduct "collector=pr-1&request=sr-1&biological-source-event=evt-1"
fhir-mql convert BiologicallyDerivedProduct "identifier=BDP-001"
```

### Device & Group — assets & cohorts

```powershell
fhir-mql convert Device "status=active&organization=org-1"
fhir-mql convert Device "type=182722004&manufacturer=Acme"
fhir-mql convert Device "expiration-date=le2025-12-31"
fhir-mql convert Group "name=Cohort-A&type=person"
fhir-mql convert Group "member=Patient/p1&membership=enumerated"
fhir-mql convert Group "characteristic=73211009"
```

### Combined / AND queries

```powershell
fhir-mql convert Patient "name=Smith&gender=male&birthdate=ge1980-01-01"
fhir-mql convert Observation "patient=p1&code=8480-6&date=ge2024-01-01&status=final"
fhir-mql convert Appointment "status=booked&patient=p1&date=ge2024-07-01"
```

---

## Search (convert + execute)

Requires MongoDB. Documents should be denormalized first (`fhir-mql denormalize`).

```powershell
fhir-mql search Patient "name=Smith&gender=male" --limit 10
fhir-mql search Patient "name=Smith" --uri $URI --db $DB --format json
fhir-mql search Observation "code=http://loinc.org|8480-6&status=final" --limit 50
fhir-mql search Slot "status=free&start=ge2024-07-01" --limit 20

# Explain MQL without running find
fhir-mql search Patient "name=Smith" --explain
```

---

## Bulk operations

For `denormalize`, `indexes`, `reset`, and `stats`, the CLI expands each
requested resource type to its **transitive dependencies** using the same
`CORE_DEPENDENCIES` graph as
`fhir-data-generation/fhir_gen/resolvers/dependency.py` (see
`fhir_search_to_mql/resolvers/dependency.py`). Dependencies are processed
**before** dependents (e.g. `MeasureReport` also runs `Measure`, `Patient`,
`Practitioner`, `Organization`). Only types with a shipped YAML config are
included. Use `--no-with-deps` to limit work to the types you name.

```powershell
# MeasureReport only on CLI → also denormalizes anchors (stderr lists added types)
fhir-mql denormalize MeasureReport --uri $URI --db $DB

# Strict: no dependency expansion
fhir-mql denormalize MeasureReport --no-with-deps --uri $URI --db $DB
```

### Indexes (from YAML)

```powershell
fhir-mql indexes Patient
fhir-mql indexes Patient Observation Encounter --uri $URI --db $DB
fhir-mql indexes --all
fhir-mql indexes Slot Schedule --dry-run
```

### Denormalize (`_search` + `_compartments`)

```powershell
fhir-mql denormalize Patient
fhir-mql denormalize Patient Observation Appointment --batch-size 1000
fhir-mql denormalize --all --uri $URI --db $DB
fhir-mql denormalize --all --limit 100          # smoke / staging
fhir-mql denormalize --all --dry-run
```

### Reset (clear denorm fields only)

```powershell
fhir-mql reset Patient
fhir-mql reset Patient Observation --uri $URI --db $DB
fhir-mql reset --all
```

### Stats (coverage)

```powershell
fhir-mql stats Patient
fhir-mql stats --all --format json
fhir-mql stats Patient Observation Encounter Appointment
```

### Full reindex pipeline (typical deploy)

```powershell
fhir-mql indexes --all --uri $URI --db $DB
fhir-mql denormalize --all --uri $URI --db $DB --batch-size 500
fhir-mql stats --all --uri $URI --db $DB
```

---

## Compartment-scoped queries

Precomputed fast-path: Patient, Practitioner, Device (and Encounter self on
Encounter). RelatedPerson / some Encounter links use dynamic resolution.

```powershell
# Patient compartment → Observations for one patient
fhir-mql convert Observation "code=8480-6" `
  --compartment-type Patient --compartment-id p1

fhir-mql search Observation "status=final" `
  --compartment-type Patient --compartment-id p1 --limit 25

# Practitioner compartment → Schedules
fhir-mql convert Schedule "" `
  --compartment-type Practitioner --compartment-id pr-1

fhir-mql search Schedule "active=true" `
  --compartment-type Practitioner --compartment-id pr-1

# Device compartment
fhir-mql convert Observation "code=8480-6" `
  --compartment-type Device --compartment-id dev-1

# Encounter compartment (precomputed on Encounter resource)
fhir-mql convert Encounter "status=in-progress" `
  --compartment-type Encounter --compartment-id enc-1

# Dynamic: RelatedPerson → Schedule (no _compartments.RelatedPerson)
fhir-mql convert Schedule "" `
  --compartment-type RelatedPerson --compartment-id rp-1
```

**REST equivalent mental model:**

| HTTP pattern | CLI |
|--------------|-----|
| `GET /Patient/{id}/Observation?code=…` | `search Observation … --compartment-type Patient --compartment-id {id}` |
| `GET /Practitioner/{id}/Schedule` | `search Schedule … --compartment-type Practitioner --compartment-id {id}` |
| `GET /Encounter/{id}/Condition?…` | `convert Condition … --compartment-type Encounter --compartment-id {id}` *(dynamic)* |

---

## Healthcare workflow scenarios

End-to-end patterns using **all shipped resource types**. Run `fhir-mql indexes --all` and
`fhir-mql denormalize --all` once per database before `search`.

### 1. Patient registration, MPI & person index

```powershell
fhir-mql search Patient "identifier=http://hospital.org/mrn|MRN-1001" --limit 5
fhir-mql search Patient "name=Smith&birthdate=1980-05-15&active=true" --limit 10
fhir-mql search Person "name=Smith&patient=p1" --limit 5
fhir-mql search RelatedPerson "patient=p1&relationship=WIFE" --limit 10
```

### 2. Provider directory, roles & network

```powershell
fhir-mql search Practitioner "name=Jones&active=true" --limit 20
fhir-mql search PractitionerRole "organization=org-1&specialty=394814009" --limit 50
fhir-mql search OrganizationAffiliation "primary-organization=org-1&active=true" --limit 25
fhir-mql search HealthcareService "organization=org-1&service-type=11429006" --limit 20
fhir-mql search Endpoint "organization=org-1&status=active&connection-type=hl7-fhir-rest" --limit 10
```

### 3. Facility, location & care episodes

```powershell
fhir-mql search Organization "name=Hospital&active=true" --limit 10
fhir-mql search Location "name=ER&status=active&organization=org-1" --limit 25
fhir-mql search EpisodeOfCare "patient=p1&status=active&type=hacc" --limit 10
fhir-mql search Account "patient=p1&status=active&type=PBILLACCT" --limit 5
```

### 4. Scheduling & access management

```powershell
fhir-mql search Schedule "active=true&actor=Practitioner/pr-1" --limit 10
fhir-mql search Slot "status=free&schedule=sched-1&start=ge2024-07-15&start=le2024-07-31" --limit 100
fhir-mql search Appointment "status=booked&patient=p1&date=ge2024-07-01" --limit 20
fhir-mql search Appointment "reason-code=185345009&actor=Practitioner/pr-1" --limit 30
```

### 5. Encounters, ward board & visit documents

```powershell
fhir-mql search Encounter "status=in-progress&location=loc-er" --limit 50
fhir-mql search Encounter "patient=p1&class=AMB&date=ge2024-07-01" --limit 30
fhir-mql search Composition "patient=p1&status=final&type=18842-5" --limit 20
fhir-mql search DocumentReference "patient=p1&status=current&type=34117-2" --limit 20
```

### 6. Problem list, allergies & clinical impressions

```powershell
fhir-mql search Condition "patient=p1&clinical-status=active" --limit 50
fhir-mql search AllergyIntolerance "patient=p1&clinical-status=active&criticality=high" --limit 20
fhir-mql search ClinicalImpression "patient=p1&status=completed&encounter=enc-1" --limit 10
fhir-mql search FamilyMemberHistory "patient=p1&relationship=FTH&code=44054006" --limit 10
fhir-mql search BodyStructure "patient=p1&morphology=368208006" --limit 5
```

### 7. Vitals, labs, imaging & specimens

```powershell
fhir-mql search Observation "patient=p1&category=vital-signs&date=ge2024-06-01" --limit 100
fhir-mql search DiagnosticReport "patient=p1&status=final&category=LAB" --limit 30
fhir-mql search ImagingStudy "patient=p1&modality=CT&status=available" --limit 15
fhir-mql search Specimen "patient=p1&status=available&type=119297000" --limit 20
```

### 8. Medication management (inpatient & outpatient)

```powershell
fhir-mql search MedicationRequest "patient=p1&status=active&intent=order" --limit 50
fhir-mql search MedicationAdministration "patient=p1&status=completed&date=ge2024-07-15" --limit 50
fhir-mql search MedicationDispense "patient=p1&status=completed&prescription=mr-1" --limit 20
fhir-mql search MedicationStatement "patient=p1&status=recorded&effective=ge2024-01-01" --limit 50
fhir-mql search DetectedIssue "patient=p1&category=drug-drug&status=final" --limit 10
```

### 9. Orders, procedures & care coordination

```powershell
fhir-mql search ServiceRequest "patient=p1&status=active&intent=order" --limit 40
fhir-mql search Procedure "patient=p1&status=completed&date=ge2024-07-01" --limit 30
fhir-mql search DeviceRequest "patient=p1&status=active&priority=stat" --limit 20
fhir-mql search RequestOrchestration "patient=p1&status=active&intent=order" --limit 10
fhir-mql search CarePlan "patient=p1&status=active" --limit 10
fhir-mql search CareTeam "patient=p1&status=active" --limit 10
fhir-mql search Goal "patient=p1&lifecycle-status=active" --limit 20
fhir-mql search Task "patient=p1&status=in-progress" --limit 30
```

### 10. Devices, supplies & implants

```powershell
fhir-mql search Device "status=active&expiration-date=le2025-12-31" --limit 50
fhir-mql search DeviceUsage "patient=p1&status=active" --limit 20
fhir-mql search DeviceDispense "patient=p1&status=completed" --limit 20
fhir-mql search SupplyRequest "patient=p1&status=active" --limit 15
fhir-mql search SupplyDelivery "patient=p1&status=completed" --limit 15
fhir-mql search BiologicallyDerivedProduct "product-status=available" --limit 10
```

### 11. Immunizations & public health

```powershell
fhir-mql search Immunization "patient=p1&status=completed&vaccine-code=140" --limit 20
fhir-mql search ImmunizationRecommendation "patient=p1&date=ge2024-09-01" --limit 10
```

### 12. Nutrition, vision & specialty orders

```powershell
fhir-mql search NutritionOrder "patient=p1&status=active" --limit 10
fhir-mql search NutritionIntake "patient=p1&status=completed&date=ge2024-07-01" --limit 20
fhir-mql search VisionPrescription "patient=p1&status=active&prescriber=pr-1" --limit 10
```

### 13. Eligibility, claims & revenue cycle

```powershell
fhir-mql search Coverage "patient=p1&status=active" --limit 10
fhir-mql search CoverageEligibilityRequest "patient=p1&status=active" --limit 10
fhir-mql search CoverageEligibilityResponse "patient=p1&outcome=complete" --limit 10
fhir-mql search Claim "patient=p1&status=active&use=claim" --limit 20
fhir-mql search ClaimResponse "patient=p1&outcome=complete&request=clm-1" --limit 20
fhir-mql search ExplanationOfBenefit "patient=p1&status=active" --limit 20
fhir-mql search ChargeItem "patient=p1&status=billable&encounter=enc-1" --limit 50
fhir-mql search ChargeItemDefinition "status=active&context=ambulatory" --limit 10
fhir-mql search Invoice "patient=p1&status=issued" --limit 10
fhir-mql search PaymentNotice "status=active&payment-status=paid" --limit 10
fhir-mql search PaymentReconciliation "status=active&outcome=complete" --limit 10
```

### 14. Payer enrollment & plan catalog

```powershell
fhir-mql search InsurancePlan "status=active&owned-by=org-1" --limit 20
fhir-mql search EnrollmentRequest "patient=p1&status=active" --limit 10
fhir-mql search EnrollmentResponse "request=enr-1&status=active" --limit 10
```

### 15. Quality reporting (eCQM / HEDIS-style)

```powershell
fhir-mql search Measure "status=active&title=Diabetes" --limit 10
fhir-mql search MeasureReport "patient=p1&status=complete&measure=diabetes" --limit 20
fhir-mql search MeasureReport "period=ge2024-01-01&reporter=pr-1" --limit 50
```

### 16. Research, genomics & safety

```powershell
fhir-mql search ResearchStudy "status=active&phase=phase-2" --limit 10
fhir-mql search ResearchSubject "patient=p1&study=rs-1&status=active" --limit 20
fhir-mql search GenomicStudy "patient=p1&status=registered" --limit 10
fhir-mql search AdverseEvent "patient=p1&actuality=actual&seriousness=serious" --limit 20
fhir-mql search Group "name=Diabetes-Cohort&characteristic=73211009" --limit 10
```

### 17. Forms, PROs & patient-reported data

```powershell
fhir-mql search Questionnaire "status=active&url=http://example.org/Questionnaire/phq9" --limit 5
fhir-mql search QuestionnaireResponse "patient=p1&status=completed&questionnaire=quest-1" --limit 20
```

### 18. Privacy, consent, contracts & communications

```powershell
fhir-mql search Consent "patient=p1&status=active&category=idscl" --limit 10
fhir-mql search Contract "patient=p1&status=executed" --limit 5
fhir-mql search Communication "patient=p1&status=completed&category=notification" --limit 20
fhir-mql search Flag "patient=p1&status=active&category=safety" --limit 10
fhir-mql search RiskAssessment "patient=p1&risk=moderate" --limit 10
```

### 19. Security audit & provenance

```powershell
fhir-mql search AuditEvent "patient=p1&action=R&date=ge2024-07-01" --limit 50
fhir-mql search Provenance "patient=p1&activity=CREATE&recorded=ge2024-07-01" --limit 30
fhir-mql search Basic "patient=p1&code=referral" --limit 10
```

### 20. Compartment REST patterns (patient chart)

```powershell
fhir-mql search Observation "status=final&code=8480-6" `
  --compartment-type Patient --compartment-id p1 --limit 25
fhir-mql search MedicationRequest "status=active" `
  --compartment-type Patient --compartment-id p1 --limit 50
fhir-mql search Encounter "status=finished" `
  --compartment-type Patient --compartment-id p1 --limit 20
```

### 21. Go-live audit & disaster recovery

```powershell
fhir-mql stats --all --uri $URI --db $DB --format json
fhir-mql denormalize --all --dry-run --uri $URI --db $DB
fhir-mql reset --all --uri $URI --db $DB
fhir-mql indexes --all --uri $URI --db $DB
fhir-mql denormalize --all --uri $URI --db $DB --batch-size 500
```

---

## Industrial & enterprise scenarios

Cross-industry patterns on the same 84-resource stack (hospitals, payers, pharma, device
manufacturers, public health, clinical research).

### A. Hospital operations & capacity

```powershell
fhir-mql search Encounter "status=in-progress&service-provider=org-1" --limit 100
fhir-mql search Location "operational-status=O&name:contains=OR" --limit 20
fhir-mql search Appointment "status=booked&date=ge2024-07-01&service-type=11429006" --limit 200
fhir-mql search Task "code=103693007&status=in-progress&encounter=enc-1" --limit 50
```

### B. Revenue cycle & patient accounting

```powershell
fhir-mql search ChargeItem "status=billable&encounter=enc-1&performer-actor=pr-1" --limit 100
fhir-mql search Invoice "issuer=org-1&status=issued&date=ge2024-07-01" --limit 50
fhir-mql search Account "owner=org-1&status=active" --limit 30
fhir-mql search Claim "insurer=org-ins&created=ge2024-07-01&status=active" --limit 100
```

### C. Payer: eligibility → claim → EOB

```powershell
fhir-mql search CoverageEligibilityRequest "insurer=org-ins&created=ge2024-07-01" --limit 50
fhir-mql search CoverageEligibilityResponse "insurer=org-ins&outcome=complete" --limit 50
fhir-mql search ClaimResponse "insurer=org-ins&outcome=complete&payment-date=ge2024-08-01" --limit 50
fhir-mql search ExplanationOfBenefit "insurer=org-ins&patient=pat-1" --limit 20
```

### D. Pharmacy & medication safety

```powershell
fhir-mql search Medication "code=319785009&status=active" --limit 10
fhir-mql search MedicationRequest "intended-dispenser=org-pharm&status=active" --limit 50
fhir-mql search MedicationDispense "whenhandedover=ge2024-07-15&status=completed" --limit 50
fhir-mql search DetectedIssue "code=DRG&implicated=mr-1" --limit 20
fhir-mql search AdverseEvent "category=product-use&actuality=actual" --limit 20
```

### E. Medical device lifecycle (manufacturer / HTM)

```powershell
fhir-mql search Device "manufacturer=Acme&status=active" --limit 100
fhir-mql search DeviceRequest "requester=pr-1&status=active" --limit 30
fhir-mql search DeviceUsage "device=Device/dev-1" --limit 20
fhir-mql search DeviceDispense "code=706172005&status=completed" --limit 20
fhir-mql search Provenance "target=Device/dev-1&activity=UPDATE" --limit 10
```

### F. Supply chain & blood bank

```powershell
fhir-mql search SupplyRequest "supplier=org-supply&status=active" --limit 30
fhir-mql search SupplyDelivery "supplier=org-supply&status=completed" --limit 30
fhir-mql search BiologicallyDerivedProduct "product-category=organ&product-status=available" --limit 20
fhir-mql search Specimen "accession=ACC-001" --limit 5
```

### G. Clinical trials & RWE

```powershell
fhir-mql search ResearchStudy "status=recruiting&condition=44054006" --limit 10
fhir-mql search ResearchSubject "study=rs-1&subject_state=on-study" --limit 100
fhir-mql search AdverseEvent "study=rs-1&seriousness=serious" --limit 20
fhir-mql search GenomicStudy "patient=p1&focus=cond-1" --limit 10
```

### H. Quality & regulatory reporting

```powershell
fhir-mql search Measure "status=active&context=ambulatory" --limit 20
fhir-mql search MeasureReport "measure=meas-1&status=complete&period=ge2024-01-01" --limit 100
fhir-mql search AuditEvent "outcome=8&category=rest" --limit 50
```

### I. Interoperability hub (organizations & endpoints)

```powershell
fhir-mql search OrganizationAffiliation "participating-organization=org-2&role=provider" --limit 20
fhir-mql search Endpoint "organization=org-1&status=active" --limit 10
fhir-mql search Provenance "agent=pr-1&entity=Endpoint/ep-1" --limit 10
```

### J. Population health & cohort analytics

```powershell
fhir-mql search Group "type=person&characteristic=73211009" --limit 10
fhir-mql search Observation "code=4548-4&value-quantity=gt7" --limit 500
fhir-mql search Condition "clinical-status=active&code=44054006" --limit 500
fhir-mql search RiskAssessment "probability=gt0.4&method=clinical" --limit 100
```

### K. Denormalize all 84 resource types (production load)

```powershell
fhir-mql indexes --all --uri $URI --db $DB
fhir-mql denormalize --all --uri $URI --db $DB --batch-size 500
fhir-mql stats --all --uri $URI --db $DB --format json
```

Or target a clinical subset:

```powershell
$clinical = @(
  "Patient","Practitioner","Organization","Location","Encounter",
  "Condition","Observation","AllergyIntolerance","MedicationRequest",
  "MedicationAdministration","MedicationStatement","DiagnosticReport",
  "Procedure","ServiceRequest","Immunization","DocumentReference","Composition"
)
fhir-mql denormalize --uri $URI --db $DB @clinical
```

---

## Multi-database / project presets

Commands copied from real hybrid / synthetic data loads:

### Schedule-appointment hybrid database

```powershell
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_schedule_appointment_hybrid"

fhir-mql denormalize --uri $URI --db $DB `
  Appointment Device Group Location Organization Patient Practitioner PractitionerRole Schedule Slot

fhir-mql reset --uri $URI --db $DB `
  Appointment Device Group Location Organization Patient Practitioner PractitionerRole Schedule Slot

fhir-mql search Patient "name=Taylor" --limit 10 --uri $URI --db $DB
```

### Synthetic demographics database

```powershell
$URI = "mongodb://localhost:27017/"
$DB  = "fhir_synthetic"

fhir-mql denormalize --uri $URI --db $DB Location Organization Patient Practitioner
fhir-mql reset --uri $URI --db $DB --all
```

### Full 84-resource denormalize (all shipped configs)

```powershell
fhir-mql indexes --all --uri $URI --db $DB
fhir-mql denormalize --all --uri $URI --db $DB --batch-size 500
fhir-mql stats --all --uri $URI --db $DB
```

### Legacy 13-resource scheduling subset

```powershell
fhir-mql denormalize --uri $URI --db $DB `
  Appointment Condition Device Encounter Group Location Observation Organization `
  Patient Practitioner PractitionerRole Schedule Slot
```

---

## Python API one-liners

```powershell
# Convert
python -c "from fhir_search_to_mql import FHIRSearchConverter as C; print(C().convert('Patient','name=Smith'))"

# Denormalize
python -c "from fhir_search_to_mql import ResourceDenormalizer as D; r={'resourceType':'Patient','id':'x','name':[{'family':'Smith'}]}; print(D().denormalize(r).get('_search',{}))"

# Compartment query
python -c "from fhir_search_to_mql import FHIRSearchConverter as C; print(C().convert_with_compartment('Patient','p1','Observation','code=8480-6'))"

# Denormalize from file / folder
python -c "
from fhir_search_to_mql import ResourceDenormalizer
d = ResourceDenormalizer()
# d.denormalize_from_file('patient.json')
# d.denormalize_from_folder('fhir_data/', resource_type='Patient', recursive=True)
"

# Bulk MongoDB update
python -c "
from pymongo import MongoClient
from fhir_search_to_mql import ResourceDenormalizer, MongoDBHandler
db = MongoClient('mongodb://localhost:27017/')['fhir_synthetic']
print(MongoDBHandler.update_search_fields(db.Patient, ResourceDenormalizer().denormalize, batch_size=500))
"
```

---

## FHIR schema tooling

Local spec indexes under `schema/` (not installed with the wheel). See
[schema/README.md](schema/README.md).

```powershell
# Regenerate resources.r5.json, search-parameters.r5.json, shipped index
python -m fhir_search_to_mql.schema.build_indexes

# Per-resource spec summary (search params, elements, compartments)
python -m fhir_search_to_mql.schema.resource_spec Encounter
python -m fhir_search_to_mql.schema.resource_spec Condition
python -m fhir_search_to_mql.schema.resource_spec Patient

# Optional: point at a relocated schema tree
$env:FHIR_SCHEMA_ROOT = "D:\path\to\schema"
python -m fhir_search_to_mql.schema.build_indexes
```

---

## Testing commands

```powershell
# Full suite (~5 min; skips @mongodb if DB down)
python -m pytest tests/ -q --no-cov --ignore=tests/integration/test_performance.py

# Unit only (no MongoDB)
python -m pytest tests/unit/ -q

# Skip CLI live-DB integration
python -m pytest --ignore=tests/integration/test_cli_integration.py -q

# Cross-config audit harness (all 84 shipped resources)
python -m pytest tests/integration/test_config_audit_regressions.py -v

# Gap-batch / single resource
python -m pytest tests/integration/test_medication_statement_comprehensive.py -v
python -m pytest tests/integration -k "Composition or DeviceRequest or Measure" -q --no-cov

# Per-resource comprehensive suites
python -m pytest tests/integration/test_encounter_comprehensive.py -v
python -m pytest tests/integration/test_condition_comprehensive.py -v
python -m pytest tests/integration/test_patient_comprehensive.py -v

# MongoDB-tagged E2E only
python -m pytest -m mongodb -v

# Coverage
python -m pytest tests/ --cov=fhir_search_to_mql --cov-report=term-missing
```

### CLI_COMMANDS E2E (convert + generate→search pipeline)

See **[E2E_COMMANDS.md](E2E_COMMANDS.md)** (pytest, this repo) and **[E2E_COMBINED.md](E2E_COMBINED.md)** (`fhir-data-generation/scripts/run_cli_e2e.py`).

Pipeline tests load data with **fhir-gen** (install sibling repo), then run
`indexes` / `denormalize` / `search` per healthcare & industrial scenarios.
Each scenario uses database `fhir_e2e_gen_<id>` on `localhost:27017` (same DB as fhir-gen; see [E2E_COMBINED.md](E2E_COMBINED.md)).

```powershell
pip install -e ".[dev]"
pip install -e "..\fhir-data-generation"

# Convert smoke (no MongoDB)
pytest tests/e2e/test_convert_e2e.py -m e2e --no-cov -q

# Full pipeline (requires MongoDB)
pytest tests/e2e/test_cli_commands_e2e.py -m "e2e and mongodb" --no-cov -q

# Or from fhir-data-generation (both repos):
cd ..\fhir-data-generation
python scripts/run_cli_e2e.py
```

---

## Troubleshooting commands

```powershell
# CLI on PATH?
where.exe fhir-mql
pip show fhir-search-to-mql

# MongoDB reachable?
python -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000).server_info())"

# Unknown resource type
fhir-mql denormalize Bogus
# → lists configured resources in error message

# Re-pick bundled YAML after local edits (editable install)
pip install -e .
```

---

## Related documentation

- [README.md](README.md) — setup, architecture, configuration, API patterns
- [schema/README.md](schema/README.md) — FHIR schema assets and indexes
- [.cursor/skills/fhir-resource-config/](.cursor/skills/fhir-resource-config/) — adding new resources
