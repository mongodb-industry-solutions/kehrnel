# FHIR-HDL-Synthetic-Conditions — SYNTHETIC FHIR R5 Condition demonstration data

> **THESE ARE NOT REAL PATIENTS AND NOT REAL DIAGNOSES.**
> Every Patient and every Condition in this directory is machine-fabricated for
> demonstration purposes. Nothing here describes anything that happened to
> anybody.

| | |
|---|---|
| dataset id | `hdl-syn-conditions-1` |
| FHIR release | R5 |
| terminology release | `hdl-synthetic-2` (itself synthetic — see below) |
| generator | `kehrnel/scripts/generate_synthetic_fhir_conditions.py` |
| verifier | `kehrnel/scripts/verify_synthetic_fhir_conditions.py` |
| seed | `20260601` (deterministic — reruns are byte-identical) |

## Honesty / provenance note

- **Fabricated patients.** Patients are **pseudonymous by construction**: no
  `name`, no `address`, no `telecom`, no `contact`, no `photo`, and no
  `subject.display` on any Condition. No realistic human names are invented
  anywhere — not even fake ones — so there is nothing that could be mistaken
  for an identity. Subjects are referenced only as `Patient/hdl-syn-p0001`.
  `birthDate` is **year precision only**, deliberately: a full date of birth
  plus a diagnosis set is a quasi-identifier even on fabricated data.
- **Fabricated diagnoses.** The code on each Condition is drawn from
  `.kehrnel/snomedct/releases/hdl-synthetic-2.json`, which is a **hand-authored
  synthetic hierarchy** imitating the document shape of a SNOMED CT
  distribution. Some identifiers in that release are real, widely published
  SNOMED identifiers used **only as references**; most are openly synthetic
  `9999xxxxx` ids. See
  [`README-hdl-synthetic-2.md`](../../../../.kehrnel/snomedct/releases/README-hdl-synthetic-2.md).
  **No SNOMED CT licence is held** by the operator of this repository and no
  bulk SNOMED CT content has been copied.
- **The `display` text is not invented.** Each coding's `display` is the
  release's own preferred term, or its FSN with the trailing semantic tag
  stripped (`Asthma (disorder)` → `Asthma`). No display string is authored here.
- **The clinical patterns are heuristics, not epidemiology.** Comorbidity
  clustering, age banding and onset spread are crude hand-tuned rules chosen so
  that cohort and temporal demo questions return non-trivial answers. They carry
  **no prevalence, incidence or risk information** whatsoever.
- **Must not be used clinically** — not for decision support, coding, billing,
  research, or any purpose where either the terminology or the clinical
  plausibility matters.

Use it for: exercising cohort expansion, parent-concept rollup, temporal
filters, projection and search breadth. Do not use it for: anything involving a
patient.

## Why it exists

The demo's cohort lane
(`HealthcareDataLab/src/lib/datalab/clinicalTerminologyResolution.js`) resolves a
question to a concept, expands it to `<<code`, and then **intersects** that
expansion with the codes the Condition store actually holds. That intersection
is deliberate — executing an expanded code the store has never seen would
overstate the query — but it also means terminology breadth alone cannot make a
question answerable. With 2978 concepts available and only **14** distinct codes
in the store, nearly everything stayed correctly *unanswerable*. This dataset
closes the gap on the data side: **526 distinct codes** across **22 clinical
areas**.

## Files and format

| file | resourceType | count |
|---|---|---|
| `patients.ndjson` | Patient | 200 |
| `conditions.ndjson` | Condition | 872 |
| `manifest.json` | — | dataset metadata |

**Format is NDJSON — one FHIR resource per line.** This matches the bulk-import
contract, not the Synthea samples:

- `kehrnel/src/kehrnel/engine/strategies/fhir/resource_store/scripts/import_resources.py`
  (`parse_import_payload`) accepts `ndjson`, a `resources` array, or a
  **collection/searchset** Bundle, and **explicitly rejects `transaction` and
  `batch` Bundles** with `FHIR_BUNDLE_INTERACTION_UNSUPPORTED` — their
  `entry.request` rows are executable FHIR interactions that Kehrnel will not
  silently flatten. The existing samples under
  `FHIR-Synthea-mCode-10-yeard/` are *transaction* Bundles, so they are **not**
  the convention to copy for a bulk-loadable dataset.
- NDJSON also streams on both sides: the importer chunks it, and the generator
  writes it one resource per line, so neither side materialises the whole
  dataset in memory.
- The per-source directory layout (`samples/in/data/<SOURCE-NAME>/`) matches the
  existing siblings `EHR/`, `FHIR-Synthea-mCode-10-yeard/` and `edi_x12/`.

**Load `patients.ndjson` first.** A bounded import is not a transaction across
resource collections, so `subject.reference` resolves cleanly only if the
Patients already exist.

```bash
kehrnel fhir import-data ./patients.ndjson   --env dev --mode upsert --execute
kehrnel fhir import-data ./conditions.ndjson --env dev --mode upsert --execute
```

The generator **never writes to MongoDB**; it produces files only.

## Measured properties (from the verifier, seed 20260601)

| property | value |
|---|---|
| Patients | 200 |
| Conditions | 872 |
| distinct SNOMED codes | **526** |
| codes present in `hdl-synthetic-2` | **526 / 526 (100.0%)** |
| onset range | `2010-01-04` .. `2024-12-31` (all 15 years populated, 46–78 per year) |
| conditions per patient | mean 4.36, range 1–10 |
| clinical areas covered | 22 of 22 |
| obstetric codes on non-female patients | 0 |
| engine `validate_resource` errors (R5, `base`) | 0 of 1072 |

Distinct generated codes matched by a broad parent — this is exactly what a
parent-concept cohort question returns:

| parent | | distinct codes | conditions |
|---|---|---|---|
| `<<49601007` | Disorder of cardiovascular system | 113 | 195 |
| `<<50043002` | Disorder of respiratory system | 51 | 97 |
| `<<40733004` | Infectious disease | 38 | 61 |
| `<<928000` | Disorder of musculoskeletal system | 33 | 54 |
| `<<53619000` | Disorder of digestive system | 33 | 52 |
| `<<362969004` | Disorder of endocrine system | 28 | 75 |
| `<<108369006` | Neoplasm | 27 | 31 |
| `<<118940003` | Disorder of nervous system | 26 | 48 |
| `<<42030000` | Disorder of urinary system | 26 | 42 |
| `<<95320005` | Disorder of skin | 25 | 35 |
| `<<73211009` | Diabetes mellitus | 23 | 62 |
| `<<74732009` | Mental disorder | 21 | 36 |
| `<<195967001` | Asthma | 21 | 39 |
| `<<84114007` | Heart failure | 20 | 29 |
| `<<709044004` | Chronic kidney disease | 17 | 26 |
| `<<13645005` | Chronic obstructive lung disease | 12 | 19 |

Comorbidity co-occurrence **lift** over independence (observed co-occurring
patients / expected if the two were independent) — measured by the verifier, not
asserted:

| pair | both | expected | lift |
|---|---|---|---|
| type 2 diabetes + chronic kidney disease | 14 | 4.0 | **3.47×** |
| heart failure + atrial fibrillation | 9 | 1.8 | **4.95×** |
| ischaemic heart disease + hyperlipidaemia | 13 | 3.8 | **3.42×** |
| heart failure + chronic kidney disease | 10 | 3.6 | **2.75×** |
| COPD + infectious disease | 9 | 4.3 | **2.08×** |
| type 2 diabetes + hypertension | 10 | 6.0 | **1.65×** |

## `onset[x]` choice safety

`Condition.onset[x]` is a FHIR **choice** element: at most one of
`onsetDateTime` / `onsetPeriod` / … may be present. A previous synthetic
generator emitted both, and because the temporal projection indexes *every*
onset element as its own interval, a 2016 diagnosis carrying a stray 2024 period
matched `onset-date=ge2018` — "patients with COPD since 2018" returned a 2016
diagnosis. See
`HealthcareDataLab/docs/clinical-semantic-query-runtime-diagnosis.md` §3d.
Every Condition here carries **exactly one** onset sibling, and the verifier
fails if that ever stops being true.

## Regenerating and verifying

```bash
python kehrnel/scripts/generate_synthetic_fhir_conditions.py
python kehrnel/scripts/verify_synthetic_fhir_conditions.py
```

The generator seeds its RNG (`--seed`, default `20260601`) and selects codes
programmatically from the release file, so the same release plus the same seed
produces byte-identical output.
