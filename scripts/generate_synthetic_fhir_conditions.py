"""
Generate a SYNTHETIC FHIR R5 Condition + Patient dataset spanning hdl-synthetic-2.

=============================================================================
HONESTY NOTICE - READ BEFORE USING THIS FILE OR ITS OUTPUT
=============================================================================
This script does NOT produce real patient data, and it does NOT produce real
clinical content.

  * Every Patient and every Condition written here is MACHINE-FABRICATED for
    demonstration purposes. No person, encounter, diagnosis, onset date or
    care episode described in the output corresponds to anything that happened.
  * Patients are PSEUDONYMOUS BY CONSTRUCTION: no `name`, no address, no
    telecom, no `subject.display`. Subjects are referenced only as
    `Patient/hdl-syn-p0001`. No realistic human names are invented anywhere -
    not even fake ones - so there is nothing to mistake for an identity.
  * The SNOMED codes carried by each Condition are drawn from
    `.kehrnel/snomedct/releases/hdl-synthetic-2.json`, which is itself a
    HAND-AUTHORED SYNTHETIC hierarchy imitating the document shape of a SNOMED
    CT distribution. Some identifiers in that release are real, widely
    published SNOMED identifiers used only as *references*; most are openly
    synthetic 9999xxxxx ids. See
    `.kehrnel/snomedct/releases/README-hdl-synthetic-2.md`.
  * The CLINICAL PATTERNS (comorbidity clustering, age banding, onset spread)
    are crude hand-tuned heuristics chosen so that cohort and temporal demo
    questions return non-trivial answers. They are NOT epidemiology and carry
    NO prevalence, incidence or risk information.
  * This dataset MUST NOT be used clinically, for decision support, for coding,
    for billing, for research, or for any purpose where either the terminology
    or the clinical plausibility matters.

Use it for: exercising cohort expansion, parent-concept rollup, temporal
filters, projection and search breadth in the HealthcareDataLab demo.
Do not use it for: anything involving a patient.
=============================================================================

WHY THIS EXISTS
---------------
The demo's cohort lane (HealthcareDataLab/src/lib/datalab/clinicalTerminologyResolution.js,
`discoverStoreCodes` + `resolveExecutableCodes`) resolves a question to a concept,
expands it to `<<code`, and then INTERSECTS that expansion with the codes the
Condition store actually holds. Executing an expanded code the store has never
seen would overstate the query, so the intersection is deliberate - but it also
means terminology breadth alone cannot make a question answerable. With 2978
concepts available and only 14 distinct codes in the store, nearly everything
stayed correctly UNANSWERABLE. This generator closes that gap on the data side.

OUTPUT FORMAT AND LOCATION
--------------------------
Output is **NDJSON**, one FHIR resource per line, under
`kehrnel/samples/in/data/FHIR-HDL-Synthetic-Conditions/`.

  * NDJSON (not a transaction Bundle) because
    `kehrnel/src/kehrnel/engine/strategies/fhir/resource_store/scripts/import_resources.py`
    `parse_import_payload` accepts `ndjson` / `resources` / `collection` Bundles and
    EXPLICITLY REJECTS `transaction` and `batch` Bundles
    (`FHIR_BUNDLE_INTERACTION_UNSUPPORTED`) - their `entry.request` rows are
    executable interactions the bulk importer will not silently flatten. The
    existing Synthea samples under `samples/in/data/FHIR-Synthea-mCode-10-yeard/`
    are transaction Bundles and therefore are NOT the convention to copy for a
    bulk-loadable dataset.
  * NDJSON also streams: the importer chunks it (`fhir import-data ./x.ndjson`,
    `Content-Type: application/fhir+ndjson`), and this generator writes it one
    resource per line so neither side ever materialises the whole dataset.
  * `samples/in/data/<SOURCE-NAME>/` matches the existing per-source directory
    convention in that tree (`EHR/`, `FHIR-Synthea-mCode-10-yeard/`, `edi_x12/`).

Patients and Conditions are written to SEPARATE files so Patients can be loaded
first - `import_resources` is not a cross-collection transaction, so subject
references resolve cleanly only if the Patients already exist.

NOTHING IS WRITTEN TO MONGODB. This script produces files only.

Usage:
    python kehrnel/scripts/generate_synthetic_fhir_conditions.py
    python kehrnel/scripts/generate_synthetic_fhir_conditions.py --patients 200 --seed 20260601
    python kehrnel/scripts/generate_synthetic_fhir_conditions.py --out-dir /tmp/x
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterator

REPO = Path(__file__).resolve().parents[1]

RELEASE_FILE = REPO / ".kehrnel/snomedct/releases/hdl-synthetic-2.json"
RELEASE_ID = "hdl-synthetic-2"
DEFAULT_OUT_DIR = REPO / "samples/in/data/FHIR-HDL-Synthetic-Conditions"

DATASET_ID = "hdl-syn-conditions-1"
PATIENT_ID_PREFIX = "hdl-syn-p"
CONDITION_ID_PREFIX = "hdl-syn-c"

SNOMED_SYSTEM = "http://snomed.info/sct"
CLINICAL_STATUS_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-clinical"
VERIFICATION_STATUS_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-ver-status"
CATEGORY_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-category"

TYPE_FSN = "900000000000003001"
TYPE_SYNONYM = "900000000000013009"
LANG_REFSET = "900000000000509007"
ACCEPT_PREFERRED = "900000000000548007"

# Onset window. The pre-existing demo dataset's onsets stopped at 2023-12-24 and
# the lane only emits an onset filter when the question names a year, so a
# "since 2015" question needs onsets on BOTH sides of 2015 to be meaningful.
ONSET_START = date(2010, 1, 1)
ONSET_END = date(2024, 12, 31)

DEFAULT_PATIENTS = 200
DEFAULT_SEED = 20260601

# --------------------------------------------------------------------------
# Clinical areas, keyed by the release's own area-root concept ids.
#
# The four 9999100xx roots are the synthetic area parents authored by
# generate_synthetic_snomed_release.py (haematology, ENT, obstetric,
# paediatric); the rest are the real-referenced body-system parents. `weight`
# biases how many DISTINCT codes each area contributes, roughly tracking how
# much of the release sits under it.
# --------------------------------------------------------------------------

AREAS: dict[str, dict[str, Any]] = {
    # Cardiovascular is deliberately NOT weighted in proportion to its share of
    # the release (405 of 2978 concepts). Every age band leans cardiovascular,
    # and the comorbidity clusters are mostly cardio-renal-metabolic, so a
    # proportional weight here crowded the other areas out of the palette.
    "cardiovascular": {"root": "49601007", "weight": 38},
    "respiratory": {"root": "50043002", "weight": 30},
    "endocrine": {"root": "362969004", "weight": 18},
    "metabolic": {"root": "75934005", "weight": 14},
    "renal-urinary": {"root": "42030000", "weight": 16},
    "neurological": {"root": "118940003", "weight": 26},
    "psychiatric": {"root": "74732009", "weight": 22},
    "musculoskeletal": {"root": "928000", "weight": 40},
    "infectious-disease": {"root": "40733004", "weight": 34},
    "oncology": {"root": "108369006", "weight": 32},
    "dermatology": {"root": "95320005", "weight": 18},
    "gastrointestinal": {"root": "53619000", "weight": 28},
    "hepatic": {"root": "235856003", "weight": 14},
    "haematology": {"root": "999910001", "weight": 16},
    "haematology-proliferative": {"root": "414022008", "weight": 14},
    "ophthalmology": {"root": "371405004", "weight": 22},
    "ent": {"root": "999910002", "weight": 18},
    "obstetric": {"root": "999910003", "weight": 20},
    "paediatric": {"root": "999910004", "weight": 22},
    "injury-trauma": {"root": "417746004", "weight": 22},
    "immunology": {"root": "414029004", "weight": 14},
    # connective-tissue has only 13 concepts in the release, so its palette is
    # pool-capped regardless of weight; the weight is raised anyway so the cap
    # is what limits it rather than the share calculation.
    "connective-tissue": {"root": "105969002", "weight": 12},
}

# Which areas are plausible at which life stage, and how strongly.
#
# A paediatric patient must not be handed an obstetric or a geriatric
# degenerative diagnosis, and vice versa - but a flat list is not enough either.
# With only ~14% of 200 patients in the paediatric band, drawing uniformly over
# 14 permitted areas gave the paediatric-specific subtree (999910004) barely two
# Conditions in the whole dataset, so "neonatal and paediatric disorder" as a
# cohort question stayed effectively unanswerable. Each band therefore WEIGHTS
# its characteristic areas well above its merely-permitted ones.
AGE_BANDS: list[dict[str, Any]] = [
    {
        "band": "paediatric",
        "share": 0.14,
        "age_range": (0, 17),
        "areas": [
            ("paediatric", 34), ("respiratory", 14), ("infectious-disease", 13),
            ("ent", 10), ("dermatology", 8), ("neurological", 5),
            ("injury-trauma", 5), ("immunology", 4), ("haematology", 3),
            ("ophthalmology", 3), ("musculoskeletal", 3),
            ("gastrointestinal", 3), ("psychiatric", 3), ("endocrine", 2),
        ],
    },
    {
        "band": "young-adult",
        "share": 0.18,
        "age_range": (18, 39),
        "areas": [
            # obstetric is weighted high because the sex gate below removes it
            # for roughly half the band; the effective share lands near the
            # psychiatric one.
            ("psychiatric", 14), ("obstetric", 26), ("infectious-disease", 11),
            ("respiratory", 9), ("injury-trauma", 8), ("dermatology", 7),
            ("gastrointestinal", 7), ("musculoskeletal", 7), ("ent", 5),
            ("neurological", 5), ("immunology", 4), ("endocrine", 4),
            ("haematology", 3), ("ophthalmology", 3),
            ("connective-tissue", 3), ("metabolic", 3),
        ],
    },
    {
        "band": "middle-age",
        "share": 0.30,
        "age_range": (40, 64),
        "areas": [
            ("cardiovascular", 16), ("endocrine", 9), ("metabolic", 8),
            ("musculoskeletal", 8), ("respiratory", 7),
            ("gastrointestinal", 7), ("psychiatric", 6), ("hepatic", 5),
            ("oncology", 5), ("renal-urinary", 5), ("neurological", 4),
            ("dermatology", 4), ("ophthalmology", 4), ("ent", 3),
            ("infectious-disease", 3), ("haematology", 4),
            ("injury-trauma", 3), ("connective-tissue", 3),
            ("immunology", 3), ("haematology-proliferative", 4),
            ("obstetric", 4),
        ],
    },
    {
        "band": "older-adult",
        "share": 0.24,
        "age_range": (65, 79),
        "areas": [
            ("cardiovascular", 18), ("musculoskeletal", 9), ("oncology", 8),
            ("respiratory", 8), ("renal-urinary", 7), ("endocrine", 6),
            ("metabolic", 6), ("neurological", 6), ("ophthalmology", 5),
            ("gastrointestinal", 5), ("haematology", 4),
            ("haematology-proliferative", 4), ("hepatic", 3),
            ("psychiatric", 3), ("ent", 3), ("dermatology", 3),
            ("injury-trauma", 3), ("infectious-disease", 3),
            ("connective-tissue", 2), ("immunology", 2),
        ],
    },
    {
        "band": "geriatric",
        "share": 0.14,
        "age_range": (80, 96),
        "areas": [
            ("cardiovascular", 19), ("musculoskeletal", 11),
            ("neurological", 9), ("renal-urinary", 8), ("respiratory", 7),
            ("oncology", 6), ("ophthalmology", 6), ("haematology", 5),
            ("endocrine", 4), ("metabolic", 4), ("psychiatric", 4),
            ("infectious-disease", 4), ("gastrointestinal", 4), ("ent", 3),
            ("dermatology", 3), ("injury-trauma", 3),
            ("haematology-proliferative", 3),
        ],
    },
]

# Comorbidity clusters: an anchor concept that, when drawn, pulls in companions
# far more often than chance. Codes are anchors in the release (verified at run
# time); companions are drawn from the anchor's own subtree OR from a named
# partner subtree, so the cluster stays inside plausible territory.
#
# Each entry: anchor concept id -> list of (partner concept id, probability).
COMORBIDITY_CLUSTERS: dict[str, dict[str, Any]] = {
    "metabolic-renal-vascular": {
        "anchor": "44054006",          # Diabetes mellitus type 2
        "partners": [
            ("709044004", 0.55),        # Chronic kidney disease
            ("38341003", 0.70),         # Hypertensive disorder
            ("55822004", 0.50),         # Hyperlipidemia
            ("414545008", 0.30),        # Ischemic heart disease
        ],
        "bands": ["middle-age", "older-adult", "geriatric"],
    },
    "cardiorenal": {
        "anchor": "84114007",          # Heart failure
        "partners": [
            ("49436004", 0.45),         # Atrial fibrillation
            ("709044004", 0.40),        # Chronic kidney disease
            ("38341003", 0.60),         # Hypertensive disorder
            ("414545008", 0.45),        # Ischemic heart disease
        ],
        "bands": ["older-adult", "geriatric", "middle-age"],
    },
    "atherosclerotic": {
        "anchor": "414545008",         # Ischemic heart disease
        "partners": [
            ("38341003", 0.65),
            ("55822004", 0.60),
            ("38716007", 0.40),         # Atherosclerosis
            ("22298006", 0.30),         # Myocardial infarction
        ],
        "bands": ["middle-age", "older-adult", "geriatric"],
    },
    "airways": {
        "anchor": "13645005",          # Chronic obstructive lung disease
        "partners": [
            ("84114007", 0.25),
            ("38341003", 0.35),
            ("40733004", 0.30),         # Infectious disease (chest infections)
        ],
        "bands": ["middle-age", "older-adult", "geriatric"],
    },
    "atopy": {
        "anchor": "195967001",         # Asthma
        "partners": [
            ("95320005", 0.35),         # Disorder of skin (eczema territory)
            ("999910002", 0.35),        # ENT (rhinitis territory)
            ("414029004", 0.20),        # Immune disorder
        ],
        "bands": ["paediatric", "young-adult", "middle-age"],
    },
}

CLINICAL_STATUS_WEIGHTS = [
    ("active", "Active", 62),
    ("resolved", "Resolved", 18),
    ("remission", "Remission", 8),
    ("inactive", "Inactive", 7),
    ("recurrence", "Recurrence", 5),
]
VERIFICATION_STATUS_WEIGHTS = [
    ("confirmed", "Confirmed", 82),
    ("provisional", "Provisional", 10),
    ("differential", "Differential", 5),
    ("unconfirmed", "Unconfirmed", 3),
]
CATEGORY_WEIGHTS = [
    ("problem-list-item", "Problem List Item", 70),
    ("encounter-diagnosis", "Encounter Diagnosis", 30),
]

# Conditions per patient. Most patients carry 1-6; a small tail carries more,
# which is what makes multi-condition cohort intersections non-empty. The
# distribution is deliberately shifted toward the middle of that range rather
# than toward 1: with ~200 patients, a 1-2 heavy distribution leaves most of the
# code palette unused, and an unused code is an unanswerable question.
CONDITION_COUNT_WEIGHTS = [
    (1, 8), (2, 14), (3, 18), (4, 18), (5, 14), (6, 11),
    (7, 7), (8, 5), (9, 3), (10, 2),
]


# --------------------------------------------------------------------------
# Release reading
# --------------------------------------------------------------------------


def strip_semantic_tag(term: str) -> str:
    """'Asthma (disorder)' -> 'Asthma'. Leaves untagged terms alone."""
    text = (term or "").strip()
    if text.endswith(")") and "(" in text:
        head, _, _tail = text.rpartition("(")
        stripped = head.strip()
        if stripped:
            return stripped
    return text


def load_release(path: Path) -> dict[str, dict[str, Any]]:
    """
    Read the release into {conceptId: {display, ancestors, is_leaf, child_count}}.

    `json.load` on the 2978-concept release is ~fine, but the per-concept detail
    (descriptions, relationships) is 10x what is needed and this host is memory
    constrained, so the full document is dropped as soon as the slim index is
    built.
    """
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)

    index: dict[str, dict[str, Any]] = {}
    for concept in raw:
        cid = str(concept.get("conceptId") or "")
        if not cid or concept.get("active") is not True:
            continue
        fsn = ""
        preferred = ""
        for desc in concept.get("descriptions", []):
            if desc.get("active") is not True:
                continue
            term = str(desc.get("term") or "")
            type_id = str(desc.get("typeId") or "")
            if type_id == TYPE_FSN and not fsn:
                fsn = term
            elif type_id == TYPE_SYNONYM and not preferred:
                acceptability = desc.get("acceptabilityMap") or {}
                if str(acceptability.get(LANG_REFSET) or "") == ACCEPT_PREFERRED:
                    preferred = term
        # Display: the preferred synonym when the release marks one, otherwise
        # the FSN with its semantic tag stripped. Both come from the release -
        # no display text is invented here.
        display = preferred or strip_semantic_tag(fsn)
        children = [str(c) for c in concept.get("inferredChildIds", [])]
        index[cid] = {
            "code": cid,
            "display": display,
            "fsn": fsn,
            "semanticTag": fsn[fsn.rfind("(") + 1 : -1] if fsn.endswith(")") and "(" in fsn else "",
            "ancestors": frozenset(str(a) for a in concept.get("inferredAncestorIds", [])),
            "childCount": len(children),
        }
    del raw
    return index


def selectable(concept: dict[str, Any]) -> bool:
    """
    Only findings/disorders are eligible as a Condition.code.

    Procedures, products, observables and body structures live in the same
    release but are not diagnoses; coding one as a Condition would be a lie
    about the data, not a shortcut.
    """
    return concept["semanticTag"] in {"disorder", "finding", "morphologic abnormality"}


# --------------------------------------------------------------------------
# Code-palette selection (deterministic, seeded)
# --------------------------------------------------------------------------


def build_area_pools(index: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    """Per-area candidate codes, restricted to LEAF and MID-LEVEL concepts.

    Mid-level/leaf matters for the demo: if the dataset only carried broad
    parents, a question like `<<50043002` would match one code and the "cohort"
    would be a single diagnosis. Codes with many children are excluded so that
    a parent rollup genuinely aggregates distinct child diagnoses.
    """
    pools: dict[str, list[str]] = {name: [] for name in AREAS}
    for cid, concept in index.items():
        if not selectable(concept):
            continue
        # <= 3 children keeps leaves and narrow mid-level concepts, and drops the
        # broad body-system / organ parents that would make rollups degenerate.
        if concept["childCount"] > 3:
            continue
        for name, spec in AREAS.items():
            if spec["root"] in concept["ancestors"]:
                pools[name].append(cid)
    for name in pools:
        pools[name].sort()  # stable order before any seeded sampling
    return pools


def select_palette(
    pools: dict[str, list[str]],
    rng: random.Random,
    target_codes: int,
) -> dict[str, list[str]]:
    """Pick the per-area DISTINCT code palette the dataset will cover."""
    total_weight = sum(spec["weight"] for spec in AREAS.values())
    palette: dict[str, list[str]] = {}
    for name, spec in AREAS.items():
        pool = pools[name]
        if not pool:
            palette[name] = []
            continue
        want = max(4, round(target_codes * spec["weight"] / total_weight))
        want = min(want, len(pool))
        palette[name] = sorted(rng.sample(pool, want))
    return palette


def weighted_choice(rng: random.Random, options: list[tuple[Any, ...]]) -> tuple[Any, ...]:
    """Pick one (value, ..., weight) row, weight last."""
    total = sum(row[-1] for row in options)
    draw = rng.uniform(0, total)
    upto = 0.0
    for row in options:
        upto += row[-1]
        if draw <= upto:
            return row
    return options[-1]


# --------------------------------------------------------------------------
# Resource construction
# --------------------------------------------------------------------------


def patient_resource(patient_id: str, band: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    """
    A deliberately minimal, PSEUDONYMOUS Patient.

    No `name`, no `address`, no `telecom`, no `birthDate` precise enough to be
    a quasi-identifier - only a birth YEAR, which is what age-appropriateness
    demos need. The identifier is the dataset's own synthetic key and is
    explicitly labelled as such.
    """
    low, high = band["age_range"]
    age = rng.randint(low, high)
    birth_year = ONSET_END.year - age
    gender = rng.choice(["female", "male", "other", "unknown"]) if rng.random() < 0.04 else rng.choice(["female", "male"])
    return {
        "resourceType": "Patient",
        "id": patient_id,
        "meta": {"tag": [{"system": "urn:hdl:synthetic", "code": DATASET_ID, "display": "Synthetic demonstration data - not a real person"}]},
        "identifier": [
            {
                "system": f"urn:hdl:synthetic:{DATASET_ID}:patient",
                "value": patient_id,
            }
        ],
        "active": True,
        "gender": gender,
        # Year precision only. Deliberately coarse: a full DOB plus a diagnosis
        # set is a quasi-identifier even on fabricated data.
        "birthDate": str(birth_year),
    }


def random_onset(rng: random.Random, patient_birth_year: int, min_age: int = 0) -> str:
    """An onsetDateTime inside [ONSET_START, ONSET_END], after birth, and not
    before the patient reaches `min_age`.

    `min_age` carries the concept's own age floor (see ADULT_ONLY_TERMS). Without
    it the age gate could still be defeated by the date draw: an adult-only
    diagnosis selected for a patient who is 40 by the end of the window could
    otherwise be dated to the year they were born.
    """
    earliest = max(ONSET_START, date(patient_birth_year + max(min_age, 0), 1, 1))
    if earliest > ONSET_END:
        earliest = max(ONSET_START, date(patient_birth_year, 1, 1))
    if earliest > ONSET_END:
        earliest = ONSET_START
    span = (ONSET_END - earliest).days
    when = earliest + timedelta(days=rng.randint(0, max(span, 0)))
    hour = rng.randint(7, 19)
    minute = rng.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
    return f"{when.isoformat()}T{hour:02d}:{minute:02d}:00+00:00"


def condition_resource(
    condition_id: str,
    patient_id: str,
    concept: dict[str, Any],
    area: str,
    onset: str,
    rng: random.Random,
) -> dict[str, Any]:
    """One FHIR R5 Condition.

    `onset[x]` is a CHOICE element: exactly ONE onset sibling is emitted. The
    previous synthetic generator emitted onsetDateTime AND onsetPeriod on some
    resources, and because the temporal projection indexes every onset element
    as its own interval, a 2016 diagnosis with a stray 2024 period matched
    `onset-date=ge2018`. See
    HealthcareDataLab/docs/clinical-semantic-query-runtime-diagnosis.md §3d.
    """
    clinical_code, clinical_display, _ = weighted_choice(rng, CLINICAL_STATUS_WEIGHTS)
    verification_code, verification_display, _ = weighted_choice(rng, VERIFICATION_STATUS_WEIGHTS)
    category_code, category_display, _ = weighted_choice(rng, CATEGORY_WEIGHTS)

    resource: dict[str, Any] = {
        "resourceType": "Condition",
        "id": condition_id,
        "meta": {
            "tag": [
                {
                    "system": "urn:hdl:synthetic",
                    "code": DATASET_ID,
                    "display": "Synthetic demonstration data - not a real diagnosis",
                }
            ]
        },
        "clinicalStatus": {
            "coding": [
                {
                    "system": CLINICAL_STATUS_SYSTEM,
                    "code": clinical_code,
                    "display": clinical_display,
                }
            ]
        },
        "verificationStatus": {
            "coding": [
                {
                    "system": VERIFICATION_STATUS_SYSTEM,
                    "code": verification_code,
                    "display": verification_display,
                }
            ]
        },
        "category": [
            {
                "coding": [
                    {
                        "system": CATEGORY_SYSTEM,
                        "code": category_code,
                        "display": category_display,
                    }
                ]
            }
        ],
        "code": {
            "coding": [
                {
                    "system": SNOMED_SYSTEM,
                    "code": concept["code"],
                    "display": concept["display"],
                }
            ],
            "text": concept["display"],
        },
        # No `subject.display`: the demo strips it, and supplying one would mean
        # inventing a human label for a person who does not exist.
        "subject": {"reference": f"Patient/{patient_id}"},
        "onsetDateTime": onset,
        "recordedDate": onset,
        "extension": [
            {
                "url": "urn:hdl:synthetic:clinical-area",
                "valueString": area,
            }
        ],
    }
    return resource


# --------------------------------------------------------------------------
# Cohort assembly
# --------------------------------------------------------------------------


def choose_band(rng: random.Random) -> dict[str, Any]:
    rows = [(band, band["share"]) for band in AGE_BANDS]
    return weighted_choice(rng, rows)[0]


# Concept-level terms that are implausible in a young child, with the minimum
# age at which each becomes believable.
#
# Why this exists ON TOP of the per-band AREA weighting above: the area gate is
# too coarse to be the only guard. "respiratory" is entirely plausible for a
# 1-year-old, but "Pickwickian syndrome" is not; "neurological" is fine, but
# "cardioembolic stroke" is not. Gating only by area produced 93 of 872
# Conditions (10.7%) on patients aged <= 12, including stroke, seropositive
# rheumatoid arthritis and ventilator-associated pneumonia in infants - and a
# demo that returns a 0-year-old with a cardioembolic stroke discredits the
# whole cohort lane, however correct the terminology plumbing underneath is.
#
# Matched against the lower-cased FSN/display, so it catches the generated
# severity/chronicity variants ("Mild form of cardioembolic stroke") too.
ADULT_ONLY_TERMS: list[tuple[int, tuple[str, ...]]] = [
    (18, (
        "stroke", "myocardial infarction", "angina", "coronary", "atheroscler",
        "aortic aneurysm", "peripheral arterial", "claudication",
        "atrial fibrillation", "atrial flutter", "heart failure",
        "cardiomyopathy", "hypertensive", "hyperlipidaemia", "hyperlipidemia",
        "hypercholesterol", "type 2 diabetes", "metabolic syndrome",
        "chronic obstructive", "emphysema", "pickwickian",
        "obstructive sleep apnoea", "obstructive sleep apnea",
        "chronic kidney disease", "renal failure", "benign prostatic",
        "erectile", "menopaus", "osteoarthritis", "osteoporosis",
        "rheumatoid arthritis", "gout", "lumbago", "spondylo", "disc degener",
        "cataract", "glaucoma", "macular degener", "presbyop",
        "dementia", "alzheimer", "parkinson", "cirrhosis", "fatty liver",
        "carcinoma", "adenocarcinoma", "melanoma", "lymphoma", "myeloma",
        "leukaemia, chronic", "chronic lymphocytic", "chronic myeloid",
        "barrett", "diverticul", "haemorrhoid", "hemorrhoid",
        "acquired immune deficiency", "ventilator-associated",
        "chronic migraine", "sensorineural hearing loss", "tinnitus",
        "polytrauma", "hip fracture", "pulmonary fibrosis",
        "varicose", "cellulitis", "psoriasis", "lyme disease", "malaria",
    )),
    (10, (
        "asthma, non-allergic", "non-allergic asthma", "exercise-induced asthma",
        "iga deficiency", "common variable immunodeficiency",
        "guttate psoriasis", "pharyngitis, chronic", "chronic acute pharyngitis",
        "mumps",
    )),
]


def min_age_for_concept(concept: dict[str, Any]) -> int:
    """Lowest plausible age for this diagnosis (0 when unconstrained)."""
    haystack = f"{concept.get('display') or ''} {concept.get('fsn') or ''}".lower()
    floor = 0
    for age, needles in ADULT_ONLY_TERMS:
        if any(needle in haystack for needle in needles):
            floor = max(floor, age)
    return floor


def age_permits(concept: dict[str, Any], age: int) -> bool:
    return age >= min_age_for_concept(concept)


def descendants_of(index: dict[str, dict[str, Any]], root: str) -> list[str]:
    """Selectable leaf/mid-level codes under `root` (inclusive)."""
    out = [
        cid
        for cid, concept in index.items()
        if (root in concept["ancestors"] or cid == root)
        and selectable(concept)
        and concept["childCount"] <= 3
    ]
    out.sort()
    return out


def generate(
    index: dict[str, dict[str, Any]],
    palette: dict[str, list[str]],
    cluster_pools: dict[str, dict[str, list[str]]],
    patients: int,
    rng: random.Random,
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield ('Patient'|'Condition', resource) one at a time - never a list."""
    code_to_area = {}
    for area, codes in palette.items():
        for code in codes:
            code_to_area.setdefault(code, area)

    cluster_names = list(COMORBIDITY_CLUSTERS)
    condition_seq = 0
    used_codes: set[str] = set()   # every code that already has >=1 Condition

    for n in range(1, patients + 1):
        patient_id = f"{PATIENT_ID_PREFIX}{n:04d}"
        band = choose_band(rng)
        patient = patient_resource(patient_id, band, rng)
        birth_year = int(patient["birthDate"])
        # Age at the END of the onset window: the most generous age this patient
        # ever reaches in the dataset. A concept is rejected only when it is
        # implausible even then, so the gate never removes a diagnosis that some
        # valid onset date could justify.
        max_age = ONSET_END.year - birth_year
        yield "Patient", patient

        count = weighted_choice(rng, CONDITION_COUNT_WEIGHTS)[0]
        # (area, weight) rows for this band, dropping areas whose palette is
        # empty so a weight can never point at nothing.
        band_areas = [(a, w) for a, w in band["areas"] if palette.get(a)]
        # Obstetric disorders are gated on recorded sex. Handing a pregnancy or
        # puerperal diagnosis to a patient recorded male would be an obvious
        # incoherence in a dataset whose whole purpose is to look plausible.
        if patient.get("gender") != "female":
            band_areas = [(a, w) for a, w in band_areas if a != "obstetric"]
        if not band_areas:
            band_areas = [(a, 1) for a in palette if palette[a]]

        chosen: list[tuple[str, str]] = []   # (code, area)
        seen_codes: set[str] = set()

        # 1. Comorbidity clusters. Drawn BEFORE the random fill so that a
        #    clustered patient's remaining slots still get area spread.
        eligible_clusters = [
            name
            for name in cluster_names
            if band["band"] in COMORBIDITY_CLUSTERS[name]["bands"]
            and cluster_pools.get(name, {}).get("anchor")
        ]
        if eligible_clusters and count >= 2 and rng.random() < 0.42:
            name = rng.choice(eligible_clusters)
            spec = COMORBIDITY_CLUSTERS[name]
            pools = cluster_pools[name]
            anchor_code = rng.choice(pools["anchor"])
            if anchor_code not in seen_codes and age_permits(index[anchor_code], max_age):
                chosen.append((anchor_code, code_to_area.get(anchor_code, pools["anchor_area"])))
                seen_codes.add(anchor_code)
            for partner_root, probability in spec["partners"]:
                if len(chosen) >= count:
                    break
                if rng.random() >= probability:
                    continue
                partner_pool = pools["partners"].get(partner_root) or []
                if not partner_pool:
                    continue
                code = rng.choice(partner_pool)
                if code in seen_codes or not age_permits(index[code], max_age):
                    continue
                chosen.append((code, code_to_area.get(code, pools["partner_areas"].get(partner_root, "cardiovascular"))))
                seen_codes.add(code)

        # 2. Age-appropriate random fill from the per-area palette.
        #
        # Codes not yet used ANYWHERE in the dataset are preferred (two draws,
        # keep the unused one). Pure uniform sampling over a 450-code palette
        # with only ~900 slots leaves a long tail of codes with zero Conditions,
        # and a code with zero Conditions is a question the demo still cannot
        # answer - which is the whole problem this dataset exists to fix. The
        # preference is a bias, not a rule, so repeat diagnoses still occur.
        guard = 0
        while len(chosen) < count and guard < count * 12:
            guard += 1
            area = weighted_choice(rng, band_areas)[0]
            code = rng.choice(palette[area])
            if code in used_codes:
                alternate = rng.choice(palette[area])
                if alternate not in used_codes:
                    code = alternate
            if code in seen_codes:
                continue
            # Reject a diagnosis this patient is too young to plausibly carry.
            # `guard` bounds the retries, so a band whose palette is mostly
            # adult-only degrades to fewer Conditions rather than looping.
            if not age_permits(index[code], max_age):
                continue
            chosen.append((code, area))
            seen_codes.add(code)

        for code, area in chosen:
            condition_seq += 1
            used_codes.add(code)
            concept = index[code]
            yield "Condition", condition_resource(
                f"{CONDITION_ID_PREFIX}{condition_seq:05d}",
                patient_id,
                concept,
                area,
                random_onset(rng, birth_year, min_age_for_concept(concept)),
                rng,
            )


def build_cluster_pools(
    index: dict[str, dict[str, Any]],
    palette: dict[str, list[str]],
) -> dict[str, dict[str, Any]]:
    """
    Resolve each cluster's anchor/partner roots to concrete leaf/mid-level codes.

    A cluster root such as 44054006 (T2DM) usually has children, so it would be
    filtered out of the palette; the cluster therefore draws from the root's own
    leaf/mid-level subtree. That keeps clusters inside the same answerable code
    set the rest of the dataset uses.
    """
    code_to_area: dict[str, str] = {}
    for area, codes in palette.items():
        for code in codes:
            code_to_area.setdefault(code, area)

    pools: dict[str, dict[str, Any]] = {}
    for name, spec in COMORBIDITY_CLUSTERS.items():
        anchor_root = spec["anchor"]
        if anchor_root not in index:
            raise SystemExit(
                f"comorbidity cluster {name!r} anchors on {anchor_root}, which is "
                f"absent from {RELEASE_ID}"
            )
        anchor_codes = descendants_of(index, anchor_root)
        anchor_area = next(
            (area for area, spec2 in AREAS.items() if spec2["root"] in index[anchor_root]["ancestors"]),
            "cardiovascular",
        )
        partner_codes: dict[str, list[str]] = {}
        partner_areas: dict[str, str] = {}
        for partner_root, _probability in spec["partners"]:
            if partner_root not in index:
                raise SystemExit(
                    f"comorbidity cluster {name!r} references partner {partner_root}, "
                    f"which is absent from {RELEASE_ID}"
                )
            partner_codes[partner_root] = descendants_of(index, partner_root)
            partner_areas[partner_root] = next(
                (
                    area
                    for area, spec2 in AREAS.items()
                    if spec2["root"] in index[partner_root]["ancestors"]
                    or spec2["root"] == partner_root
                ),
                "cardiovascular",
            )
        pools[name] = {
            "anchor": anchor_codes,
            "anchor_area": anchor_area,
            "partners": partner_codes,
            "partner_areas": partner_areas,
        }
    return pools


# --------------------------------------------------------------------------
# Streaming writers
# --------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate synthetic FHIR R5 Conditions spanning hdl-synthetic-2"
    )
    parser.add_argument("--release", type=Path, default=RELEASE_FILE)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--patients", type=int, default=DEFAULT_PATIENTS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--target-codes",
        type=int,
        default=460,
        help="approximate number of DISTINCT SNOMED codes the palette should cover",
    )
    args = parser.parse_args()

    if not args.release.exists():
        print(f"FAIL: release file not found: {args.release}")
        return 1

    index = load_release(args.release)
    print(f"release   : {args.release}")
    print(f"concepts  : {len(index)} active ({sum(1 for c in index.values() if selectable(c))} finding/disorder)")

    rng = random.Random(args.seed)
    pools = build_area_pools(index)
    palette = select_palette(pools, rng, args.target_codes)
    cluster_pools = build_cluster_pools(index, palette)

    palette_size = len({code for codes in palette.values() for code in codes})
    print(f"palette   : {palette_size} distinct codes across {len(AREAS)} clinical areas")

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    patients_path = out_dir / "patients.ndjson"
    conditions_path = out_dir / "conditions.ndjson"

    counts = {"Patient": 0, "Condition": 0}
    # Codes that actually reach a Condition. This is NOT the palette size: the
    # comorbidity clusters draw from their anchors' subtrees, which include
    # codes outside the per-area palette, so the realised count runs higher.
    # Reporting the palette size in the manifest would understate the dataset.
    emitted_codes: set[str] = set()
    # STREAMED: one json.dumps per resource, written and discarded. Never a list
    # of thousands of dicts and never one giant string - a previous attempt hit
    # MemoryError doing exactly that on this host.
    with patients_path.open("w", encoding="utf-8", newline="\n") as patient_handle, \
            conditions_path.open("w", encoding="utf-8", newline="\n") as condition_handle:
        for kind, resource in generate(index, palette, cluster_pools, args.patients, rng):
            handle = patient_handle if kind == "Patient" else condition_handle
            handle.write(json.dumps(resource, separators=(",", ":"), ensure_ascii=False))
            handle.write("\n")
            counts[kind] += 1
            if kind == "Condition":
                for coding in resource["code"]["coding"]:
                    emitted_codes.add(coding["code"])

    manifest = {
        "datasetId": DATASET_ID,
        "synthetic": True,
        "notice": (
            "SYNTHETIC demonstration data. Fabricated patients and fabricated "
            "diagnoses carrying codes from the synthetic hdl-synthetic-2 "
            "hierarchy. NOT real patient data, NOT SNOMED CT content, NOT for "
            "clinical use."
        ),
        "generator": "kehrnel/scripts/generate_synthetic_fhir_conditions.py",
        "verifier": "kehrnel/scripts/verify_synthetic_fhir_conditions.py",
        "terminologyRelease": RELEASE_ID,
        "fhirRelease": "R5",
        "seed": args.seed,
        "patients": counts["Patient"],
        "conditions": counts["Condition"],
        "distinctCodes": len(emitted_codes),
        "paletteCodes": palette_size,
        "onsetWindow": {"start": ONSET_START.isoformat(), "end": ONSET_END.isoformat()},
        "files": [
            {"file": "patients.ndjson", "resourceType": "Patient", "count": counts["Patient"]},
            {"file": "conditions.ndjson", "resourceType": "Condition", "count": counts["Condition"]},
        ],
        "loadOrder": ["patients.ndjson", "conditions.ndjson"],
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    print(f"wrote     : {patients_path}  ({counts['Patient']} Patient)")
    print(f"wrote     : {conditions_path}  ({counts['Condition']} Condition)")
    print(
        f"codes     : {len(emitted_codes)} DISTINCT SNOMED codes actually emitted "
        f"(palette {palette_size}; clusters add codes from anchor subtrees)"
    )
    print(f"wrote     : {out_dir / 'manifest.json'}")
    print("REMINDER: synthetic demonstration content. Not real patients. Not for clinical use.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
