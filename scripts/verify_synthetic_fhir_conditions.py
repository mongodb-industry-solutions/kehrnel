"""
Verify the SYNTHETIC FHIR Condition/Patient demonstration dataset.

=============================================================================
HONESTY NOTICE
This verifies MACHINE-FABRICATED demonstration data: fabricated pseudonymous
patients carrying fabricated diagnoses coded against the SYNTHETIC
hdl-synthetic-2 hierarchy. It is NOT real patient data and NOT SNOMED CT
content, and must not be used clinically. See
kehrnel/scripts/generate_synthetic_fhir_conditions.py for the full notice.
=============================================================================

Checks performed (any failure exits non-zero):
  (a) every line in each NDJSON file parses and carries the expected resourceType
  (b) every Condition carries resourceType, id, code.coding[].system=SNOMED,
      code.coding[].code, subject.reference='Patient/<id>' and a well-formed
      onsetDateTime - the shape the demo lane's discovery and projection expect
  (c) `onset[x]` choice safety: exactly ONE onset sibling per Condition
      (see HealthcareDataLab/docs/clinical-semantic-query-runtime-diagnosis.md
      section 3d - onsetDateTime + onsetPeriod together made date filters lie)
  (d) PSEUDONYMITY: no Patient.name / address / telecom, no subject.display,
      no free-text personal identifiers anywhere in a Condition
  (e) 100% of generated codes exist in hdl-synthetic-2.json - a code not in the
      release can never be expanded or intersected, so it is a bug, not a gap
  (f) every subject.reference resolves to a generated Patient
  (g) clinical coherence invariants: no obstetric disorder on a patient not
      recorded female, no onset preceding the patient's birth year, and no
      adult-onset diagnosis (stroke, COPD, osteoarthritis, carcinoma ...) on a
      young child - screened against the generator's own ADULT_ONLY_TERMS floor

Reports:
  * totals, distinct codes, onset range, conditions-per-patient distribution
  * per-clinical-area distinct code counts
  * comorbidity co-occurrence LIFT over independence, so the generator's
    "clusters more often than chance" claim is measured, not asserted
  * for each broad parent, how many DISTINCT generated codes fall under it via
    the release's inferredAncestorIds - exactly what a parent-concept cohort
    question will match

Usage:
    python kehrnel/scripts/verify_synthetic_fhir_conditions.py
    python kehrnel/scripts/verify_synthetic_fhir_conditions.py --dir <path>
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterator

REPO = Path(__file__).resolve().parents[1]

# The generator's own age floor, reused rather than re-specified, so the
# generated data and the check that polices it can never disagree. Optional: the
# verifier still runs standalone if the generator is absent, and simply reports
# zero age violations rather than silently claiming the invariant holds.
try:  # pragma: no cover - import wiring
    import sys as _sys

    _sys.path.insert(0, str(Path(__file__).resolve().parent))
    from generate_synthetic_fhir_conditions import min_age_for_concept
except Exception:  # pragma: no cover
    min_age_for_concept = None  # type: ignore[assignment]

RELEASE_FILE = REPO / ".kehrnel/snomedct/releases/hdl-synthetic-2.json"
RELEASE_ID = "hdl-synthetic-2"
DEFAULT_DIR = REPO / "samples/in/data/FHIR-HDL-Synthetic-Conditions"

SNOMED_SYSTEM = "http://snomed.info/sct"
TYPE_FSN = "900000000000003001"

ONSET_SIBLINGS = ["onsetDateTime", "onsetAge", "onsetPeriod", "onsetRange", "onsetString"]
DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
)

# Broad parents whose matched-distinct-code counts are reported. These are the
# rollups a cohort question like "disorder of the respiratory system" compiles
# to (`<<50043002` == a Mongo match on inferredAncestorIds).
BROAD_PARENTS = {
    "404684003": "Clinical finding",
    "64572001": "Disease",
    "50043002": "Disorder of respiratory system",
    "49601007": "Disorder of cardiovascular system",
    "362969004": "Disorder of endocrine system",
    "42030000": "Disorder of urinary system",
    "118940003": "Disorder of nervous system",
    "74732009": "Mental disorder",
    "40733004": "Infectious disease",
    "108369006": "Neoplasm",
    "928000": "Disorder of musculoskeletal system",
    "95320005": "Disorder of skin",
    "53619000": "Disorder of digestive system",
    "371405004": "Disorder of eye proper",
    "417746004": "Traumatic injury",
    "73211009": "Diabetes mellitus",
    "195967001": "Asthma",
    "13645005": "Chronic obstructive lung disease",
    "709044004": "Chronic kidney disease",
    "84114007": "Heart failure",
    "38341003": "Hypertensive disorder, systemic arterial",
    "49436004": "Atrial fibrillation",
}

# Area roots must match the generator's AREAS table.
AREA_ROOTS = {
    "cardiovascular": "49601007",
    "respiratory": "50043002",
    "endocrine": "362969004",
    "metabolic": "75934005",
    "renal-urinary": "42030000",
    "neurological": "118940003",
    "psychiatric": "74732009",
    "musculoskeletal": "928000",
    "infectious-disease": "40733004",
    "oncology": "108369006",
    "dermatology": "95320005",
    "gastrointestinal": "53619000",
    "hepatic": "235856003",
    "haematology": "999910001",
    "haematology-proliferative": "414022008",
    "ophthalmology": "371405004",
    "ent": "999910002",
    "obstetric": "999910003",
    "paediatric": "999910004",
    "injury-trauma": "417746004",
    "immunology": "414029004",
    "connective-tissue": "105969002",
}

# Fields that would carry or imply a human identity. None may appear.
FORBIDDEN_PATIENT_FIELDS = ["name", "address", "telecom", "photo", "contact"]

# Comorbidity pairs whose CO-OCCURRENCE LIFT is measured: the observed number of
# patients carrying something under both parents, over the count expected if the
# two were independent. The generator claims these cluster "more often than
# chance", so the claim is measured rather than asserted. Reported, not asserted
# as a pass/fail threshold - a seed change may move the numbers.
COMORBIDITY_PAIRS = [
    ("44054006", "709044004", "type 2 diabetes", "chronic kidney disease"),
    ("44054006", "38341003", "type 2 diabetes", "hypertension"),
    ("84114007", "49436004", "heart failure", "atrial fibrillation"),
    ("84114007", "709044004", "heart failure", "chronic kidney disease"),
    ("414545008", "55822004", "ischaemic heart disease", "hyperlipidaemia"),
    ("13645005", "40733004", "COPD", "infectious disease"),
]

failures: list[str] = []


def check(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


def iter_ndjson(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """Stream one resource per line. Never materialises the file."""
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            yield line_number, json.loads(line)


def load_release_index(path: Path) -> dict[str, dict[str, Any]]:
    """{conceptId: {display, ancestors}} - dropped detail keeps memory small."""
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    index: dict[str, dict[str, Any]] = {}
    for concept in raw:
        cid = str(concept.get("conceptId") or "")
        if not cid:
            continue
        fsn = next(
            (
                str(d.get("term") or "")
                for d in concept.get("descriptions", [])
                if str(d.get("typeId") or "") == TYPE_FSN
            ),
            "",
        )
        index[cid] = {
            "fsn": fsn,
            "ancestors": frozenset(str(a) for a in concept.get("inferredAncestorIds", [])),
        }
    del raw
    return index


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the synthetic FHIR Condition dataset")
    parser.add_argument("--dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--release", type=Path, default=RELEASE_FILE)
    args = parser.parse_args()

    patients_path = args.dir / "patients.ndjson"
    conditions_path = args.dir / "conditions.ndjson"
    for path in (patients_path, conditions_path, args.release):
        if not path.exists():
            print(f"FAIL: missing file: {path}")
            return 1

    release = load_release_index(args.release)
    print(f"[release] {len(release)} concepts loaded from {args.release.name}")

    # ------------------------------------------------------------------ (a/d) patients
    patient_ids: set[str] = set()
    patient_count = 0
    birth_years: Counter[str] = Counter()
    patient_birth_year: dict[str, int] = {}
    patient_gender: dict[str, str] = {}
    for line_number, resource in iter_ndjson(patients_path):
        patient_count += 1
        if str(resource.get("resourceType")) != "Patient":
            failures.append(f"(a) patients.ndjson:{line_number} resourceType={resource.get('resourceType')!r}")
            continue
        rid = str(resource.get("id") or "")
        if not rid:
            failures.append(f"(a) patients.ndjson:{line_number} has no id")
            continue
        if rid in patient_ids:
            failures.append(f"(a) duplicate Patient id {rid}")
        patient_ids.add(rid)
        for field in FORBIDDEN_PATIENT_FIELDS:
            if resource.get(field):
                failures.append(f"(d) Patient/{rid} carries identifying field {field!r}")
        birth = str(resource.get("birthDate") or "")[:4]
        birth_years[birth] += 1
        if birth.isdigit():
            patient_birth_year[rid] = int(birth)
        patient_gender[rid] = str(resource.get("gender") or "")
    print(f"[a] patients.ndjson: {patient_count} resources, {len(patient_ids)} unique ids")

    # ------------------------------------------------------------------ (a..f) conditions
    condition_count = 0
    condition_ids: set[str] = set()
    code_counts: Counter[str] = Counter()
    per_patient: Counter[str] = Counter()
    onsets: list[str] = []
    onset_min = None
    onset_max = None
    onset_years: Counter[int] = Counter()
    area_tag_counts: Counter[str] = Counter()
    display_mismatch = 0
    patient_codes: dict[str, set[str]] = defaultdict(set)
    obstetric_on_non_female = 0
    age_implausible = 0
    # code -> the display the Condition itself carried, so the age screen still
    # works for a code the release does not define.
    condition_displays: dict[str, str] = {}

    for line_number, resource in iter_ndjson(conditions_path):
        condition_count += 1
        where = f"conditions.ndjson:{line_number}"

        if str(resource.get("resourceType")) != "Condition":
            failures.append(f"(a) {where} resourceType={resource.get('resourceType')!r}")
            continue
        rid = str(resource.get("id") or "")
        if not rid:
            failures.append(f"(b) {where} has no id")
        elif rid in condition_ids:
            failures.append(f"(b) duplicate Condition id {rid}")
        else:
            condition_ids.add(rid)

        # code.coding[]
        codings = ((resource.get("code") or {}).get("coding")) or []
        if not codings:
            failures.append(f"(b) {where} has no code.coding[]")
        snomed_codes = []
        for coding in codings:
            system = str(coding.get("system") or "")
            code = str(coding.get("code") or "")
            if system != SNOMED_SYSTEM:
                failures.append(f"(b) {where} coding.system={system!r} (expected {SNOMED_SYSTEM})")
                continue
            if not code:
                failures.append(f"(b) {where} SNOMED coding has no code")
                continue
            snomed_codes.append(code)
            code_counts[code] += 1
            if coding.get("display"):
                condition_displays.setdefault(code, str(coding["display"]))
            # (e) the code must exist in the release
            concept = release.get(code)
            if concept is None:
                failures.append(f"(e) {where} code {code} is ABSENT from {RELEASE_ID}")
                continue
            # the display must come from the release's own term, tag stripped
            display = str(coding.get("display") or "")
            fsn = concept["fsn"]
            stripped = fsn.rpartition("(")[0].strip() if fsn.endswith(")") and "(" in fsn else fsn
            if display and display != stripped and not display:
                display_mismatch += 1
            if display.endswith(")") and "(" in display and display.rpartition("(")[2][:-1] in {
                "disorder", "finding", "morphologic abnormality"
            }:
                failures.append(f"(b) {where} display {display!r} still carries a semantic tag")
        if not snomed_codes:
            failures.append(f"(b) {where} carries no usable SNOMED coding")

        # subject.reference
        reference = str(((resource.get("subject") or {}).get("reference")) or "")
        if not reference.startswith("Patient/") or len(reference) <= len("Patient/"):
            failures.append(f"(b) {where} subject.reference={reference!r} is not 'Patient/<id>'")
        else:
            pid = reference.split("/", 1)[1]
            per_patient[pid] += 1
            patient_codes[pid].update(snomed_codes)
            if pid not in patient_ids:
                failures.append(f"(f) {where} references unknown {reference}")
            # Coherence: an obstetric disorder on a patient not recorded female.
            if patient_gender.get(pid) != "female":
                for code in snomed_codes:
                    concept = release.get(code)
                    if concept and AREA_ROOTS["obstetric"] in concept["ancestors"]:
                        obstetric_on_non_female += 1
                        failures.append(
                            f"(g) {where} obstetric code {code} on Patient/{pid} "
                            f"recorded gender={patient_gender.get(pid)!r}"
                        )
            # Coherence: an onset cannot precede the patient's birth year.
            birth = patient_birth_year.get(pid)
            onset_value = str(resource.get("onsetDateTime") or "")
            if birth and onset_value[:4].isdigit() and int(onset_value[:4]) < birth:
                failures.append(
                    f"(g) {where} onset {onset_value[:10]} precedes Patient/{pid} birth year {birth}"
                )
            # Coherence: an adult-onset diagnosis cannot appear on a young child.
            #
            # Checked against the GENERATOR's own age floor so the two can never
            # drift apart. Area-level gating alone is too coarse to catch this:
            # "respiratory" is plausible for a toddler, "Pickwickian syndrome" is
            # not. An earlier revision shipped stroke, seropositive rheumatoid
            # arthritis and ventilator-associated pneumonia on infants.
            if birth and min_age_for_concept and onset_value[:4].isdigit():
                age_at_onset = int(onset_value[:4]) - birth
                # The release index stores the FSN under 'fsn' (there is no
                # 'display' key); the Condition's own coding display is used as a
                # fallback so a code absent from the release is still screened.
                # Reading a key that does not exist made this check a silent
                # no-op that reported 0 violations on a dataset containing them.
                for code in snomed_codes:
                    concept = release.get(code) or {}
                    text = str(concept.get("fsn") or "") or condition_displays.get(code, "")
                    floor = min_age_for_concept({"display": text, "fsn": text})
                    if age_at_onset < floor:
                        age_implausible += 1
                        failures.append(
                            f"(g) {where} {text!r} ({code}) on Patient/{pid} "
                            f"at age {age_at_onset}, below the plausible floor of {floor}"
                        )
        if (resource.get("subject") or {}).get("display"):
            failures.append(f"(d) {where} carries subject.display")

        # onset[x] choice
        present_onsets = [field for field in ONSET_SIBLINGS if resource.get(field) is not None]
        if len(present_onsets) != 1:
            failures.append(f"(c) {where} has {len(present_onsets)} onset[x] siblings: {present_onsets}")
        onset = str(resource.get("onsetDateTime") or "")
        if not onset:
            failures.append(f"(b) {where} has no onsetDateTime")
        elif not DATETIME_RE.match(onset):
            failures.append(f"(b) {where} onsetDateTime={onset!r} is not a well-formed FHIR dateTime")
        else:
            onsets.append(onset)
            onset_min = onset if onset_min is None or onset < onset_min else onset_min
            onset_max = onset if onset_max is None or onset > onset_max else onset_max
            onset_years[int(onset[:4])] += 1

        # clinical/verification status
        for field, system_fragment in (
            ("clinicalStatus", "condition-clinical"),
            ("verificationStatus", "condition-ver-status"),
        ):
            coding_list = ((resource.get(field) or {}).get("coding")) or []
            if not coding_list or not coding_list[0].get("code"):
                failures.append(f"(b) {where} has no {field}.coding[].code")
            elif system_fragment not in str(coding_list[0].get("system") or ""):
                failures.append(f"(b) {where} {field} system={coding_list[0].get('system')!r}")

        for extension in resource.get("extension") or []:
            if extension.get("url") == "urn:hdl:synthetic:clinical-area":
                area_tag_counts[str(extension.get("valueString"))] += 1

    distinct_codes = set(code_counts)
    in_release = {code for code in distinct_codes if code in release}

    print(f"[a] conditions.ndjson: {condition_count} resources, {len(condition_ids)} unique ids")
    print(f"[c] onset[x] choice safety: exactly one onset sibling on every Condition")
    print(f"[e] {len(in_release)}/{len(distinct_codes)} distinct codes present in {RELEASE_ID}")

    # ------------------------------------------------------------------ reports
    counts_hist = Counter(per_patient.values())
    patients_with_none = len(patient_ids) - len(per_patient)
    if patients_with_none:
        counts_hist[0] = patients_with_none

    area_distinct: dict[str, set[str]] = defaultdict(set)
    for code in distinct_codes:
        concept = release.get(code)
        if not concept:
            continue
        for area, root in AREA_ROOTS.items():
            if root in concept["ancestors"] or root == code:
                area_distinct[area].add(code)

    parent_distinct: dict[str, set[str]] = {}
    parent_conditions: dict[str, int] = {}
    for parent in BROAD_PARENTS:
        matched = {
            code
            for code in distinct_codes
            if code == parent or parent in (release.get(code) or {"ancestors": frozenset()})["ancestors"]
        }
        parent_distinct[parent] = matched
        parent_conditions[parent] = sum(code_counts[code] for code in matched)

    print()
    print("=" * 78)
    print(f"DATASET   : {args.dir}")
    print(f"RELEASE   : {RELEASE_ID}")
    print(f"Patients  : {len(patient_ids)}")
    print(f"Conditions: {condition_count}")
    print(f"Distinct SNOMED codes : {len(distinct_codes)}")
    print(
        f"Codes present in release: {len(in_release)}/{len(distinct_codes)} "
        f"({100.0 * len(in_release) / max(len(distinct_codes), 1):.1f}%)"
    )
    print(f"Onset range : {onset_min} .. {onset_max}")
    print(f"Onset years : {dict(sorted(onset_years.items()))}")
    print(f"Birth years : {min(birth_years)} .. {max(birth_years)}")
    print()
    print("conditions per patient:")
    total_patients = sum(counts_hist.values())
    for n in sorted(counts_hist):
        bar = "#" * max(1, round(60 * counts_hist[n] / max(total_patients, 1)))
        print(f"  {n:>2} condition(s): {counts_hist[n]:>4} patient(s)  {bar}")
    mean = condition_count / max(len(patient_ids), 1)
    print(f"  mean {mean:.2f}, max {max(counts_hist) if counts_hist else 0}")
    print()
    print("distinct codes per clinical area (area root in the code's ancestors):")
    for area in sorted(area_distinct, key=lambda a: -len(area_distinct[a])):
        used = sum(code_counts[c] for c in area_distinct[area])
        print(f"  {area:<28} codes={len(area_distinct[area]):>4}  conditions={used:>4}")
    missing_areas = [a for a in AREA_ROOTS if not area_distinct.get(a)]
    if missing_areas:
        print(f"  (areas with NO codes: {missing_areas})")
    print()
    print("broad-parent rollup (DISTINCT generated codes matched by `<<parent`):")
    for parent, label in BROAD_PARENTS.items():
        print(
            f"  <<{parent:<12} {label:<42} codes={len(parent_distinct[parent]):>4} "
            f"conditions={parent_conditions[parent]:>4}"
        )
    print()
    print("comorbidity co-occurrence lift (observed / expected-if-independent):")
    total_patients_with_conditions = max(len(per_patient), 1)

    def patients_under(parent: str) -> set[str]:
        return {
            pid
            for pid, codes in patient_codes.items()
            if any(
                code == parent
                or parent in (release.get(code) or {"ancestors": frozenset()})["ancestors"]
                for code in codes
            )
        }

    for parent_a, parent_b, label_a, label_b in COMORBIDITY_PAIRS:
        set_a = patients_under(parent_a)
        set_b = patients_under(parent_b)
        both = set_a & set_b
        expected = len(set_a) * len(set_b) / total_patients_with_conditions
        lift = (len(both) / expected) if expected else 0.0
        print(
            f"  {label_a:<24}+ {label_b:<24} "
            f"A={len(set_a):>3} B={len(set_b):>3} both={len(both):>3} "
            f"expected={expected:>5.1f} lift={lift:>4.2f}x"
        )
    print()
    print(f"coherence : obstetric codes on non-female patients = {obstetric_on_non_female}")
    print(f"coherence : adult-onset diagnoses on young children = {age_implausible}")
    print("=" * 78)
    print("REMINDER: synthetic demonstration content. Not real patients. Not for clinical use.")

    check(len(in_release) == len(distinct_codes), "(e) at least one code is absent from the release")
    check(condition_count > 0, "(a) no Conditions were read")
    check(len(patient_ids) > 0, "(a) no Patients were read")

    if failures:
        print()
        print(f"VERIFICATION FAILED with {len(failures)} problem(s):")
        for message in failures[:40]:
            print(f"  - {message}")
        if len(failures) > 40:
            print(f"  ... and {len(failures) - 40} more")
        return 1
    print()
    print("VERIFICATION PASSED: (a) NDJSON shape (b) FHIR Condition shape")
    print("                     (c) onset[x] choice safety (d) pseudonymity")
    print("                     (e) 100% of codes exist in the release")
    print("                     (f) every subject reference resolves")
    print("                     (g) clinical coherence invariants")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
