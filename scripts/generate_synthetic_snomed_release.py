"""
Generate a SYNTHETIC, SNOMED-CT-SHAPED demonstration release for Kehrnel ingest.

=============================================================================
HONESTY NOTICE - READ BEFORE USING THIS FILE OR ITS OUTPUT
=============================================================================
This script does NOT produce a SNOMED CT release.

  * The output file (hdl-synthetic-2.json) is a HAND-AUTHORED, SYNTHETIC
    hierarchy that merely imitates the *document shape* of an official
    SNOMED CT JSON distribution so that Kehrnel's ingest, closure and search
    pipelines can be exercised at realistic breadth.
  * Some concept identifiers are REAL, widely published SNOMED CT identifiers
    (e.g. 195967001 Asthma, 73211009 Diabetes mellitus). They appear here only
    as *references* for structural fidelity - so that the demo's codes line up
    with the real world where that is safe to assert. Every other identifier is
    openly SYNTHETIC and is minted in the reserved-looking 9999xxxxx namespace
    so it can never be mistaken for a real SNOMED code.
  * The PARENT/CHILD EDGES, the transitive closure, the synonyms and the
    release content as a whole are authored by hand. They are NOT the SNOMED CT
    inferred relationship set and will not agree with it in detail.
  * NO bulk content has been copied from any SNOMED CT distribution. The
    operator of this repository holds NO SNOMED CT licence.
  * This release is NOT a substitute for a licensed SNOMED CT release and MUST
    NOT be used clinically, for decision support, for coding, for billing, or
    for any purpose where terminological correctness matters.

Use it for: pipeline breadth, ECL/closure behaviour, search ranking, demos.
Do not use it for: anything involving a patient.
=============================================================================

Output contract (verified against
kehrnel/src/kehrnel/engine/domains/snomedct/model.py,
sidecar.py and stream_json.py):

  Top-level JSON array of concept objects, each with conceptId, active,
  effectiveTime, moduleId, definitionStatusId, descriptions[], relationships[],
  concreteRelationships[], inferredParentIds[], inferredAncestorIds[],
  inferredChildIds[], memberOfRefsetIds[].

`normalize_concept` does NOT compute transitive closure - it only stringifies
the lists it is given. ECL `<<X` compiles to a Mongo match on
`inferredAncestorIds: X`. Therefore this generator computes the FULL transitive
closure programmatically from the authored parent edges and asserts
parent/child reciprocity before writing. Ancestor lists are never hand-typed.

Usage:
    python kehrnel/scripts/generate_synthetic_snomed_release.py
    python kehrnel/scripts/generate_synthetic_snomed_release.py --out /tmp/x.json
"""

from __future__ import annotations

import argparse
import json
from collections import deque
from pathlib import Path
from typing import Iterable

# --------------------------------------------------------------------------
# Release constants
# --------------------------------------------------------------------------

RELEASE_ID = "hdl-synthetic-2"
RELEASE_LABEL = (
    "Synthetic SNOMED-shaped demonstration hierarchy (NOT an official SNOMED CT release)"
)
EFFECTIVE_TIME = "20260601"
MODULE_ID = "900000000000207008"
PRIMITIVE = "900000000000074008"
DEFINED = "900000000000073002"
TYPE_FSN = "900000000000003001"
TYPE_SYNONYM = "900000000000013009"
CASE_SIGNIFICANCE = "900000000000448009"
LANG_REFSET = "900000000000509007"
ACCEPT_PREFERRED = "900000000000548007"
ACCEPT_ACCEPTABLE = "900000000000549004"
IS_A = "116680003"
CHARACTERISTIC_INFERRED = "900000000000011006"
MODIFIER_SOME = "900000000000451002"

DEFAULT_OUT = Path(".kehrnel/snomedct/releases/hdl-synthetic-2.json")

# SYN is the explicit "I am NOT confident this term's real SNOMED code is known
# to me, so mint an openly-fake id instead of guessing" sentinel. Guessing a
# plausible-but-wrong real code would be far worse than an obviously fake one,
# so every uncertain term below is authored as SYN.
SYN = None

# Synthetic identifier namespace. Anything minted here is openly fake.
SYNTHETIC_PREFIX = "9999"
_synthetic_counter = [10000]


def mint_synthetic_id() -> str:
    """Mint an obviously-synthetic concept id (9999xxxxx)."""
    _synthetic_counter[0] += 1
    return f"{SYNTHETIC_PREFIX}{_synthetic_counter[0]}"


# --------------------------------------------------------------------------
# Authoring model
# --------------------------------------------------------------------------
# A node is authored as:
#   (concept_id_or_None, fsn, [synonyms], [parent_ids])
# concept_id None  -> mint a synthetic 9999xxxxx id.
# concept_id str   -> a REAL SNOMED identifier used as a reference. Only used
#                     where the author is confident of the code/term pairing.
#
# `NODES` accumulates every authored node in declaration order.

NODES: list[dict] = []
_seen_ids: set[str] = set()
REAL_IDS: set[str] = set()
SYNTH_IDS: set[str] = set()


def node(
    concept_id: str | None,
    fsn: str,
    synonyms: Iterable[str] = (),
    parents: Iterable[str] = (),
    *,
    defined: bool = False,
) -> str:
    """Author one concept. Returns its concept id."""
    if concept_id is None:
        cid = mint_synthetic_id()
        SYNTH_IDS.add(cid)
    else:
        cid = str(concept_id)
        REAL_IDS.add(cid)
    if cid in _seen_ids:
        raise ValueError(f"duplicate concept id authored: {cid} ({fsn})")
    _seen_ids.add(cid)
    NODES.append(
        {
            "conceptId": cid,
            "fsn": fsn,
            "synonyms": [s for s in synonyms if s],
            "parents": [str(p) for p in parents],
            "defined": defined,
        }
    )
    return cid


def family(
    parent: str,
    tag: str,
    children: Iterable[tuple[str | None, str, tuple[str, ...]]],
) -> list[str]:
    """Author a flat group of children under `parent`, appending `tag` to FSNs."""
    out = []
    for cid, label, syns in children:
        out.append(node(cid, f"{label} ({tag})", syns, [parent]))
    return out


# ==========================================================================
# TOP ROOTS
# ==========================================================================
# DEFAULT_TOP_ROOT_IDS in sidecar.py drives areaTags/topRoots. Clinical
# findings must carry 404684003 in their closure; we additionally populate
# procedure, product, observable, body structure, situation and event roots so
# that search breadth is not findings-only.

CLINICAL_FINDING = node("404684003", "Clinical finding (finding)", ["Clinical finding"])
BODY_STRUCTURE = node("123037004", "Body structure (body structure)", ["Body structure"])
PROCEDURE = node("71388002", "Procedure (procedure)", ["Procedure"])
PRODUCT = node("373873005", "Pharmaceutical / biologic product (product)", ["Product"])
EVENT = node("272379006", "Event (event)", ["Event"])
SITUATION = node(
    "243796009",
    "Situation with explicit context (situation)",
    ["Situation with explicit context"],
)
OBSERVABLE = node("78621006", "Observable entity (observable entity)", ["Observable entity"])

DISEASE = node("64572001", "Disease (disorder)", ["Disease", "Disorder"], [CLINICAL_FINDING])
FINDING_BY_SITE = node(
    "118234003", "Finding by site (finding)", ["Finding by site"], [CLINICAL_FINDING]
)

# ==========================================================================
# BODY-SYSTEM DISORDER PARENTS (level 3) - real, widely published ids
# ==========================================================================

CARDIOVASCULAR = node(
    "49601007",
    "Disorder of cardiovascular system (disorder)",
    ["Cardiovascular disorder", "Cardiovascular disease", "CVD"],
    [DISEASE],
)
RESPIRATORY = node(
    "50043002",
    "Disorder of respiratory system (disorder)",
    ["Respiratory disorder", "Respiratory disease"],
    [DISEASE],
)
ENDOCRINE = node(
    "362969004",
    "Disorder of endocrine system (disorder)",
    ["Endocrine disorder", "Endocrine disease"],
    [DISEASE],
)
METABOLIC = node(
    "75934005",
    "Metabolic disease (disorder)",
    ["Metabolic disorder", "Metabolic disease"],
    [DISEASE],
)
URINARY = node(
    "42030000",
    "Disorder of urinary system (disorder)",
    ["Urinary tract disorder", "Renal and urinary disorder"],
    [DISEASE],
)
NERVOUS = node(
    "118940003",
    "Disorder of nervous system (disorder)",
    ["Neurological disorder", "Nervous system disease"],
    [DISEASE],
)
MENTAL = node(
    "74732009",
    "Mental disorder (disorder)",
    ["Mental disorder", "Psychiatric disorder", "Mental illness"],
    [DISEASE],
)
MUSCULOSKELETAL = node(
    "928000",
    "Disorder of musculoskeletal system (disorder)",
    ["Musculoskeletal disorder", "Musculoskeletal disease"],
    [DISEASE],
)
INFECTIOUS = node(
    "40733004",
    "Infectious disease (disorder)",
    ["Infection", "Infectious disease"],
    [DISEASE],
)
NEOPLASM = node(
    "108369006",
    "Neoplasm (morphologic abnormality)",
    ["Neoplasm", "Tumor", "Tumour"],
    [DISEASE],
)
SKIN = node(
    "95320005",
    "Disorder of skin (disorder)",
    ["Skin disorder", "Dermatological disorder"],
    [DISEASE],
)
DIGESTIVE = node(
    "53619000",
    "Disorder of digestive system (disorder)",
    ["Digestive system disorder", "Gastrointestinal disorder", "GI disorder"],
    [DISEASE],
)
HAEMATOLOGIC = node(
    "414022008",
    "Disorder of hematopoietic cell proliferation (disorder)",
    ["Hematopoietic proliferation disorder"],
    [DISEASE],
)
BLOOD = node(
    SYN,
    "Disorder of hematological system (disorder)",
    ["Haematological disorder", "Blood disorder", "Hematologic disorder"],
    [DISEASE],
)
EYE = node(
    "371405004",
    "Disorder of eye proper (disorder)",
    ["Eye disorder", "Ophthalmic disorder"],
    [DISEASE],
)
EAR_NOSE_THROAT = node(
    None,
    "Disorder of ear, nose and throat (disorder)",
    ["ENT disorder", "Ear nose and throat disorder"],
    [DISEASE],
)
OBSTETRIC = node(
    SYN,
    "Disorder of pregnancy, childbirth and the puerperium (disorder)",
    ["Obstetric disorder", "Pregnancy-related disorder"],
    [DISEASE],
)
PAEDIATRIC = node(
    None,
    "Disorder of the neonatal and paediatric period (disorder)",
    ["Paediatric disorder", "Pediatric disorder", "Neonatal disorder"],
    [DISEASE],
)
INJURY = node(
    "417746004",
    "Traumatic injury (disorder)",
    ["Injury", "Trauma", "Traumatic injury"],
    [CLINICAL_FINDING],
)
IMMUNE = node(
    "414029004",
    "Disorder of immune function (disorder)",
    ["Immune disorder", "Immunological disorder"],
    [DISEASE],
)
CONNECTIVE = node(
    "105969002",
    "Disorder of connective tissue (disorder)",
    ["Connective tissue disorder"],
    [DISEASE],
)
HEPATIC = node(
    "235856003",
    "Disorder of liver (disorder)",
    ["Liver disease", "Hepatic disorder", "Hepatopathy"],
    [DIGESTIVE],
)

BODY_SYSTEM_AREAS: dict[str, str] = {}


def register_area(concept_id: str, label: str) -> None:
    BODY_SYSTEM_AREAS[concept_id] = label


for _cid, _label in [
    (CARDIOVASCULAR, "cardiovascular"),
    (RESPIRATORY, "respiratory"),
    (ENDOCRINE, "endocrine"),
    (METABOLIC, "metabolic"),
    (URINARY, "renal-urinary"),
    (NERVOUS, "neurological"),
    (MENTAL, "psychiatric"),
    (MUSCULOSKELETAL, "musculoskeletal"),
    (INFECTIOUS, "infectious-disease"),
    (NEOPLASM, "oncology"),
    (SKIN, "dermatology"),
    (DIGESTIVE, "gastrointestinal"),
    (HEPATIC, "hepatic"),
    (BLOOD, "haematology"),
    (HAEMATOLOGIC, "haematology-proliferative"),
    (EYE, "ophthalmology"),
    (EAR_NOSE_THROAT, "ent"),
    (OBSTETRIC, "obstetric"),
    (PAEDIATRIC, "paediatric"),
    (INJURY, "injury-trauma"),
    (IMMUNE, "immunology"),
    (CONNECTIVE, "connective-tissue"),
]:
    register_area(_cid, _label)


# ==========================================================================
# CARDIOVASCULAR  (level 4 organ parents -> level 5/6 disorders & subtypes)
# ==========================================================================

HEART_DISEASE = node(
    "56265001",
    "Heart disease (disorder)",
    ["Heart disease", "Cardiac disease"],
    [CARDIOVASCULAR],
)
VASCULAR_DISEASE = node(
    "27550009",
    "Disorder of blood vessel (disorder)",
    ["Vascular disease", "Blood vessel disorder"],
    [CARDIOVASCULAR],
)

ISCHAEMIC_HEART = node(
    "414545008",
    "Ischemic heart disease (disorder)",
    ["Ischaemic heart disease", "IHD", "Coronary artery disease", "CAD"],
    [HEART_DISEASE],
)
MYOCARDIAL_INFARCTION = node(
    "22298006",
    "Myocardial infarction (disorder)",
    ["Myocardial infarction", "MI", "Heart attack"],
    [ISCHAEMIC_HEART],
)
family(
    MYOCARDIAL_INFARCTION,
    "disorder",
    [
        ("401303003", "Acute ST segment elevation myocardial infarction", ("STEMI", "ST elevation MI")),
        ("401314000", "Acute non-ST segment elevation myocardial infarction", ("NSTEMI", "Non-ST elevation MI")),
        ("73795002", "Old myocardial infarction", ("Healed myocardial infarction",)),
        (None, "Anterior wall myocardial infarction", ("Anterior MI",)),
        (None, "Inferior wall myocardial infarction", ("Inferior MI",)),
        (None, "Lateral wall myocardial infarction", ("Lateral MI",)),
        (None, "Posterior wall myocardial infarction", ("Posterior MI",)),
        (None, "Right ventricular myocardial infarction", ("RV infarction",)),
        (None, "Periprocedural myocardial infarction", ("Type 4a MI",)),
        (None, "Myocardial infarction due to demand ischaemia", ("Type 2 MI",)),
    ],
)
ANGINA = node(
    "194828000",
    "Angina (disorder)",
    ["Angina", "Angina pectoris"],
    [ISCHAEMIC_HEART],
)
family(
    ANGINA,
    "disorder",
    [
        ("59021001", "Angina decubitus", ("Decubitus angina",)),
        ("4557003", "Preinfarction syndrome", ("Unstable angina", "UA")),
        (None, "Stable angina pectoris", ("Stable angina", "Exertional angina")),
        (None, "Vasospastic angina", ("Prinzmetal angina", "Variant angina")),
        (None, "Microvascular angina", ("Cardiac syndrome X",)),
        (None, "Refractory angina pectoris", ("Refractory angina",)),
    ],
)
family(
    ISCHAEMIC_HEART,
    "disorder",
    [
        (None, "Chronic coronary syndrome", ("CCS", "Chronic ischaemic heart disease")),
        (None, "Silent myocardial ischaemia", ("Silent ischaemia",)),
        (None, "Coronary artery spasm", ("Coronary vasospasm",)),
        (None, "Coronary microvascular dysfunction", ("CMD",)),
        (None, "Ischaemic cardiomyopathy", ("Ischemic cardiomyopathy",)),
    ],
)

HEART_FAILURE = node(
    "84114007",
    "Heart failure (disorder)",
    ["Heart failure", "HF", "Cardiac failure"],
    [HEART_DISEASE],
)
family(
    HEART_FAILURE,
    "disorder",
    [
        ("42343007", "Congestive heart failure", ("CCF", "Congestive cardiac failure")),
        ("56675007", "Acute heart failure", ("Acute cardiac failure",)),
        ("48447003", "Chronic heart failure", ("Chronic cardiac failure",)),
        (None, "Heart failure with reduced ejection fraction", ("HFrEF", "Systolic heart failure")),
        (None, "Heart failure with preserved ejection fraction", ("HFpEF", "Diastolic heart failure")),
        (None, "Heart failure with mildly reduced ejection fraction", ("HFmrEF",)),
        (None, "Right ventricular failure", ("Right heart failure", "RV failure")),
        (None, "Decompensated heart failure", ("Acute decompensated heart failure", "ADHF")),
        (None, "Refractory heart failure", ("End-stage heart failure",)),
        (None, "High output cardiac failure", ("High output heart failure",)),
    ],
)

ARRHYTHMIA = node(
    "698247007",
    "Cardiac arrhythmia (disorder)",
    ["Cardiac arrhythmia", "Arrhythmia", "Dysrhythmia"],
    [HEART_DISEASE],
)
ATRIAL_FIB = node(
    "49436004",
    "Atrial fibrillation (disorder)",
    ["Atrial fibrillation", "AF", "AFib"],
    [ARRHYTHMIA],
)
family(
    ATRIAL_FIB,
    "disorder",
    [
        (None, "Paroxysmal atrial fibrillation", ("Paroxysmal AF",)),
        (None, "Persistent atrial fibrillation", ("Persistent AF",)),
        (None, "Permanent atrial fibrillation", ("Permanent AF",)),
        (None, "Valvular atrial fibrillation", ("Valvular AF",)),
        (None, "Non-valvular atrial fibrillation", ("Non-valvular AF", "NVAF")),
        (None, "Rapid atrial fibrillation", ("AF with rapid ventricular response", "AF with RVR")),
    ],
)
family(
    ARRHYTHMIA,
    "disorder",
    [
        ("5370000", "Atrial flutter", ("Atrial flutter",)),
        ("6456007", "Supraventricular tachycardia", ("SVT",)),
        ("25569003", "Ventricular tachycardia", ("VT",)),
        ("71908006", "Ventricular fibrillation", ("VF", "V-fib")),
        ("49710005", "Sinus bradycardia", ("Sinus bradycardia",)),
        ("11092001", "Sinus tachycardia", ("Sinus tachycardia",)),
        ("27885002", "Complete atrioventricular block", ("Complete heart block", "Third degree AV block")),
        ("270492004", "First degree atrioventricular block", ("First degree AV block", "1st degree AV block")),
        ("195042002", "Second degree atrioventricular block", ("Second degree AV block", "2nd degree AV block")),
        ("59118001", "Right bundle branch block", ("RBBB",)),
        ("63467002", "Left bundle branch block", ("LBBB",)),
        ("36083008", "Sick sinus syndrome", ("Sinus node dysfunction", "SSS")),
        ("74390002", "Wolff-Parkinson-White pattern", ("WPW", "Wolff-Parkinson-White syndrome")),
        (None, "Long QT syndrome", ("LQTS",)),
        (None, "Brugada syndrome", ("Brugada",)),
        (None, "Premature ventricular contraction", ("PVC", "Ventricular ectopic beat")),
        (None, "Premature atrial contraction", ("PAC", "Atrial ectopic beat")),
        (None, "Junctional rhythm", ("Nodal rhythm",)),
        (None, "Atrioventricular nodal re-entrant tachycardia", ("AVNRT",)),
        (None, "Atrioventricular re-entrant tachycardia", ("AVRT",)),
    ],
)

VALVE_DISEASE = node(
    "368009",
    "Heart valve disorder (disorder)",
    ["Valvular heart disease", "Heart valve disease", "VHD"],
    [HEART_DISEASE],
)
family(
    VALVE_DISEASE,
    "disorder",
    [
        ("60573004", "Aortic valve stenosis", ("Aortic stenosis", "AS")),
        ("60234000", "Aortic valve regurgitation", ("Aortic regurgitation", "AR")),
        ("79619009", "Mitral valve stenosis", ("Mitral stenosis", "MS")),
        ("48724000", "Mitral valve regurgitation", ("Mitral regurgitation", "MR")),
        ("409712001", "Mitral valve prolapse", ("MVP", "Floppy mitral valve")),
        ("111287006", "Tricuspid valve regurgitation", ("Tricuspid regurgitation", "TR")),
        (None, "Tricuspid valve stenosis", ("Tricuspid stenosis", "TS")),
        (None, "Pulmonary valve stenosis", ("Pulmonic stenosis", "PS")),
        (None, "Pulmonary valve regurgitation", ("Pulmonic regurgitation", "PR")),
        (None, "Prosthetic heart valve dysfunction", ("Prosthetic valve failure",)),
        (None, "Bicuspid aortic valve", ("BAV",)),
        (None, "Rheumatic mitral valve disease", ("Rheumatic mitral disease",)),
    ],
)

CARDIOMYOPATHY = node(
    "85898001",
    "Cardiomyopathy (disorder)",
    ["Cardiomyopathy", "Myocardial disease"],
    [HEART_DISEASE],
)
family(
    CARDIOMYOPATHY,
    "disorder",
    [
        ("399020009", "Dilated cardiomyopathy secondary to ischemia", ("Ischaemic dilated cardiomyopathy",)),
        ("195021004", "Hypertrophic obstructive cardiomyopathy", ("HOCM",)),
        (None, "Dilated cardiomyopathy", ("DCM",)),
        (None, "Hypertrophic cardiomyopathy", ("HCM",)),
        (None, "Restrictive cardiomyopathy", ("RCM",)),
        (None, "Arrhythmogenic right ventricular cardiomyopathy", ("ARVC",)),
        (None, "Peripartum cardiomyopathy", ("PPCM",)),
        (None, "Takotsubo cardiomyopathy", ("Stress cardiomyopathy", "Broken heart syndrome")),
        (None, "Alcoholic cardiomyopathy", ("Alcohol-induced cardiomyopathy",)),
        (None, "Cardiac amyloidosis", ("Amyloid cardiomyopathy",)),
    ],
)

PERICARDIAL = node(
    "3238004",
    "Pericarditis (disorder)",
    ["Pericarditis",],
    [HEART_DISEASE],
)
family(
    PERICARDIAL,
    "disorder",
    [
        (None, "Acute pericarditis", ("Acute pericarditis",)),
        (None, "Constrictive pericarditis", ("Constrictive pericarditis",)),
        (None, "Recurrent pericarditis", ("Relapsing pericarditis",)),
        (None, "Uraemic pericarditis", ("Uremic pericarditis",)),
    ],
)
family(
    HEART_DISEASE,
    "disorder",
    [
        ("56819008", "Endocarditis", ("Endocarditis",)),
        ("50920009", "Myocarditis", ("Myocarditis",)),
        ("13213009", "Congenital heart disease", ("CHD", "Congenital cardiac anomaly")),
        (None, "Cardiac tamponade", ("Tamponade",)),
        (None, "Pericardial effusion", ("Pericardial fluid collection",)),
        (None, "Infective endocarditis", ("IE", "Bacterial endocarditis")),
        (None, "Left ventricular hypertrophy", ("LVH",)),
        (None, "Cardiac arrest", ("Cardiopulmonary arrest", "Asystole")),
    ],
)

HYPERTENSION = node(
    "38341003",
    "Hypertensive disorder, systemic arterial (disorder)",
    ["Hypertension", "High blood pressure", "HTN"],
    [VASCULAR_DISEASE],
)
family(
    HYPERTENSION,
    "disorder",
    [
        ("59621000", "Essential hypertension", ("Primary hypertension", "Idiopathic hypertension")),
        ("31992008", "Secondary hypertension", ("Secondary hypertension",)),
        ("48552006", "Malignant hypertension", ("Malignant hypertension",)),
        ("70272006", "Renovascular hypertension", ("Renovascular hypertension",)),
        (None, "Resistant hypertension", ("Treatment-resistant hypertension",)),
        (None, "White coat hypertension", ("White coat syndrome",)),
        (None, "Isolated systolic hypertension", ("ISH",)),
        (None, "Hypertensive emergency", ("Hypertensive crisis",)),
        (None, "Hypertensive heart disease", ("Hypertensive cardiomyopathy",)),
        (None, "Hypertensive nephropathy", ("Hypertensive kidney disease",)),
    ],
)
PERIPHERAL_ARTERIAL = node(
    "399957001",
    "Peripheral arterial disease (disorder)",
    ["Peripheral arterial disease", "PAD", "Peripheral vascular disease"],
    [VASCULAR_DISEASE],
)
family(
    PERIPHERAL_ARTERIAL,
    "disorder",
    [
        ("63491006", "Intermittent claudication", ("Claudication",)),
        (None, "Critical limb ischaemia", ("CLI", "Chronic limb threatening ischaemia")),
        (None, "Acute limb ischaemia", ("ALI",)),
        (None, "Aortoiliac occlusive disease", ("Leriche syndrome",)),
    ],
)
family(
    VASCULAR_DISEASE,
    "disorder",
    [
        ("128053003", "Deep venous thrombosis", ("DVT", "Deep vein thrombosis")),
        ("59282003", "Pulmonary embolism", ("PE", "Pulmonary thromboembolism")),
        ("128609009", "Aortic aneurysm", ("Aortic aneurysm",)),
        ("433068007", "Abdominal aortic aneurysm", ("AAA",)),
        ("308113006", "Aortic dissection", ("Dissecting aortic aneurysm",)),
        ("128403000", "Varicose veins of lower extremity", ("Varicose veins",)),
        ("55822004", "Hyperlipidemia", ("Hyperlipidaemia", "Dyslipidaemia")),
        ("38716007", "Atherosclerosis", ("Atherosclerotic disease", "Arteriosclerosis")),
        ("64156001", "Thrombophlebitis", ("Thrombophlebitis",)),
        ("195295006", "Raynaud's phenomenon", ("Raynaud phenomenon", "Raynaud's")),
        (None, "Chronic venous insufficiency", ("CVI", "Venous insufficiency")),
        (None, "Superficial vein thrombosis", ("SVT of leg",)),
        (None, "Thoracic aortic aneurysm", ("TAA",)),
        (None, "Giant cell arteritis", ("Temporal arteritis", "GCA")),
        (None, "Takayasu arteritis", ("Pulseless disease",)),
        (None, "Lymphoedema", ("Lymphedema",)),
        (None, "Arteriovenous malformation", ("AVM",)),
        (None, "Venous leg ulcer", ("Venous ulcer",)),
        (None, "Pulmonary arterial hypertension", ("PAH",)),
    ],
)

# ==========================================================================
# RESPIRATORY
# ==========================================================================

LOWER_RESP = node(
    "79688008",
    "Respiratory tract infection (disorder)",
    ["Respiratory tract infection", "RTI"],
    [RESPIRATORY, INFECTIOUS],
)
LUNG_DISEASE = node(
    "19829001",
    "Disorder of lung (disorder)",
    ["Lung disease", "Pulmonary disorder"],
    [RESPIRATORY],
)
AIRWAY_DISEASE = node(
    SYN,
    "Obstructive airway disease (disorder)",
    ["Obstructive airways disease", "Airflow obstruction"],
    [LUNG_DISEASE],
)

ASTHMA = node(
    "195967001",
    "Asthma (disorder)",
    ["Asthma", "Bronchial asthma"],
    [AIRWAY_DISEASE],
)
family(
    ASTHMA,
    "disorder",
    [
        ("389145006", "Allergic asthma", ("Atopic asthma", "Extrinsic asthma")),
        ("266361008", "Non-allergic asthma", ("Intrinsic asthma",)),
        ("57607007", "Occupational asthma", ("Work-related asthma",)),
        ("708093000", "Severe asthma", ("Severe persistent asthma",)),
        ("708038006", "Acute exacerbation of asthma", ("Asthma exacerbation", "Asthma attack")),
        (None, "Mild intermittent asthma", ("Intermittent asthma",)),
        (None, "Moderate persistent asthma", ("Moderate asthma",)),
        (None, "Exercise-induced asthma", ("Exercise-induced bronchoconstriction", "EIB")),
        (None, "Aspirin-exacerbated respiratory disease", ("AERD", "Samter triad")),
        (None, "Eosinophilic asthma", ("Type 2 high asthma",)),
        (None, "Steroid-dependent asthma", ("Oral corticosteroid dependent asthma",)),
        (None, "Brittle asthma", ("Unstable asthma",)),
    ],
)

COPD = node(
    "13645005",
    "Chronic obstructive lung disease (disorder)",
    ["COPD", "Chronic obstructive pulmonary disease", "COAD"],
    [AIRWAY_DISEASE],
)
family(
    COPD,
    "disorder",
    [
        ("87433001", "Pulmonary emphysema", ("Emphysema",)),
        ("63480004", "Chronic bronchitis", ("Chronic bronchitis",)),
        ("195951007", "Acute exacerbation of chronic obstructive airways disease", ("AECOPD", "COPD exacerbation")),
        (None, "Chronic obstructive pulmonary disease with chronic bronchitis", ("COPD chronic bronchitis phenotype",)),
        (None, "Alpha-1 antitrypsin deficiency related emphysema", ("A1AT emphysema",)),
        (None, "Asthma-COPD overlap", ("ACO", "Asthma COPD overlap syndrome")),
        (None, "Chronic obstructive pulmonary disease with acute respiratory failure", ("COPD with respiratory failure",)),
    ],
)
family(
    AIRWAY_DISEASE,
    "disorder",
    [
        (SYN, "Bronchiectasis", ("Bronchiectasis",)),
        ("32398004", "Bronchitis", ("Bronchitis",)),
        ("4120002", "Bronchiolitis", ("Bronchiolitis",)),
        (None, "Bronchiolitis obliterans", ("Obliterative bronchiolitis",)),
        (None, "Tracheobronchomalacia", ("Airway malacia",)),
    ],
)

PNEUMONIA = node(
    "233604007",
    "Pneumonia (disorder)",
    ["Pneumonia", "Pneumonitis"],
    [LUNG_DISEASE, LOWER_RESP],
)
family(
    PNEUMONIA,
    "disorder",
    [
        ("385093006", "Community acquired pneumonia", ("CAP",)),
        (SYN, "Hospital acquired pneumonia", ("HAP", "Nosocomial pneumonia")),
        ("53084003", "Bacterial pneumonia", ("Bacterial pneumonia",)),
        ("75570004", "Viral pneumonia", ("Viral pneumonia",)),
        ("233607000", "Pneumocystis jirovecii pneumonia", ("PCP", "PJP")),
        ("29422001", "Pneumonia due to Streptococcus pneumoniae", ("Pneumococcal pneumonia",)),
        ("70036007", "Pneumonia due to Legionella pneumophila", ("Legionnaires disease",)),
        ("422588002", "Aspiration pneumonia", ("Aspiration pneumonitis",)),
        (None, "Ventilator-associated pneumonia", ("VAP",)),
        (None, "Atypical pneumonia", ("Walking pneumonia",)),
        (None, "Lobar pneumonia", ("Lobar consolidation",)),
        (None, "Bronchopneumonia", ("Bronchopneumonia",)),
        (None, "Fungal pneumonia", ("Pulmonary mycosis",)),
    ],
)
INTERSTITIAL = node(
    SYN,
    "Interstitial lung disease (disorder)",
    ["ILD", "Diffuse parenchymal lung disease", "Interstitial lung disease"],
    [LUNG_DISEASE],
)
family(
    INTERSTITIAL,
    "disorder",
    [
        ("700250006", "Idiopathic pulmonary fibrosis", ("IPF",)),
        (SYN, "Pulmonary fibrosis", ("Lung fibrosis",)),
        (SYN, "Hypersensitivity pneumonitis", ("Extrinsic allergic alveolitis", "EAA")),
        (None, "Non-specific interstitial pneumonia", ("NSIP",)),
        (None, "Cryptogenic organising pneumonia", ("COP",)),
        (None, "Asbestosis", ("Asbestos pneumoconiosis",)),
        (None, "Silicosis", ("Silica pneumoconiosis",)),
        (None, "Coal workers pneumoconiosis", ("Black lung",)),
        (None, "Drug-induced interstitial lung disease", ("Drug-induced pneumonitis",)),
        (None, "Connective tissue disease associated interstitial lung disease", ("CTD-ILD",)),
    ],
)
family(
    LUNG_DISEASE,
    "disorder",
    [
        (SYN, "Pleural effusion", ("Pleural fluid",)),
        (SYN, "Pneumothorax", ("Collapsed lung",)),
        (SYN, "Chronic respiratory failure", ("Chronic respiratory failure",)),
        ("65710008", "Acute respiratory failure", ("Acute respiratory failure", "ARF")),
        ("67782005", "Acute respiratory distress syndrome", ("ARDS",)),
        (SYN, "Tuberculosis of lung", ("Pulmonary TB", "Pulmonary tuberculosis")),
        ("190905008", "Cystic fibrosis", ("CF", "Mucoviscidosis")),
        ("73430006", "Sleep apnea", ("Sleep apnoea",)),
        (SYN, "Obstructive sleep apnoea", ("OSA", "Obstructive sleep apnea syndrome")),
        (SYN, "Pulmonary oedema", ("Pulmonary edema",)),
        (SYN, "Lung abscess", ("Pulmonary abscess",)),
        (None, "Empyema of pleural cavity", ("Pleural empyema",)),
        (None, "Sarcoidosis of lung", ("Pulmonary sarcoidosis",)),
        (None, "Respiratory syncytial virus bronchiolitis", ("RSV bronchiolitis",)),
        (None, "Haemoptysis of pulmonary origin", ("Pulmonary haemorrhage",)),
        (None, "Atelectasis of lung", ("Lung collapse",)),
        (None, "Chronic hypoxaemic respiratory failure", ("Type 1 respiratory failure",)),
        (None, "Chronic hypercapnic respiratory failure", ("Type 2 respiratory failure",)),
        (None, "Obesity hypoventilation syndrome", ("Pickwickian syndrome", "OHS")),
    ],
)
family(
    RESPIRATORY,
    "disorder",
    [
        (SYN, "Common cold", ("Acute coryza", "Upper respiratory infection")),
        ("54150009", "Upper respiratory infection", ("URTI", "Upper respiratory tract infection")),
        ("36971009", "Sinusitis", ("Sinusitis", "Rhinosinusitis")),
        ("90176007", "Chronic sinusitis", ("Chronic rhinosinusitis", "CRS")),
        ("6142004", "Influenza", ("Flu", "Influenzal illness")),
        ("840539006", "Disease caused by severe acute respiratory syndrome coronavirus 2", ("COVID-19", "SARS-CoV-2 infection")),
        (SYN, "Pleurisy", ("Pleuritis",)),
        (None, "Allergic rhinitis", ("Hay fever", "Seasonal allergic rhinitis")),
        (None, "Chronic cough", ("Persistent cough",)),
        (None, "Post-COVID-19 condition", ("Long COVID",)),
    ],
)

# ==========================================================================
# ENDOCRINE / METABOLIC
# ==========================================================================

DIABETES = node(
    "73211009",
    "Diabetes mellitus (disorder)",
    ["Diabetes mellitus", "DM", "Diabetes"],
    [ENDOCRINE, METABOLIC],
)
T1DM = node(
    "46635009",
    "Diabetes mellitus type 1 (disorder)",
    ["Type 1 diabetes mellitus", "T1DM", "Insulin dependent diabetes mellitus"],
    [DIABETES],
)
T2DM = node(
    "44054006",
    "Diabetes mellitus type 2 (disorder)",
    ["Type 2 diabetes mellitus", "T2DM", "Non-insulin dependent diabetes mellitus"],
    [DIABETES],
)
family(
    T2DM,
    "disorder",
    [
        (None, "Type 2 diabetes mellitus with diabetic nephropathy", ("T2DM with nephropathy",)),
        (None, "Type 2 diabetes mellitus with diabetic retinopathy", ("T2DM with retinopathy",)),
        (None, "Type 2 diabetes mellitus with diabetic neuropathy", ("T2DM with neuropathy",)),
        (None, "Type 2 diabetes mellitus in remission", ("Diabetes in remission",)),
        (None, "Insulin treated type 2 diabetes mellitus", ("Insulin requiring T2DM",)),
    ],
)
family(
    T1DM,
    "disorder",
    [
        (None, "Type 1 diabetes mellitus with ketoacidosis", ("T1DM with DKA",)),
        (None, "Brittle type 1 diabetes mellitus", ("Labile diabetes",)),
        (None, "Latent autoimmune diabetes in adults", ("LADA",)),
    ],
)
family(
    DIABETES,
    "disorder",
    [
        ("11687002", "Gestational diabetes mellitus", ("GDM", "Diabetes of pregnancy")),
        (SYN, "Secondary diabetes mellitus", ("Drug-induced diabetes",)),
        (SYN, "Maturity onset diabetes of the young", ("MODY",)),
        ("420422005", "Ketoacidosis due to diabetes mellitus", ("Diabetic ketoacidosis", "DKA")),
        ("127013003", "Diabetic renal disease", ("Diabetic nephropathy", "Diabetic kidney disease")),
        ("4855003", "Retinopathy due to diabetes mellitus", ("Diabetic retinopathy", "DR")),
        ("49455004", "Diabetic polyneuropathy", ("Diabetic neuropathy",)),
        (None, "Hyperosmolar hyperglycaemic state", ("HHS", "Hyperosmolar non-ketotic coma")),
        (None, "Diabetic foot ulcer", ("Diabetic foot",)),
        (None, "Neonatal diabetes mellitus", ("Neonatal diabetes",)),
    ],
)

THYROID = node(
    "14304000",
    "Disorder of thyroid gland (disorder)",
    ["Thyroid disorder", "Thyroid disease"],
    [ENDOCRINE],
)
HYPOTHYROID = node(
    "40930008",
    "Hypothyroidism (disorder)",
    ["Hypothyroidism", "Underactive thyroid"],
    [THYROID],
)
family(
    HYPOTHYROID,
    "disorder",
    [
        ("21983002", "Hashimoto thyroiditis", ("Chronic lymphocytic thyroiditis", "Autoimmune thyroiditis")),
        (None, "Subclinical hypothyroidism", ("Compensated hypothyroidism",)),
        (None, "Congenital hypothyroidism", ("Cretinism",)),
        (None, "Iatrogenic hypothyroidism", ("Post-ablative hypothyroidism",)),
        (None, "Myxoedema coma", ("Myxedema coma",)),
    ],
)
HYPERTHYROID = node(
    "34486009",
    "Hyperthyroidism (disorder)",
    ["Hyperthyroidism", "Overactive thyroid", "Thyrotoxicosis"],
    [THYROID],
)
family(
    HYPERTHYROID,
    "disorder",
    [
        (SYN, "Graves disease", ("Graves' disease", "Diffuse toxic goitre")),
        (None, "Toxic multinodular goitre", ("Plummer disease",)),
        (None, "Toxic adenoma of thyroid", ("Toxic thyroid nodule",)),
        (None, "Subclinical hyperthyroidism", ("Subclinical thyrotoxicosis",)),
        (None, "Thyroid storm", ("Thyrotoxic crisis",)),
    ],
)
family(
    THYROID,
    "disorder",
    [
        (SYN, "Goitre", ("Goiter", "Thyroid enlargement")),
        (SYN, "Thyroid nodule", ("Nodular thyroid disease",)),
        (SYN, "Thyroiditis", ("Inflammation of thyroid",)),
        (None, "Subacute thyroiditis", ("De Quervain thyroiditis",)),
        (None, "Post-partum thyroiditis", ("Postpartum thyroid dysfunction",)),
    ],
)
family(
    ENDOCRINE,
    "disorder",
    [
        (SYN, "Cushing syndrome", ("Hypercortisolism",)),
        (SYN, "Adrenal insufficiency", ("Hypoadrenalism",)),
        (SYN, "Addison disease", ("Primary adrenal insufficiency",)),
        (SYN, "Hyperparathyroidism", ("Overactive parathyroid",)),
        (SYN, "Hypoparathyroidism", ("Underactive parathyroid",)),
        (SYN, "Hyperprolactinaemia", ("Hyperprolactinemia",)),
        (SYN, "Acromegaly", ("Growth hormone excess",)),
        (SYN, "Hypopituitarism", ("Pituitary insufficiency",)),
        (SYN, "Diabetes insipidus", ("Arginine vasopressin deficiency",)),
        (SYN, "Polycystic ovary syndrome", ("PCOS", "Stein-Leventhal syndrome")),
        (None, "Primary hyperaldosteronism", ("Conn syndrome",)),
        (None, "Phaeochromocytoma", ("Pheochromocytoma",)),
        (None, "Pituitary adenoma", ("Pituitary tumour",)),
        (None, "Multiple endocrine neoplasia type 1", ("MEN1",)),
        (None, "Multiple endocrine neoplasia type 2", ("MEN2",)),
        (None, "Gynaecomastia", ("Gynecomastia",)),
        (None, "Male hypogonadism", ("Testosterone deficiency",)),
    ],
)
family(
    METABOLIC,
    "disorder",
    [
        ("414916001", "Obesity", ("Obesity",)),
        (SYN, "Morbid obesity", ("Severe obesity", "Class III obesity")),
        (SYN, "Metabolic syndrome", ("Syndrome X", "Insulin resistance syndrome")),
        (SYN, "Hyperkalaemia", ("Hyperkalemia", "High potassium")),
        (SYN, "Hypokalaemia", ("Hypokalemia", "Low potassium")),
        ("89627008", "Hyponatremia", ("Hyponatraemia", "Low sodium")),
        ("39355002", "Hypernatremia", ("Hypernatraemia", "High sodium")),
        (SYN, "Hypocalcaemia", ("Hypocalcemia", "Low calcium")),
        ("66931009", "Hypercalcemia", ("Hypercalcaemia", "High calcium")),
        ("302866003", "Hypoglycemia", ("Hypoglycaemia", "Low blood glucose")),
        ("80394007", "Hyperglycemia", ("Hyperglycaemia", "High blood glucose")),
        (SYN, "Hypomagnesaemia", ("Low magnesium",)),
        (SYN, "Metabolic acidosis", ("Acidosis",)),
        (None, "Metabolic alkalosis", ("Alkalosis",)),
        (None, "Lactic acidosis", ("Hyperlactataemia",)),
        (None, "Vitamin D deficiency", ("Hypovitaminosis D",)),
        (None, "Vitamin B12 deficiency", ("Cobalamin deficiency",)),
        (None, "Iron deficiency", ("Low iron stores",)),
        (None, "Gout", ("Gouty arthritis", "Urate arthropathy")),
        (None, "Hyperuricaemia", ("Raised serum urate",)),
        (None, "Phenylketonuria", ("PKU",)),
        (None, "Hereditary haemochromatosis", ("Iron overload",)),
        (None, "Wilson disease", ("Hepatolenticular degeneration",)),
        (None, "Familial hypercholesterolaemia", ("FH", "Familial hypercholesterolemia")),
        (None, "Hypertriglyceridaemia", ("Raised triglycerides",)),
        (None, "Malnutrition", ("Undernutrition", "Protein energy malnutrition")),
        (None, "Refeeding syndrome", ("Refeeding hypophosphataemia",)),
    ],
)

# ==========================================================================
# RENAL / URINARY
# ==========================================================================

KIDNEY_DISEASE = node(
    "90708001",
    "Kidney disease (disorder)",
    ["Kidney disease", "Renal disease", "Nephropathy"],
    [URINARY],
)
CKD = node(
    "709044004",
    "Chronic kidney disease (disorder)",
    ["Chronic kidney disease", "CKD", "Chronic renal failure"],
    [KIDNEY_DISEASE],
)
family(
    CKD,
    "disorder",
    [
        ("431855005", "Chronic kidney disease stage 1", ("CKD stage 1", "CKD G1")),
        ("431856006", "Chronic kidney disease stage 2", ("CKD stage 2", "CKD G2")),
        ("433144002", "Chronic kidney disease stage 3", ("CKD stage 3", "CKD G3")),
        ("431857002", "Chronic kidney disease stage 4", ("CKD stage 4", "CKD G4")),
        ("433146006", "Chronic kidney disease stage 5", ("CKD stage 5", "End stage renal disease")),
        (None, "Chronic kidney disease stage 3a", ("CKD G3a",)),
        (None, "Chronic kidney disease stage 3b", ("CKD G3b",)),
        (None, "End stage renal disease on dialysis", ("ESRD on dialysis", "Dialysis dependent CKD")),
    ],
)
AKI = node(
    "14669001",
    "Acute renal failure syndrome (disorder)",
    ["Acute kidney injury", "AKI", "Acute renal failure"],
    [KIDNEY_DISEASE],
)
family(
    AKI,
    "disorder",
    [
        (None, "Acute kidney injury stage 1", ("AKI stage 1",)),
        (None, "Acute kidney injury stage 2", ("AKI stage 2",)),
        (None, "Acute kidney injury stage 3", ("AKI stage 3",)),
        (None, "Prerenal acute kidney injury", ("Prerenal azotaemia",)),
        (None, "Acute tubular necrosis", ("ATN",)),
        (None, "Contrast-induced nephropathy", ("Contrast nephropathy", "CIN")),
    ],
)
GLOMERULAR = node(
    SYN,
    "Glomerular disease (disorder)",
    ["Glomerulopathy", "Glomerular disorder"],
    [KIDNEY_DISEASE],
)
family(
    GLOMERULAR,
    "disorder",
    [
        ("36171008", "Glomerulonephritis", ("GN", "Nephritis")),
        ("52254009", "Nephrotic syndrome", ("Nephrosis",)),
        (None, "IgA nephropathy", ("Berger disease",)),
        (None, "Membranous nephropathy", ("Membranous glomerulonephritis",)),
        (None, "Minimal change disease", ("Minimal change nephropathy", "MCD")),
        (None, "Focal segmental glomerulosclerosis", ("FSGS",)),
        (None, "Lupus nephritis", ("SLE nephritis",)),
        (None, "Rapidly progressive glomerulonephritis", ("Crescentic glomerulonephritis", "RPGN")),
        (None, "Post-infectious glomerulonephritis", ("Post-streptococcal glomerulonephritis",)),
    ],
)
family(
    KIDNEY_DISEASE,
    "disorder",
    [
        (SYN, "Polycystic kidney disease", ("PKD", "Adult polycystic kidney disease")),
        ("95570007", "Kidney stone", ("Renal calculus", "Nephrolithiasis")),
        (SYN, "Recurrent urinary tract infection", ("Recurrent UTI",)),
        ("45816000", "Pyelonephritis", ("Kidney infection",)),
        (None, "Hydronephrosis", ("Renal pelvic dilatation",)),
        (None, "Renal artery stenosis", ("RAS",)),
        (None, "Renal tubular acidosis", ("RTA",)),
        (None, "Interstitial nephritis", ("Tubulointerstitial nephritis",)),
        (None, "Renal transplant failure", ("Graft failure",)),
        (None, "Analgesic nephropathy", ("Analgesic kidney disease",)),
    ],
)
family(
    URINARY,
    "disorder",
    [
        ("68566005", "Urinary tract infectious disease", ("UTI", "Urinary tract infection")),
        (SYN, "Cystitis", ("Bladder infection",)),
        (SYN, "Urinary incontinence", ("Incontinence of urine",)),
        (SYN, "Urinary retention", ("Retention of urine",)),
        (SYN, "Overactive bladder", ("OAB", "Urge syndrome")),
        (SYN, "Benign prostatic hyperplasia", ("BPH", "Prostatic enlargement")),
        (SYN, "Urethritis", ("Inflammation of urethra",)),
        (None, "Bladder outlet obstruction", ("BOO",)),
        (None, "Neurogenic bladder", ("Neuropathic bladder",)),
        (None, "Vesicoureteric reflux", ("VUR",)),
        (None, "Haematuria", ("Hematuria", "Blood in urine")),
        (None, "Proteinuria", ("Albuminuria",)),
    ],
)

# ==========================================================================
# NEUROLOGICAL
# ==========================================================================

CEREBROVASCULAR = node(
    "62914000",
    "Cerebrovascular disease (disorder)",
    ["Cerebrovascular disease", "CVD of brain"],
    [NERVOUS, CARDIOVASCULAR],
)
STROKE = node(
    "230690007",
    "Cerebrovascular accident (disorder)",
    ["Stroke", "CVA", "Cerebrovascular accident"],
    [CEREBROVASCULAR],
)
family(
    STROKE,
    "disorder",
    [
        ("422504002", "Ischemic stroke", ("Ischaemic stroke", "Cerebral infarction")),
        ("274100004", "Cerebral hemorrhage", ("Intracerebral haemorrhage", "ICH")),
        ("21454007", "Subarachnoid intracranial hemorrhage", ("Subarachnoid haemorrhage", "SAH")),
        (None, "Lacunar stroke", ("Lacunar infarct",)),
        (None, "Cardioembolic stroke", ("Embolic stroke",)),
        (None, "Large artery atherosclerotic stroke", ("Large vessel stroke",)),
        (None, "Cryptogenic stroke", ("Stroke of undetermined aetiology",)),
        (None, "Posterior circulation stroke", ("Vertebrobasilar stroke",)),
    ],
)
family(
    CEREBROVASCULAR,
    "disorder",
    [
        ("266257000", "Transient ischemic attack", ("TIA", "Mini stroke")),
        (SYN, "Cerebral venous sinus thrombosis", ("CVST", "Cerebral venous thrombosis")),
        (None, "Intracranial aneurysm", ("Cerebral aneurysm",)),
        (None, "Carotid artery stenosis", ("Carotid stenosis",)),
        (None, "Vascular dementia", ("Multi-infarct dementia",)),
        (None, "Cerebral small vessel disease", ("Small vessel disease",)),
    ],
)
EPILEPSY = node(
    "84757009",
    "Epilepsy (disorder)",
    ["Epilepsy", "Seizure disorder"],
    [NERVOUS],
)
family(
    EPILEPSY,
    "disorder",
    [
        (SYN, "Focal epilepsy", ("Partial epilepsy", "Localisation-related epilepsy")),
        (SYN, "Generalised epilepsy", ("Generalized epilepsy",)),
        (SYN, "Juvenile myoclonic epilepsy", ("JME",)),
        (SYN, "Childhood absence epilepsy", ("Absence epilepsy", "Petit mal")),
        (SYN, "Temporal lobe epilepsy", ("TLE",)),
        (SYN, "Status epilepticus", ("Convulsive status epilepticus",)),
        (None, "Drug-resistant epilepsy", ("Refractory epilepsy",)),
        (None, "Epilepsy with generalised tonic-clonic seizures", ("Grand mal epilepsy",)),
        (None, "Post-traumatic epilepsy", ("Post-traumatic seizures",)),
    ],
)
DEMENTIA = node(
    "52448006",
    "Dementia (disorder)",
    ["Dementia", "Major neurocognitive disorder"],
    [NERVOUS, MENTAL],
)
family(
    DEMENTIA,
    "disorder",
    [
        ("26929004", "Alzheimer's disease", ("Alzheimer disease", "AD")),
        (SYN, "Dementia with Lewy bodies", ("Lewy body dementia", "DLB")),
        (SYN, "Frontotemporal dementia", ("FTD", "Pick disease")),
        (None, "Mixed dementia", ("Mixed Alzheimer and vascular dementia",)),
        (None, "Young onset dementia", ("Early onset dementia",)),
        (None, "Mild cognitive impairment", ("MCI",)),
    ],
)
PARKINSONISM = node(
    SYN,
    "Parkinsonism (disorder)",
    ["Parkinsonian syndrome", "Parkinsonism"],
    [NERVOUS],
)
family(
    PARKINSONISM,
    "disorder",
    [
        ("49049000", "Parkinson's disease", ("Parkinson disease", "PD", "Idiopathic Parkinsonism")),
        (None, "Progressive supranuclear palsy", ("PSP", "Steele-Richardson-Olszewski syndrome")),
        (None, "Multiple system atrophy", ("MSA",)),
        (None, "Drug-induced parkinsonism", ("Neuroleptic-induced parkinsonism",)),
        (None, "Corticobasal degeneration", ("CBD",)),
    ],
)
HEADACHE_DIS = node(
    SYN,
    "Headache disorder (disorder)",
    ["Headache disorder", "Cephalalgia disorder"],
    [NERVOUS],
)
family(
    HEADACHE_DIS,
    "disorder",
    [
        ("37796009", "Migraine", ("Migraine headache",)),
        (SYN, "Migraine with aura", ("Classical migraine",)),
        (SYN, "Migraine without aura", ("Common migraine",)),
        (SYN, "Chronic migraine", ("Chronic migraine headache",)),
        (SYN, "Tension-type headache", ("Tension headache",)),
        (SYN, "Cluster headache", ("Histamine cephalalgia",)),
        (None, "Medication overuse headache", ("Analgesic rebound headache",)),
        (None, "Trigeminal neuralgia", ("Tic douloureux",)),
    ],
)
NEUROPATHY = node(
    SYN,
    "Peripheral neuropathy (disorder)",
    ["Peripheral neuropathy", "Neuropathy"],
    [NERVOUS],
)
family(
    NEUROPATHY,
    "disorder",
    [
        ("40956001", "Guillain-Barre syndrome", ("GBS", "Acute inflammatory demyelinating polyneuropathy")),
        (SYN, "Carpal tunnel syndrome", ("Median nerve entrapment", "CTS")),
        (None, "Chronic inflammatory demyelinating polyneuropathy", ("CIDP",)),
        (None, "Alcoholic polyneuropathy", ("Alcohol-related neuropathy",)),
        (None, "Chemotherapy-induced peripheral neuropathy", ("CIPN",)),
        (None, "Charcot-Marie-Tooth disease", ("Hereditary motor and sensory neuropathy", "CMT")),
        (None, "Ulnar neuropathy at the elbow", ("Cubital tunnel syndrome",)),
        (None, "Sciatica", ("Lumbar radiculopathy",)),
    ],
)
family(
    NERVOUS,
    "disorder",
    [
        ("24700007", "Multiple sclerosis", ("MS", "Disseminated sclerosis")),
        ("86044005", "Amyotrophic lateral sclerosis", ("ALS", "Motor neurone disease")),
        (SYN, "Myasthenia gravis", ("MG",)),
        (SYN, "Seizure", ("Convulsion", "Fit")),
        (SYN, "Bacterial meningitis", ("Meningitis bacterial",)),
        ("7180009", "Meningitis", ("Inflammation of meninges",)),
        (SYN, "Viral encephalitis", ("Encephalitis viral",)),
        ("45170000", "Encephalitis", ("Inflammation of brain",)),
        ("127295002", "Traumatic brain injury", ("TBI", "Head injury with brain involvement")),
        (SYN, "Cerebral palsy", ("CP",)),
        (SYN, "Hydrocephalus", ("Water on the brain",)),
        (SYN, "Essential tremor", ("Benign essential tremor",)),
        (SYN, "Restless legs syndrome", ("RLS", "Willis-Ekbom disease")),
        (SYN, "Bell palsy", ("Idiopathic facial nerve palsy",)),
        (SYN, "Narcolepsy", ("Narcoleptic syndrome",)),
        (SYN, "Spinal muscular atrophy", ("SMA",)),
        (SYN, "Huntington disease", ("Huntington's chorea",)),
        (None, "Normal pressure hydrocephalus", ("NPH",)),
        (None, "Idiopathic intracranial hypertension", ("IIH", "Pseudotumour cerebri")),
        (None, "Cervical myelopathy", ("Cervical spondylotic myelopathy",)),
        (None, "Syringomyelia", ("Spinal cord syrinx",)),
        (None, "Transverse myelitis", ("Acute transverse myelitis",)),
        (None, "Neuromyelitis optica spectrum disorder", ("NMOSD", "Devic disease")),
        (None, "Delirium", ("Acute confusional state",)),
        (None, "Syncope", ("Fainting", "Vasovagal syncope")),
    ],
)

# ==========================================================================
# PSYCHIATRIC
# ==========================================================================

MOOD_DISORDER = node(
    SYN,
    "Mood disorder (disorder)",
    ["Affective disorder", "Mood disorder"],
    [MENTAL],
)
DEPRESSION = node(
    "35489007",
    "Depressive disorder (disorder)",
    ["Depression", "Depressive disorder", "Clinical depression"],
    [MOOD_DISORDER],
)
family(
    DEPRESSION,
    "disorder",
    [
        ("370143000", "Major depressive disorder", ("MDD", "Major depression")),
        (SYN, "Recurrent depressive disorder", ("Recurrent major depression",)),
        (SYN, "Persistent depressive disorder", ("Dysthymia",)),
        (SYN, "Postnatal depression", ("Postpartum depression", "PND")),
        (None, "Treatment-resistant depression", ("TRD",)),
        (None, "Seasonal affective disorder", ("SAD", "Winter depression")),
        (None, "Severe depressive episode with psychotic symptoms", ("Psychotic depression",)),
    ],
)
BIPOLAR = node(
    "13746004",
    "Bipolar disorder (disorder)",
    ["Bipolar affective disorder", "Manic depression", "Bipolar"],
    [MOOD_DISORDER],
)
family(
    BIPOLAR,
    "disorder",
    [
        (None, "Bipolar I disorder", ("Bipolar type 1",)),
        (None, "Bipolar II disorder", ("Bipolar type 2",)),
        (None, "Cyclothymic disorder", ("Cyclothymia",)),
        (None, "Rapid cycling bipolar disorder", ("Rapid cycling bipolar",)),
        (None, "Bipolar disorder, current episode manic", ("Bipolar mania",)),
        (None, "Bipolar disorder, current episode depressed", ("Bipolar depression",)),
    ],
)
ANXIETY = node(
    "197480006",
    "Anxiety disorder (disorder)",
    ["Anxiety disorder", "Anxiety state"],
    [MENTAL],
)
family(
    ANXIETY,
    "disorder",
    [
        ("21897009", "Generalized anxiety disorder", ("GAD", "Generalised anxiety disorder")),
        ("371631005", "Panic disorder", ("Panic attacks disorder",)),
        (SYN, "Social anxiety disorder", ("Social phobia",)),
        ("47505003", "Posttraumatic stress disorder", ("PTSD", "Post-traumatic stress disorder")),
        ("191736004", "Obsessive-compulsive disorder", ("OCD",)),
        (None, "Agoraphobia", ("Fear of open spaces",)),
        (None, "Specific phobia", ("Simple phobia",)),
        (None, "Health anxiety disorder", ("Illness anxiety disorder", "Hypochondriasis")),
    ],
)
PSYCHOTIC = node(
    "69322001",
    "Psychotic disorder (disorder)",
    ["Psychosis", "Psychotic disorder"],
    [MENTAL],
)
family(
    PSYCHOTIC,
    "disorder",
    [
        ("58214004", "Schizophrenia", ("Schizophrenic disorder",)),
        (SYN, "Schizoaffective disorder", ("Schizoaffective psychosis",)),
        (SYN, "Delusional disorder", ("Paranoid disorder",)),
        (None, "First episode psychosis", ("FEP",)),
        (None, "Drug-induced psychosis", ("Substance-induced psychotic disorder",)),
        (None, "Treatment-resistant schizophrenia", ("Refractory schizophrenia",)),
    ],
)
SUBSTANCE = node(
    SYN,
    "Substance use disorder (disorder)",
    ["Substance misuse", "Substance use disorder", "Addiction"],
    [MENTAL],
)
family(
    SUBSTANCE,
    "disorder",
    [
        ("7200002", "Alcoholism", ("Alcohol dependence", "Alcohol use disorder")),
        (SYN, "Opioid dependence", ("Opioid use disorder",)),
        (SYN, "Nicotine dependence", ("Tobacco use disorder", "Smoking dependence")),
        (SYN, "Cannabis use disorder", ("Cannabis dependence",)),
        (SYN, "Cocaine dependence", ("Cocaine use disorder",)),
        (None, "Benzodiazepine dependence", ("Benzodiazepine use disorder",)),
        (None, "Alcohol withdrawal syndrome", ("Alcohol withdrawal",)),
        (None, "Opioid withdrawal syndrome", ("Opioid withdrawal",)),
    ],
)
family(
    MENTAL,
    "disorder",
    [
        (SYN, "Anorexia nervosa", ("AN",)),
        (SYN, "Bulimia nervosa", ("BN",)),
        (SYN, "Binge eating disorder", ("BED",)),
        ("406506008", "Attention deficit hyperactivity disorder", ("ADHD", "Hyperkinetic disorder")),
        ("35919005", "Pervasive developmental disorder", ("Autism spectrum disorder", "ASD")),
        (SYN, "Emotionally unstable personality disorder", ("Borderline personality disorder", "EUPD")),
        (SYN, "Insomnia disorder", ("Chronic insomnia",)),
        (SYN, "Self-harm", ("Deliberate self-harm", "DSH")),
        (SYN, "Suicidal ideation", ("Suicidal thoughts",)),
        (SYN, "Somatic symptom disorder", ("Somatisation disorder",)),
        (None, "Adjustment disorder", ("Adjustment reaction",)),
        (None, "Intellectual disability", ("Learning disability",)),
        (None, "Tourette syndrome", ("Gilles de la Tourette syndrome",)),
        (None, "Antisocial personality disorder", ("Dissocial personality disorder",)),
        (None, "Gambling disorder", ("Pathological gambling",)),
    ],
)

# ==========================================================================
# MUSCULOSKELETAL / RHEUMATOLOGY
# ==========================================================================

ARTHROPATHY = node(
    SYN,
    "Arthropathy (disorder)",
    ["Joint disease", "Arthropathy"],
    [MUSCULOSKELETAL],
)
ARTHRITIS = node(
    "3723001",
    "Arthritis (disorder)",
    ["Arthritis", "Joint inflammation"],
    [ARTHROPATHY],
)
OSTEOARTHRITIS = node(
    "396275006",
    "Osteoarthritis (disorder)",
    ["Osteoarthritis", "OA", "Degenerative joint disease"],
    [ARTHROPATHY],
)
family(
    OSTEOARTHRITIS,
    "disorder",
    [
        (SYN, "Osteoarthritis of knee", ("Knee OA", "Gonarthrosis")),
        (SYN, "Osteoarthritis of hip", ("Hip OA", "Coxarthrosis")),
        (SYN, "Osteoarthritis of hand", ("Hand OA",)),
        (None, "Osteoarthritis of shoulder", ("Shoulder OA",)),
        (None, "Osteoarthritis of spine", ("Spondylosis",)),
        (None, "Generalised osteoarthritis", ("Polyarticular osteoarthritis",)),
        (None, "Erosive osteoarthritis", ("Inflammatory osteoarthritis",)),
    ],
)
INFLAMMATORY_ARTHRITIS = node(
    None,
    "Inflammatory arthritis (disorder)",
    ["Inflammatory joint disease",],
    [ARTHRITIS],
)
family(
    INFLAMMATORY_ARTHRITIS,
    "disorder",
    [
        ("69896004", "Rheumatoid arthritis", ("RA",)),
        (SYN, "Psoriatic arthritis", ("PsA", "Arthritis psoriatica")),
        (SYN, "Ankylosing spondylitis", ("AS", "Axial spondyloarthritis")),
        ("90560007", "Gout", ("Gouty arthritis",)),
        (SYN, "Reactive arthritis", ("Reiter syndrome",)),
        (SYN, "Juvenile idiopathic arthritis", ("JIA", "Juvenile arthritis")),
        (SYN, "Calcium pyrophosphate deposition disease", ("Pseudogout", "CPPD")),
        (SYN, "Septic arthritis", ("Infective arthritis", "Pyogenic arthritis")),
        (None, "Enteropathic arthritis", ("IBD-associated arthritis",)),
        (None, "Seronegative rheumatoid arthritis", ("Seronegative RA",)),
        (None, "Seropositive rheumatoid arthritis", ("Seropositive RA",)),
        (None, "Polymyalgia rheumatica", ("PMR",)),
    ],
)
BACK_PAIN = node(
    "161891005",
    "Backache (finding)",
    ["Back pain", "Backache"],
    [MUSCULOSKELETAL, FINDING_BY_SITE],
)
family(
    BACK_PAIN,
    "finding",
    [
        ("279039007", "Low back pain", ("Lumbago", "LBP")),
        (None, "Chronic low back pain", ("Chronic back pain",)),
        (None, "Thoracic back pain", ("Dorsalgia",)),
        (None, "Mechanical back pain", ("Non-specific low back pain",)),
    ],
)
family(
    MUSCULOSKELETAL,
    "disorder",
    [
        ("64859006", "Osteoporosis", ("Osteoporosis",)),
        (SYN, "Osteopenia", ("Low bone mass",)),
        ("203082005", "Fibromyalgia", ("Fibromyalgia syndrome", "FMS")),
        (SYN, "Lateral epicondylitis", ("Tennis elbow",)),
        (SYN, "Medial epicondylitis", ("Golfer elbow",)),
        (SYN, "Adhesive capsulitis of shoulder", ("Frozen shoulder",)),
        (SYN, "Rotator cuff syndrome", ("Rotator cuff tear",)),
        (SYN, "Plantar fasciitis", ("Plantar heel pain",)),
        (SYN, "Tendinopathy", ("Tendinitis", "Tendonitis")),
        (SYN, "Paget disease of bone", ("Osteitis deformans",)),
        (SYN, "Osteomalacia", ("Softening of bone",)),
        (SYN, "Osteomyelitis", ("Bone infection",)),
        (SYN, "Cervical spondylosis", ("Cervical degenerative disc disease",)),
        (SYN, "Lumbar spinal stenosis", ("Spinal canal stenosis",)),
        (SYN, "Intervertebral disc prolapse", ("Slipped disc", "Herniated disc")),
        (SYN, "Scoliosis", ("Lateral spinal curvature",)),
        (SYN, "Bursitis", ("Inflammation of bursa",)),
        (SYN, "Dupuytren contracture", ("Palmar fibromatosis",)),
        (None, "Sarcopenia", ("Age-related muscle loss",)),
        (None, "Rhabdomyolysis", ("Muscle breakdown",)),
        (None, "Myopathy", ("Muscle disease",)),
        (None, "Duchenne muscular dystrophy", ("DMD",)),
        (None, "Hypermobility spectrum disorder", ("Joint hypermobility",)),
    ],
)
family(
    CONNECTIVE,
    "disorder",
    [
        ("55464009", "Systemic lupus erythematosus", ("SLE", "Lupus")),
        ("89155008", "Systemic sclerosis", ("Scleroderma", "SSc")),
        (SYN, "Sjogren syndrome", ("Sicca syndrome",)),
        (SYN, "Dermatomyositis", ("Dermatomyositis",)),
        (SYN, "Polymyositis", ("Idiopathic inflammatory myopathy",)),
        (SYN, "Ehlers-Danlos syndrome", ("EDS",)),
        (SYN, "Marfan syndrome", ("Marfan's syndrome",)),
        (SYN, "Mixed connective tissue disease", ("MCTD",)),
        (None, "Antiphospholipid syndrome", ("APS", "Hughes syndrome")),
        (None, "Granulomatosis with polyangiitis", ("GPA", "Wegener granulomatosis")),
        (None, "Eosinophilic granulomatosis with polyangiitis", ("EGPA", "Churg-Strauss syndrome")),
        (None, "Microscopic polyangiitis", ("MPA",)),
        (None, "Behcet disease", ("Behcet syndrome",)),
    ],
)

# ==========================================================================
# INFECTIOUS DISEASE
# ==========================================================================

BACTERIAL_INF = node(
    "87628006",
    "Bacterial infectious disease (disorder)",
    ["Bacterial infection",],
    [INFECTIOUS],
)
VIRAL_INF = node(
    "34014006",
    "Viral disease (disorder)",
    ["Viral infection", "Virus infection"],
    [INFECTIOUS],
)
FUNGAL_INF = node(
    "3218000",
    "Mycosis (disorder)",
    ["Fungal infection", "Mycosis"],
    [INFECTIOUS],
)
PARASITIC_INF = node(
    "17322007",
    "Parasitic disease (disorder)",
    ["Parasitic infection", "Parasitosis"],
    [INFECTIOUS],
)
SEPSIS = node(
    "91302008",
    "Sepsis (disorder)",
    ["Sepsis", "Septicaemia", "Septicemia"],
    [INFECTIOUS],
)
family(
    SEPSIS,
    "disorder",
    [
        ("76571007", "Septic shock", ("Septic shock",)),
        (SYN, "Severe sepsis", ("Sepsis with organ dysfunction",)),
        (None, "Neutropenic sepsis", ("Febrile neutropenia",)),
        (None, "Urosepsis", ("Urinary sepsis",)),
        (None, "Neonatal sepsis", ("Sepsis of newborn",)),
        (None, "Catheter-related bloodstream infection", ("CRBSI", "Line infection")),
    ],
)
family(
    BACTERIAL_INF,
    "disorder",
    [
        (SYN, "Mycobacterial infection", ("Mycobacteriosis",)),
        (SYN, "Meningococcal infection", ("Neisseria meningitidis infection",)),
        (SYN, "Clostridioides difficile infection", ("C difficile infection", "CDI")),
        (SYN, "Staphylococcus aureus infection", ("Staph aureus infection",)),
        (SYN, "Meticillin resistant Staphylococcus aureus infection", ("MRSA infection",)),
        (SYN, "Group A streptococcal infection", ("Strep A infection",)),
        (SYN, "Syphilis", ("Lues",)),
        (SYN, "Gonorrhoea", ("Gonorrhea", "GC infection")),
        (SYN, "Chlamydia trachomatis infection", ("Chlamydia",)),
        (SYN, "Lyme disease", ("Lyme borreliosis",)),
        (SYN, "Typhoid fever", ("Enteric fever",)),
        (SYN, "Pertussis", ("Whooping cough",)),
        (SYN, "Tetanus", ("Lockjaw",)),
        (None, "Cellulitis", ("Skin and soft tissue infection",)),
        (None, "Necrotising fasciitis", ("Necrotizing fasciitis",)),
        (None, "Infected diabetic foot ulcer", ("Diabetic foot infection",)),
        (None, "Extended spectrum beta-lactamase producing organism infection", ("ESBL infection",)),
        (None, "Carbapenemase producing enterobacterales infection", ("CPE infection",)),
    ],
)
TUBERCULOSIS = node(
    "56717001",
    "Tuberculosis (disorder)",
    ["Tuberculosis", "TB"],
    [BACTERIAL_INF],
)
family(
    TUBERCULOSIS,
    "disorder",
    [
        (None, "Pulmonary tuberculosis", ("Lung TB",)),
        (None, "Miliary tuberculosis", ("Disseminated TB",)),
        (None, "Tuberculous meningitis", ("TB meningitis",)),
        (None, "Latent tuberculosis infection", ("LTBI",)),
        (None, "Multidrug-resistant tuberculosis", ("MDR-TB",)),
        (None, "Extrapulmonary tuberculosis", ("Extra-pulmonary TB",)),
    ],
)
family(
    VIRAL_INF,
    "disorder",
    [
        ("86406008", "Human immunodeficiency virus infection", ("HIV infection", "HIV")),
        ("62479008", "Acquired immune deficiency syndrome", ("AIDS",)),
        ("66071002", "Viral hepatitis type B", ("Hepatitis B", "HBV infection")),
        ("50711007", "Viral hepatitis type C", ("Hepatitis C", "HCV infection")),
        ("40468003", "Viral hepatitis, type A", ("Hepatitis A", "HAV infection")),
        (SYN, "Herpes zoster", ("Shingles",)),
        (SYN, "Herpes simplex infection", ("HSV infection", "Cold sore")),
        ("38907003", "Varicella", ("Chickenpox",)),
        ("14189004", "Measles", ("Rubeola",)),
        ("36989005", "Mumps", ("Epidemic parotitis",)),
        ("36653000", "Rubella", ("German measles",)),
        (SYN, "Infectious mononucleosis", ("Glandular fever", "EBV infection")),
        (SYN, "Cytomegalovirus infection", ("CMV infection",)),
        (SYN, "Human papillomavirus infection", ("HPV infection",)),
        (None, "Norovirus gastroenteritis", ("Winter vomiting disease",)),
        (None, "Dengue fever", ("Dengue",)),
        (None, "Respiratory syncytial virus infection", ("RSV infection",)),
    ],
)
family(
    FUNGAL_INF,
    "disorder",
    [
        (SYN, "Candidiasis", ("Candida infection", "Thrush")),
        (SYN, "Histoplasmosis", ("Histoplasma infection",)),
        (SYN, "Aspergillosis", ("Aspergillus infection",)),
        (None, "Invasive aspergillosis", ("Invasive pulmonary aspergillosis",)),
        (None, "Cryptococcal meningitis", ("Cryptococcosis of CNS",)),
        (None, "Dermatophytosis", ("Tinea", "Ringworm")),
    ],
)
family(
    PARASITIC_INF,
    "disorder",
    [
        ("61462000", "Malaria", ("Malarial infection",)),
        (None, "Plasmodium falciparum malaria", ("Falciparum malaria",)),
        (None, "Giardiasis", ("Giardia infection",)),
        (None, "Toxoplasmosis", ("Toxoplasma infection",)),
        (None, "Schistosomiasis", ("Bilharzia",)),
        (None, "Strongyloidiasis", ("Strongyloides infection",)),
    ],
)

# ==========================================================================
# ONCOLOGY / NEOPLASMS
# ==========================================================================

MALIGNANT_NEOPLASM = node(
    "363346000",
    "Malignant neoplastic disease (disorder)",
    ["Cancer", "Malignancy", "Malignant neoplasm"],
    [NEOPLASM],
)
BENIGN_NEOPLASM = node(
    "20376005",
    "Benign neoplastic disease (disorder)",
    ["Benign tumour", "Benign neoplasm"],
    [NEOPLASM],
)
CARCINOMA = node(
    "68453008",
    "Carcinoma (morphologic abnormality)",
    ["Carcinoma",],
    [MALIGNANT_NEOPLASM],
)
family(
    MALIGNANT_NEOPLASM,
    "disorder",
    [
        ("254837009", "Malignant neoplasm of breast", ("Breast cancer",)),
        ("254632001", "Small cell carcinoma of lung", ("Small cell lung cancer", "SCLC")),
        (SYN, "Non-small cell lung cancer", ("NSCLC",)),
        ("363358000", "Malignant tumor of lung", ("Lung cancer",)),
        ("363406005", "Malignant tumor of colon", ("Colon cancer",)),
        (SYN, "Colorectal carcinoma", ("Colorectal cancer", "CRC")),
        ("399068003", "Malignant tumor of prostate", ("Prostate cancer",)),
        ("363418001", "Malignant tumor of pancreas", ("Pancreatic cancer",)),
        (SYN, "Malignant tumour of stomach", ("Gastric cancer",)),
        (SYN, "Malignant tumour of oesophagus", ("Oesophageal cancer", "Esophageal cancer")),
        ("109838007", "Overlapping malignant neoplasm of colon", ("Overlapping colon cancer",)),
        (SYN, "Malignant melanoma of skin", ("Melanoma",)),
        (SYN, "Malignant tumour of kidney", ("Renal cell carcinoma", "RCC")),
        (SYN, "Malignant tumour of urinary bladder", ("Bladder cancer",)),
        (SYN, "Malignant tumour of ovary", ("Ovarian cancer",)),
        (SYN, "Malignant tumour of cervix uteri", ("Cervical cancer",)),
        (SYN, "Malignant tumour of uterus", ("Endometrial cancer", "Uterine cancer")),
        (SYN, "Malignant tumour of liver", ("Hepatocellular carcinoma", "HCC")),
        (SYN, "Malignant tumour of brain", ("Brain tumour", "Glioma")),
        (SYN, "Glioblastoma", ("Glioblastoma multiforme", "GBM")),
        (SYN, "Malignant tumour of thyroid gland", ("Thyroid cancer",)),
        (SYN, "Malignant tumour of head and neck", ("Head and neck cancer",)),
        (SYN, "Malignant tumour of testis", ("Testicular cancer",)),
        (SYN, "Sarcoma", ("Soft tissue sarcoma",)),
        (SYN, "Metastatic malignant neoplasm", ("Secondary cancer", "Metastasis")),
        (SYN, "Cancer of unknown primary", ("CUP",)),
    ],
)
HAEM_MALIGNANCY = node(
    SYN,
    "Haematological malignancy (disorder)",
    ["Blood cancer", "Haematological malignancy", "Hematologic malignancy"],
    [MALIGNANT_NEOPLASM, HAEMATOLOGIC],
)
family(
    HAEM_MALIGNANCY,
    "disorder",
    [
        ("93143009", "Leukemia", ("Leukaemia",)),
        ("91861009", "Acute myeloid leukemia", ("AML", "Acute myeloid leukaemia")),
        (SYN, "Acute lymphoblastic leukaemia", ("ALL",)),
        (SYN, "Chronic lymphocytic leukaemia", ("CLL",)),
        (SYN, "Chronic myeloid leukaemia", ("CML",)),
        (SYN, "Lymphoma", ("Malignant lymphoma",)),
        (SYN, "Hodgkin lymphoma", ("Hodgkin disease", "HL")),
        (SYN, "Non-Hodgkin lymphoma", ("NHL",)),
        ("109989006", "Multiple myeloma", ("Myeloma", "Plasma cell myeloma")),
        (SYN, "Myelodysplastic syndrome", ("MDS",)),
        (SYN, "Myeloproliferative neoplasm", ("MPN",)),
        (None, "Polycythaemia vera", ("Polycythemia vera", "PV")),
        (None, "Essential thrombocythaemia", ("Essential thrombocythemia", "ET")),
        (None, "Primary myelofibrosis", ("Myelofibrosis",)),
        (None, "Diffuse large B-cell lymphoma", ("DLBCL",)),
        (None, "Follicular lymphoma", ("FL",)),
        (None, "Monoclonal gammopathy of undetermined significance", ("MGUS",)),
    ],
)
family(
    BENIGN_NEOPLASM,
    "disorder",
    [
        (SYN, "Uterine leiomyoma", ("Fibroid", "Uterine fibroid")),
        (SYN, "Lipoma", ("Benign fatty tumour",)),
        ("68496003", "Polyp of colon", ("Colonic polyp",)),
        (None, "Colonic adenoma", ("Adenomatous polyp",)),
        (None, "Haemangioma", ("Hemangioma",)),
        (None, "Meningioma", ("Benign meningeal tumour",)),
        (None, "Seborrhoeic keratosis", ("Seborrheic keratosis",)),
    ],
)

# ==========================================================================
# DERMATOLOGY
# ==========================================================================

DERMATITIS = node(
    "43116000",
    "Eczema (disorder)",
    ["Eczema", "Dermatitis"],
    [SKIN],
)
family(
    DERMATITIS,
    "disorder",
    [
        ("24079001", "Atopic dermatitis", ("Atopic eczema", "Infantile eczema")),
        ("40275004", "Contact dermatitis", ("Contact eczema",)),
        (SYN, "Allergic contact dermatitis", ("Allergic contact eczema",)),
        (SYN, "Irritant contact dermatitis", ("Irritant eczema",)),
        (SYN, "Seborrhoeic dermatitis", ("Seborrheic dermatitis",)),
        (None, "Discoid eczema", ("Nummular eczema",)),
        (None, "Pompholyx", ("Dyshidrotic eczema",)),
        (None, "Venous eczema", ("Stasis dermatitis", "Gravitational eczema")),
    ],
)
PSORIASIS = node(
    "9014002",
    "Psoriasis (disorder)",
    ["Psoriasis",],
    [SKIN],
)
family(
    PSORIASIS,
    "disorder",
    [
        (SYN, "Plaque psoriasis", ("Psoriasis vulgaris",)),
        (SYN, "Guttate psoriasis", ("Guttate psoriasis",)),
        (None, "Pustular psoriasis", ("Generalised pustular psoriasis",)),
        (None, "Scalp psoriasis", ("Psoriasis of scalp",)),
        (None, "Nail psoriasis", ("Psoriatic nail disease",)),
        (None, "Erythrodermic psoriasis", ("Psoriatic erythroderma",)),
    ],
)
family(
    SKIN,
    "disorder",
    [
        ("11381005", "Acne vulgaris", ("Acne",)),
        (SYN, "Rosacea", ("Acne rosacea",)),
        ("126485001", "Urticaria", ("Hives", "Nettle rash")),
        (SYN, "Cellulitis of skin", ("Skin cellulitis",)),
        (SYN, "Hidradenitis suppurativa", ("Acne inversa", "HS")),
        (SYN, "Impetigo", ("Impetigo contagiosa",)),
        (SYN, "Pressure ulcer", ("Bed sore", "Decubitus ulcer")),
        (SYN, "Pressure sore", ("Pressure damage",)),
        (SYN, "Vitiligo", ("Leucoderma",)),
        (SYN, "Alopecia areata", ("Patchy hair loss",)),
        (SYN, "Androgenetic alopecia", ("Male pattern baldness",)),
        (SYN, "Basal cell carcinoma of skin", ("BCC", "Rodent ulcer")),
        (SYN, "Squamous cell carcinoma of skin", ("Cutaneous SCC",)),
        (SYN, "Actinic keratosis", ("Solar keratosis",)),
        (SYN, "Lichen planus", ("Lichen planus",)),
        (SYN, "Bullous pemphigoid", ("Pemphigoid",)),
        (SYN, "Scabies", ("Sarcoptes scabiei infestation",)),
        (None, "Erythema multiforme", ("EM",)),
        (None, "Stevens-Johnson syndrome", ("SJS",)),
        (None, "Toxic epidermal necrolysis", ("TEN", "Lyell syndrome")),
        (None, "Pyoderma gangrenosum", ("PG",)),
        (None, "Chronic spontaneous urticaria", ("CSU", "Chronic idiopathic urticaria")),
    ],
)

# ==========================================================================
# GASTROINTESTINAL / HEPATIC
# ==========================================================================

UPPER_GI = node(
    None,
    "Disorder of upper gastrointestinal tract (disorder)",
    ["Upper GI disorder",],
    [DIGESTIVE],
)
LOWER_GI = node(
    None,
    "Disorder of lower gastrointestinal tract (disorder)",
    ["Lower GI disorder",],
    [DIGESTIVE],
)
family(
    UPPER_GI,
    "disorder",
    [
        ("235595009", "Gastroesophageal reflux disease", ("GORD", "GERD", "Acid reflux")),
        (SYN, "Barrett oesophagus", ("Barrett's oesophagus", "Barrett esophagus")),
        ("13200003", "Peptic ulcer", ("Peptic ulcer disease", "PUD")),
        (SYN, "Gastric ulcer", ("Stomach ulcer",)),
        (SYN, "Duodenal ulcer", ("Duodenal ulceration",)),
        ("4556007", "Gastritis", ("Inflammation of stomach",)),
        (SYN, "Helicobacter pylori infection", ("H pylori infection",)),
        (SYN, "Dyspepsia", ("Indigestion",)),
        (SYN, "Achalasia", ("Achalasia of cardia",)),
        (SYN, "Oesophagitis", ("Esophagitis",)),
        (None, "Eosinophilic oesophagitis", ("EoE", "Eosinophilic esophagitis")),
        (None, "Upper gastrointestinal haemorrhage", ("Upper GI bleed", "UGIB")),
        (None, "Oesophageal varices", ("Esophageal varices",)),
        (None, "Gastroparesis", ("Delayed gastric emptying",)),
        (None, "Hiatus hernia", ("Hiatal hernia",)),
    ],
)
IBD = node(
    "24526004",
    "Inflammatory bowel disease (disorder)",
    ["IBD", "Inflammatory bowel disease"],
    [LOWER_GI],
)
family(
    IBD,
    "disorder",
    [
        ("34000006", "Crohn's disease", ("Crohn disease", "Regional enteritis")),
        ("64766004", "Ulcerative colitis", ("UC",)),
        (None, "Indeterminate colitis", ("IBD unclassified", "IBDU")),
        (None, "Ileocolonic Crohn disease", ("Ileocolonic Crohn's",)),
        (None, "Perianal Crohn disease", ("Perianal fistulising Crohn's",)),
        (None, "Acute severe ulcerative colitis", ("ASUC",)),
        (None, "Pouchitis", ("Inflammation of ileoanal pouch",)),
    ],
)
family(
    LOWER_GI,
    "disorder",
    [
        (SYN, "Irritable bowel syndrome", ("IBS", "Spastic colon")),
        (SYN, "Diverticular disease of colon", ("Diverticulosis",)),
        (SYN, "Diverticulitis", ("Diverticulitis of colon",)),
        (SYN, "Coeliac disease", ("Celiac disease", "Gluten enteropathy")),
        ("25374005", "Gastroenteritis", ("Gastro", "Infectious diarrhoea")),
        ("14760008", "Constipation", ("Costiveness",)),
        ("62315008", "Diarrhea", ("Diarrhoea", "Loose stools")),
        (SYN, "Haemorrhoids", ("Hemorrhoids", "Piles")),
        (SYN, "Anal fissure", ("Fissure in ano",)),
        (SYN, "Perianal abscess", ("Anorectal abscess",)),
        (SYN, "Bowel obstruction", ("Intestinal obstruction",)),
        (SYN, "Ischaemic colitis", ("Ischemic colitis",)),
        (SYN, "Microscopic colitis", ("Lymphocytic colitis",)),
        (None, "Lower gastrointestinal haemorrhage", ("Lower GI bleed", "LGIB")),
        (None, "Small intestinal bacterial overgrowth", ("SIBO",)),
        (None, "Inguinal hernia", ("Groin hernia",)),
        (None, "Appendicitis", ("Inflammation of appendix",)),
        (None, "Faecal incontinence", ("Fecal incontinence",)),
    ],
)
family(
    HEPATIC,
    "disorder",
    [
        ("19943007", "Cirrhosis of liver", ("Hepatic cirrhosis", "Liver cirrhosis")),
        (SYN, "Non-alcoholic fatty liver disease", ("NAFLD", "Metabolic dysfunction-associated steatotic liver disease")),
        (SYN, "Non-alcoholic steatohepatitis", ("NASH",)),
        (SYN, "Alcoholic liver disease", ("Alcohol-related liver disease", "ARLD")),
        (SYN, "Hepatic steatosis", ("Fatty liver",)),
        ("128241005", "Inflammatory disease of liver", ("Hepatitis",)),
        (SYN, "Autoimmune hepatitis", ("AIH",)),
        (SYN, "Primary biliary cholangitis", ("PBC", "Primary biliary cirrhosis")),
        (SYN, "Primary sclerosing cholangitis", ("PSC",)),
        (SYN, "Hepatic encephalopathy", ("Portosystemic encephalopathy",)),
        (SYN, "Portal hypertension", ("Portal hypertension",)),
        (SYN, "Ascites", ("Peritoneal fluid collection",)),
        (SYN, "Acute liver failure", ("Fulminant hepatic failure",)),
        (SYN, "Drug-induced liver injury", ("DILI", "Hepatotoxicity")),
        (None, "Decompensated cirrhosis", ("Decompensated liver disease",)),
        (None, "Spontaneous bacterial peritonitis", ("SBP",)),
        (None, "Hepatorenal syndrome", ("HRS",)),
    ],
)
BILIARY_PANCREAS = node(
    None,
    "Disorder of biliary tract and pancreas (disorder)",
    ["Hepatobiliary and pancreatic disorder",],
    [DIGESTIVE],
)
family(
    BILIARY_PANCREAS,
    "disorder",
    [
        ("235919008", "Cholelithiasis", ("Gallstones", "Gallstone disease")),
        (SYN, "Cholecystitis", ("Inflammation of gallbladder",)),
        ("75694006", "Pancreatitis", ("Inflammation of pancreas",)),
        (SYN, "Acute pancreatitis", ("Acute pancreatic inflammation",)),
        (SYN, "Chronic pancreatitis", ("Chronic pancreatic inflammation",)),
        (SYN, "Cholangitis", ("Biliary infection",)),
        (None, "Ascending cholangitis", ("Acute cholangitis",)),
        (None, "Exocrine pancreatic insufficiency", ("EPI",)),
        (None, "Choledocholithiasis", ("Bile duct stone",)),
    ],
)

# ==========================================================================
# HAEMATOLOGY (non-malignant)
# ==========================================================================

ANAEMIA = node(
    "271737000",
    "Anemia (disorder)",
    ["Anaemia", "Anemia", "Low haemoglobin"],
    [BLOOD],
)
family(
    ANAEMIA,
    "disorder",
    [
        ("87522002", "Iron deficiency anemia", ("Iron deficiency anaemia", "IDA")),
        (SYN, "Anaemia of chronic disease", ("Anaemia of inflammation",)),
        (SYN, "Pernicious anaemia", ("Vitamin B12 deficiency anaemia",)),
        (SYN, "Folate deficiency anaemia", ("Folate deficiency anemia",)),
        (SYN, "Haemolytic anaemia", ("Hemolytic anemia",)),
        (SYN, "Autoimmune haemolytic anaemia", ("AIHA",)),
        (SYN, "Sickle cell disease", ("Sickle cell anaemia", "SCD")),
        (SYN, "Thalassaemia", ("Thalassemia",)),
        (SYN, "Aplastic anaemia", ("Aplastic anemia",)),
        (SYN, "Anaemia of chronic kidney disease", ("Renal anaemia",)),
        (None, "Macrocytic anaemia", ("Macrocytic anemia",)),
        (None, "Microcytic anaemia", ("Microcytic anemia",)),
        (None, "Normocytic anaemia", ("Normocytic anemia",)),
        (None, "Glucose-6-phosphate dehydrogenase deficiency", ("G6PD deficiency",)),
    ],
)
COAGULOPATHY = node(
    SYN,
    "Coagulation disorder (disorder)",
    ["Coagulopathy", "Clotting disorder"],
    [BLOOD],
)
family(
    COAGULOPATHY,
    "disorder",
    [
        (SYN, "Haemophilia A", ("Hemophilia A", "Factor VIII deficiency")),
        (SYN, "Haemophilia B", ("Hemophilia B", "Christmas disease")),
        (SYN, "Von Willebrand disease", ("vWD",)),
        (SYN, "Disseminated intravascular coagulation", ("DIC", "Consumptive coagulopathy")),
        (SYN, "Thrombophilia", ("Hypercoagulable state",)),
        (None, "Factor V Leiden thrombophilia", ("Factor V Leiden",)),
        (None, "Warfarin-related coagulopathy", ("Over-anticoagulation",)),
    ],
)
family(
    BLOOD,
    "disorder",
    [
        (SYN, "Thrombocytopenia", ("Low platelet count",)),
        (SYN, "Immune thrombocytopenic purpura", ("ITP", "Idiopathic thrombocytopenic purpura")),
        (SYN, "Neutropenia", ("Low neutrophil count",)),
        (SYN, "Pancytopenia", ("Reduction in all blood cell lines",)),
        (SYN, "Leucopenia", ("Leukopenia", "Low white cell count")),
        (SYN, "Polycythaemia", ("Polycythemia", "Raised haematocrit")),
        (SYN, "Eosinophilia", ("Raised eosinophil count",)),
        (SYN, "Lymphopenia", ("Low lymphocyte count",)),
        (SYN, "Thrombocytosis", ("Raised platelet count",)),
        (None, "Heparin-induced thrombocytopenia", ("HIT",)),
        (None, "Methaemoglobinaemia", ("Methemoglobinemia",)),
        (None, "Haemochromatosis with iron overload", ("Transfusional iron overload",)),
    ],
)

# ==========================================================================
# OPHTHALMOLOGY
# ==========================================================================

family(
    EYE,
    "disorder",
    [
        ("193570009", "Cataract", ("Lens opacity",)),
        ("23986001", "Glaucoma", ("Glaucomatous optic neuropathy",)),
        (SYN, "Primary open angle glaucoma", ("POAG",)),
        (SYN, "Acute angle closure glaucoma", ("Angle closure glaucoma",)),
        (SYN, "Age-related macular degeneration", ("AMD", "ARMD")),
        (SYN, "Wet age-related macular degeneration", ("Neovascular AMD",)),
        (SYN, "Dry age-related macular degeneration", ("Atrophic AMD",)),
        (SYN, "Conjunctivitis", ("Pink eye",)),
        (SYN, "Bacterial conjunctivitis", ("Infective conjunctivitis",)),
        (SYN, "Allergic conjunctivitis", ("Atopic conjunctivitis",)),
        (SYN, "Uveitis", ("Intraocular inflammation",)),
        (SYN, "Anterior uveitis", ("Iritis",)),
        (SYN, "Retinal detachment", ("Detached retina",)),
        (SYN, "Retinal vein occlusion", ("RVO",)),
        (SYN, "Retinal artery occlusion", ("RAO",)),
        (SYN, "Diabetic macular oedema", ("DMO", "Diabetic macular edema")),
        (SYN, "Proliferative diabetic retinopathy", ("PDR",)),
        (SYN, "Dry eye disease", ("Keratoconjunctivitis sicca",)),
        (SYN, "Keratitis", ("Corneal inflammation",)),
        (SYN, "Keratoconus", ("Conical cornea",)),
        (SYN, "Blepharitis", ("Lid margin inflammation",)),
        (SYN, "Amblyopia", ("Lazy eye",)),
        (SYN, "Strabismus", ("Squint",)),
        (SYN, "Optic neuritis", ("Inflammation of optic nerve",)),
        (None, "Hypertensive retinopathy", ("Retinopathy due to hypertension",)),
        (None, "Myopia", ("Short-sightedness", "Near-sightedness")),
        (None, "Hypermetropia", ("Long-sightedness", "Hyperopia")),
        (None, "Presbyopia", ("Age-related long-sightedness",)),
    ],
)

# ==========================================================================
# ENT
# ==========================================================================

family(
    EAR_NOSE_THROAT,
    "disorder",
    [
        ("65363002", "Otitis media", ("Middle ear infection",)),
        (SYN, "Acute otitis media", ("AOM",)),
        (SYN, "Otitis media with effusion", ("Glue ear", "OME")),
        (SYN, "Chronic suppurative otitis media", ("CSOM",)),
        (SYN, "Otitis externa", ("Swimmer ear",)),
        (SYN, "Tonsillitis", ("Inflammation of tonsils",)),
        (SYN, "Acute pharyngitis", ("Sore throat",)),
        (SYN, "Laryngitis", ("Inflammation of larynx",)),
        ("60862001", "Tinnitus", ("Ringing in the ears",)),
        ("15188001", "Hearing loss", ("Deafness", "Hearing impairment")),
        (SYN, "Sensorineural hearing loss", ("SNHL",)),
        (SYN, "Conductive hearing loss", ("Conductive deafness",)),
        (SYN, "Presbycusis", ("Age-related hearing loss",)),
        (SYN, "Benign paroxysmal positional vertigo", ("BPPV",)),
        (SYN, "Meniere disease", ("Meniere's disease", "Endolymphatic hydrops")),
        (SYN, "Vestibular neuritis", ("Vestibular neuronitis",)),
        (SYN, "Nasal polyp", ("Polyp of nose",)),
        (SYN, "Deviated nasal septum", ("Septal deviation",)),
        (SYN, "Epistaxis", ("Nosebleed",)),
        (SYN, "Obstructive sleep apnoea of adult", ("Adult OSA",)),
        (SYN, "Peritonsillar abscess", ("Quinsy",)),
        (SYN, "Vocal cord nodule", ("Singer nodule",)),
        (None, "Chronic rhinosinusitis with nasal polyps", ("CRSwNP",)),
        (None, "Eustachian tube dysfunction", ("ETD",)),
    ],
)

# ==========================================================================
# OBSTETRIC
# ==========================================================================

family(
    OBSTETRIC,
    "disorder",
    [
        ("398254007", "Pre-eclampsia", ("Preeclampsia", "Pregnancy-induced hypertension with proteinuria")),
        (SYN, "Eclampsia", ("Eclamptic convulsion",)),
        (SYN, "Pregnancy-induced hypertension", ("Gestational hypertension",)),
        ("17369002", "Miscarriage", ("Spontaneous abortion",)),
        ("34801009", "Ectopic pregnancy", ("Tubal pregnancy",)),
        (SYN, "Postpartum haemorrhage", ("PPH", "Postpartum hemorrhage")),
        (SYN, "Antepartum haemorrhage", ("APH",)),
        (SYN, "Placenta praevia", ("Placenta previa",)),
        (SYN, "Placental abruption", ("Abruptio placentae",)),
        (SYN, "Preterm labour", ("Premature labor",)),
        (SYN, "Premature rupture of membranes", ("PROM",)),
        (SYN, "Intrauterine growth restriction", ("IUGR", "Fetal growth restriction")),
        (SYN, "Hyperemesis gravidarum", ("Severe pregnancy sickness",)),
        (SYN, "Obstetric cholestasis", ("Intrahepatic cholestasis of pregnancy", "ICP")),
        (SYN, "HELLP syndrome", ("Haemolysis elevated liver enzymes low platelets",)),
        (SYN, "Gestational thrombocytopenia", ("Thrombocytopenia of pregnancy",)),
        (None, "Recurrent miscarriage", ("Recurrent pregnancy loss",)),
        (None, "Postnatal anaemia", ("Postpartum anaemia",)),
        (None, "Puerperal sepsis", ("Postpartum sepsis",)),
    ],
)

# ==========================================================================
# PAEDIATRIC / NEONATAL
# ==========================================================================

family(
    PAEDIATRIC,
    "disorder",
    [
        (SYN, "Neonatal jaundice", ("Jaundice of newborn",)),
        (SYN, "Respiratory distress syndrome of newborn", ("Hyaline membrane disease", "Neonatal RDS")),
        (SYN, "Neonatal hypoglycaemia", ("Hypoglycaemia of newborn",)),
        (SYN, "Necrotising enterocolitis of newborn", ("NEC",)),
        (SYN, "Bronchopulmonary dysplasia", ("BPD", "Chronic lung disease of prematurity")),
        (SYN, "Hypoxic ischaemic encephalopathy", ("HIE", "Birth asphyxia")),
        (SYN, "Prematurity of infant", ("Preterm infant",)),
        (SYN, "Low birth weight infant", ("LBW infant",)),
        (SYN, "Croup", ("Acute laryngotracheobronchitis",)),
        (SYN, "Febrile convulsion", ("Febrile seizure",)),
        (SYN, "Kawasaki disease", ("Mucocutaneous lymph node syndrome",)),
        (SYN, "Hand, foot and mouth disease", ("HFMD",)),
        (SYN, "Intussusception", ("Bowel telescoping",)),
        (SYN, "Infantile hypertrophic pyloric stenosis", ("Pyloric stenosis",)),
        (SYN, "Developmental dysplasia of the hip", ("DDH",)),
        (SYN, "Nocturnal enuresis", ("Bedwetting",)),
        (None, "Transient tachypnoea of the newborn", ("TTN",)),
        (None, "Retinopathy of prematurity", ("ROP",)),
        (None, "Neonatal abstinence syndrome", ("NAS",)),
        (None, "Down syndrome", ("Trisomy 21",)),
    ],
)

# ==========================================================================
# INJURY / TRAUMA
# ==========================================================================

FRACTURE = node(
    "125605004",
    "Fracture of bone (disorder)",
    ["Fracture", "Bone fracture"],
    [INJURY],
)
family(
    FRACTURE,
    "disorder",
    [
        ("5913000", "Fracture of neck of femur", ("Hip fracture", "NOF fracture")),
        (SYN, "Fracture of distal radius", ("Colles fracture", "Wrist fracture")),
        (SYN, "Fracture of vertebral column", ("Vertebral fracture",)),
        (SYN, "Fracture of rib", ("Rib fracture",)),
        (SYN, "Fracture of ankle", ("Ankle fracture",)),
        (SYN, "Fracture of humerus", ("Humeral fracture",)),
        (SYN, "Fracture of tibia", ("Tibial fracture",)),
        (SYN, "Fracture of clavicle", ("Clavicular fracture",)),
        (SYN, "Fracture of skull", ("Skull fracture",)),
        (SYN, "Pathological fracture", ("Fracture through diseased bone",)),
        (None, "Fragility fracture", ("Osteoporotic fracture",)),
        (None, "Open fracture", ("Compound fracture",)),
        (None, "Stress fracture", ("Fatigue fracture",)),
    ],
)
family(
    INJURY,
    "disorder",
    [
        ("125666000", "Burn", ("Burn injury",)),
        (SYN, "Laceration", ("Cut", "Open wound")),
        (SYN, "Contusion", ("Bruise",)),
        (SYN, "Sprain", ("Ligamentous sprain",)),
        (SYN, "Dislocation of joint", ("Joint dislocation",)),
        (SYN, "Concussion injury of brain", ("Concussion", "Mild traumatic brain injury")),
        (SYN, "Subdural haematoma", ("Subdural hematoma", "SDH")),
        (SYN, "Extradural haematoma", ("Epidural haematoma", "EDH")),
        (SYN, "Spinal cord injury", ("SCI",)),
        (SYN, "Whiplash injury of neck", ("Whiplash",)),
        (SYN, "Crush injury", ("Crush trauma",)),
        (SYN, "Traumatic amputation", ("Amputation injury",)),
        (SYN, "Poisoning", ("Toxic ingestion",)),
        (SYN, "Paracetamol overdose", ("Acetaminophen overdose",)),
        (SYN, "Opioid overdose", ("Narcotic overdose",)),
        (SYN, "Anaphylaxis", ("Anaphylactic reaction",)),
        (None, "Polytrauma", ("Multiple trauma",)),
        (None, "Fall from standing height", ("Ground level fall",)),
        (None, "Inhalation injury", ("Smoke inhalation",)),
        (None, "Heat stroke", ("Heatstroke",)),
        (None, "Hypothermia", ("Low body temperature",)),
    ],
)

# ==========================================================================
# IMMUNE / ALLERGY
# ==========================================================================

family(
    IMMUNE,
    "disorder",
    [
        (SYN, "Allergy to substance", ("Allergic disposition",)),
        (SYN, "Allergy to nut", ("Nut allergy",)),
        (SYN, "Allergy to penicillin", ("Penicillin allergy",)),
        (SYN, "Allergy to peanut", ("Peanut allergy",)),
        (SYN, "Common variable immunodeficiency", ("CVID",)),
        (SYN, "Selective IgA deficiency", ("IgA deficiency",)),
        (SYN, "Severe combined immunodeficiency", ("SCID",)),
        (SYN, "Secondary immunodeficiency", ("Acquired immunodeficiency",)),
        (None, "Drug hypersensitivity", ("Drug allergy",)),
        (None, "Food allergy", ("Food hypersensitivity",)),
        (None, "Hereditary angioedema", ("HAE", "C1 inhibitor deficiency")),
        (None, "Mast cell activation syndrome", ("MCAS",)),
    ],
)

# ==========================================================================
# GENERAL FINDINGS / SYMPTOMS (findings, not disorders)
# ==========================================================================

GENERAL_FINDING = node(
    None,
    "General clinical finding (finding)",
    ["General finding", "Constitutional finding"],
    [CLINICAL_FINDING],
)
family(
    GENERAL_FINDING,
    "finding",
    [
        ("386661006", "Fever", ("Pyrexia", "High temperature")),
        ("267036007", "Dyspnea", ("Dyspnoea", "Shortness of breath", "SOB")),
        ("29857009", "Chest pain", ("Thoracic pain",)),
        ("22253000", "Pain", ("Painful sensation",)),
        ("84229001", "Fatigue", ("Tiredness", "Lethargy")),
        ("422587007", "Nausea", ("Feeling sick",)),
        ("422400008", "Vomiting", ("Emesis",)),
        ("25064002", "Headache", ("Cephalalgia",)),
        ("404640003", "Dizziness", ("Light-headedness",)),
        ("271594007", "Syncope", ("Fainting",)),
        ("49727002", "Cough", ("Tussis",)),
        (SYN, "Weight loss", ("Loss of weight",)),
        (SYN, "Weight gain", ("Increase in weight",)),
        (SYN, "Night sweats", ("Nocturnal hyperhidrosis",)),
        ("68962001", "Myalgia", ("Muscle pain",)),
        ("57676002", "Arthralgia", ("Joint pain",)),
        (SYN, "Abdominal pain", ("Stomach ache",)),
        (SYN, "Loss of appetite", ("Anorexia symptom",)),
        (SYN, "Wheeze", ("Wheezing",)),
        (SYN, "Palpitations", ("Awareness of heartbeat",)),
        ("267038008", "Edema", ("Oedema", "Swelling")),
        (SYN, "Skin rash", ("Eruption of skin",)),
        (SYN, "Itching", ("Pruritus",)),
        (SYN, "Asthenia", ("Weakness",)),
        (None, "Reduced exercise tolerance", ("Exercise intolerance",)),
        (None, "Frailty", ("Frailty syndrome",)),
        (None, "Cachexia", ("Wasting syndrome",)),
    ],
)

# ==========================================================================
# PROCEDURES (so search breadth is not findings-only)
# ==========================================================================

SURGICAL = node(
    "387713003",
    "Surgical procedure (procedure)",
    ["Surgery", "Operative procedure"],
    [PROCEDURE],
)
IMAGING = node(
    "363679005",
    "Imaging (procedure)",
    ["Imaging procedure", "Radiological procedure"],
    [PROCEDURE],
)
LAB_PROC = node(
    "108252007",
    "Laboratory procedure (procedure)",
    ["Laboratory test", "Lab procedure"],
    [PROCEDURE],
)
THERAPEUTIC = node(
    "277132007",
    "Therapeutic procedure (procedure)",
    ["Therapy", "Therapeutic intervention"],
    [PROCEDURE],
)
family(
    SURGICAL,
    "procedure",
    [
        ("80146002", "Appendectomy", ("Appendicectomy", "Removal of appendix")),
        ("38102005", "Cholecystectomy", ("Removal of gallbladder",)),
        ("232717009", "Coronary artery bypass graft", ("CABG", "Coronary bypass")),
        ("415070008", "Percutaneous coronary intervention", ("PCI", "Coronary angioplasty")),
        ("52734007", "Total replacement of hip", ("Total hip replacement", "THR")),
        ("609588000", "Total knee replacement", ("TKR", "Total arthroplasty of knee")),
        ("11466000", "Cesarean section", ("Caesarean section", "C-section")),
        (SYN, "Hernia repair", ("Herniorrhaphy",)),
        (SYN, "Colectomy", ("Resection of colon",)),
        (SYN, "Mastectomy", ("Removal of breast",)),
        (SYN, "Prostatectomy", ("Removal of prostate",)),
        (SYN, "Nephrectomy", ("Removal of kidney",)),
        (SYN, "Craniotomy", ("Surgical opening of skull",)),
        (SYN, "Laparotomy", ("Surgical opening of abdomen",)),
        (SYN, "Cataract extraction with intraocular lens implant", ("Cataract surgery",)),
        (SYN, "Transcatheter aortic valve implantation", ("TAVI", "TAVR")),
        (SYN, "Implantation of cardiac pacemaker", ("Pacemaker insertion",)),
        (SYN, "Catheter ablation of cardiac tissue", ("Cardiac ablation",)),
        (SYN, "Tonsillectomy", ("Removal of tonsils",)),
        (SYN, "Bariatric surgery", ("Weight loss surgery",)),
    ],
)
family(
    IMAGING,
    "procedure",
    [
        ("399208008", "Plain chest X-ray", ("Chest radiograph", "CXR")),
        ("77477000", "Computerized tomography", ("CT scan", "CAT scan")),
        ("113091000", "Magnetic resonance imaging", ("MRI", "MR imaging")),
        ("16310003", "Ultrasonography", ("Ultrasound scan", "USS")),
        ("40701008", "Echocardiography", ("Echocardiogram", "Echo")),
        (SYN, "Positron emission tomography", ("PET scan",)),
        (SYN, "Computed tomography of chest", ("CT chest",)),
        (SYN, "Computed tomography pulmonary angiogram", ("CTPA",)),
        (SYN, "Magnetic resonance imaging of brain", ("MRI brain",)),
        (SYN, "Coronary angiography", ("Cardiac catheterisation",)),
        (SYN, "Mammography", ("Breast radiography",)),
        (SYN, "Dual energy X-ray absorptiometry", ("DEXA scan", "DXA")),
    ],
)
family(
    LAB_PROC,
    "procedure",
    [
        ("26604007", "Complete blood count", ("Full blood count", "FBC", "CBC")),
        (SYN, "Blood chemistry panel", ("Urea and electrolytes", "U&E")),
        ("43396009", "Hemoglobin A1c measurement", ("HbA1c", "Glycated haemoglobin measurement")),
        (SYN, "Serum creatinine measurement", ("Creatinine level",)),
        (SYN, "Estimated glomerular filtration rate measurement", ("eGFR measurement",)),
        (SYN, "Liver function test", ("LFT",)),
        (SYN, "Thyroid function test", ("TFT",)),
        (SYN, "C-reactive protein measurement", ("CRP measurement",)),
        (SYN, "Blood culture", ("Culture of blood",)),
        (SYN, "Urine culture", ("Culture of urine",)),
        (SYN, "Troponin measurement", ("Cardiac troponin assay",)),
        (SYN, "B-type natriuretic peptide measurement", ("BNP measurement", "NT-proBNP")),
        (SYN, "Arterial blood gas analysis", ("ABG",)),
        (SYN, "Lipid panel", ("Lipid profile",)),
        (SYN, "Coagulation screen", ("Clotting screen", "INR")),
    ],
)
family(
    THERAPEUTIC,
    "procedure",
    [
        ("265764009", "Renal dialysis", ("Dialysis",)),
        (SYN, "Haemodialysis", ("Hemodialysis", "HD")),
        (SYN, "Peritoneal dialysis", ("PD",)),
        (SYN, "Mechanical ventilation", ("Invasive ventilation",)),
        (SYN, "Non-invasive ventilation", ("NIV", "BiPAP")),
        (SYN, "Oxygen therapy", ("Supplemental oxygen",)),
        ("367336001", "Chemotherapy", ("Antineoplastic chemotherapy",)),
        ("108290001", "Radiation oncology AND/OR radiotherapy", ("Radiotherapy", "Radiation therapy")),
        (SYN, "Physiotherapy", ("Physical therapy",)),
        (SYN, "Cognitive behavioural therapy", ("CBT",)),
        (SYN, "Palliative care", ("End of life care",)),
        (SYN, "Medication review", ("Review of medication",)),
        (SYN, "Blood transfusion", ("Transfusion of blood product",)),
        (SYN, "Wound care", ("Wound dressing",)),
        (SYN, "Smoking cessation support", ("Stop smoking intervention",)),
    ],
)

# ==========================================================================
# PRODUCTS (pharmaceutical / biologic)
# ==========================================================================

family(
    PRODUCT,
    "product",
    [
        ("387517004", "Paracetamol", ("Acetaminophen",)),
        ("387207008", "Ibuprofen", ("Ibuprofen",)),
        ("372756006", "Warfarin", ("Warfarin sodium",)),
        ("387458008", "Aspirin", ("Acetylsalicylic acid", "ASA")),
        ("372687004", "Amoxicillin", ("Amoxycillin",)),
        ("387467008", "Insulin", ("Insulin product",)),
        ("109081006", "Metformin", ("Metformin hydrochloride",)),
        (SYN, "Atorvastatin", ("Atorvastatin calcium",)),
        (SYN, "Ramipril", ("Ramipril product",)),
        (SYN, "Bisoprolol", ("Bisoprolol fumarate",)),
        (SYN, "Amlodipine", ("Amlodipine besilate",)),
        (SYN, "Omeprazole", ("Omeprazole product",)),
        (SYN, "Furosemide", ("Frusemide",)),
        (SYN, "Salbutamol", ("Albuterol",)),
        (SYN, "Prednisolone", ("Prednisolone product",)),
        (SYN, "Levothyroxine", ("Thyroxine", "L-thyroxine")),
        (SYN, "Apixaban", ("Apixaban product",)),
        (SYN, "Rivaroxaban", ("Rivaroxaban product",)),
        (SYN, "Clopidogrel", ("Clopidogrel product",)),
        (SYN, "Sertraline", ("Sertraline hydrochloride",)),
        (SYN, "Gabapentin", ("Gabapentin product",)),
        (SYN, "Co-amoxiclav", ("Amoxicillin and clavulanic acid",)),
        (SYN, "Enoxaparin", ("Low molecular weight heparin",)),
        (SYN, "Dapagliflozin", ("SGLT2 inhibitor dapagliflozin",)),
        (SYN, "Empagliflozin", ("SGLT2 inhibitor empagliflozin",)),
    ],
)

# ==========================================================================
# OBSERVABLE ENTITIES
# ==========================================================================

family(
    OBSERVABLE,
    "observable entity",
    [
        ("75367002", "Blood pressure", ("BP",)),
        ("364075005", "Heart rate", ("Pulse rate", "HR")),
        ("86290005", "Respiratory rate", ("RR",)),
        ("386725007", "Body temperature", ("Core temperature",)),
        ("103228002", "Hemoglobin saturation with oxygen", ("Oxygen saturation", "SpO2")),
        ("27113001", "Body weight", ("Weight",)),
        ("50373000", "Body height measure", ("Height",)),
        ("60621009", "Body mass index", ("BMI",)),
        ("271649006", "Systolic blood pressure", ("SBP",)),
        ("271650005", "Diastolic blood pressure", ("DBP",)),
        (SYN, "Estimated glomerular filtration rate", ("eGFR",)),
        (SYN, "Serum creatinine concentration", ("Creatinine concentration",)),
        (SYN, "Glycated haemoglobin level", ("HbA1c level",)),
        (SYN, "Left ventricular ejection fraction", ("LVEF",)),
        (SYN, "Pain severity score", ("Pain score",)),
        (SYN, "National early warning score", ("NEWS2 score",)),
        (SYN, "Glasgow coma scale score", ("GCS",)),
    ],
)

# ==========================================================================
# BODY STRUCTURES
# ==========================================================================

family(
    BODY_STRUCTURE,
    "body structure",
    [
        ("80891009", "Heart structure", ("Heart",)),
        ("39607008", "Lung structure", ("Lung",)),
        ("64033007", "Kidney structure", ("Kidney",)),
        ("10200004", "Liver structure", ("Liver",)),
        ("12738006", "Brain structure", ("Brain",)),
        ("89837001", "Urinary bladder structure", ("Bladder",)),
        ("69695003", "Stomach structure", ("Stomach",)),
        ("71854001", "Colon structure", ("Colon",)),
        ("15497006", "Ovarian structure", ("Ovary",)),
        ("41216001", "Prostatic structure", ("Prostate",)),
        ("76752008", "Breast structure", ("Breast",)),
        ("113257007", "Structure of cardiovascular system", ("Cardiovascular system",)),
        ("20139000", "Structure of respiratory system", ("Respiratory system",)),
        (SYN, "Structure of coronary artery", ("Coronary artery",)),
        ("955009", "Bronchial structure", ("Bronchus",)),
    ],
)

# ==========================================================================
# SITUATIONS AND EVENTS
# ==========================================================================

family(
    SITUATION,
    "situation",
    [
        (SYN, "Family history of diabetes mellitus", ("FH diabetes",)),
        (SYN, "Family history of ischaemic heart disease", ("FH IHD",)),
        (SYN, "History of myocardial infarction", ("Previous MI",)),
        (SYN, "History of stroke", ("Previous stroke",)),
        (SYN, "No known drug allergies", ("NKDA",)),
        (SYN, "At risk of falls", ("Falls risk",)),
        (SYN, "Suspected sepsis", ("Possible sepsis",)),
        (SYN, "Asthma resolved", ("History of asthma, resolved")),
    ],
)
family(
    EVENT,
    "event",
    [
        (SYN, "Fall", ("Accidental fall",)),
        (SYN, "Road traffic accident", ("RTA", "Motor vehicle collision")),
        (SYN, "Hospital admission", ("Admission to hospital",)),
        (SYN, "Unplanned readmission", ("Emergency readmission",)),
        (SYN, "Cardiac arrest event", ("Arrest event",)),
    ],
)


# --------------------------------------------------------------------------
# Programmatic breadth expansion
# --------------------------------------------------------------------------
# To reach demonstration breadth (1500-3000 concepts) without inventing
# plausible-but-wrong real codes, deeper leaves are minted purely in the
# synthetic 9999xxxxx namespace with clinically-coherent modifier phrasing.

SEVERITY_AXIS = [
    ("Mild {base}", ("Mild form of {base}",)),
    ("Moderate {base}", ("Moderate form of {base}",)),
    ("Severe {base}", ("Severe form of {base}",)),
]
COURSE_AXIS = [
    ("Acute {base}", ("Acute episode of {base}",)),
    ("Chronic {base}", ("Chronic {base}",)),
    ("Recurrent {base}", ("Recurring {base}",)),
]
STATUS_AXIS = [
    ("{base} in remission", ("Remission of {base}",)),
    ("{base} with complication", ("Complicated {base}",)),
    ("Well controlled {base}", ("{base}, controlled",)),
    ("Poorly controlled {base}", ("{base}, uncontrolled",)),
]
LATERALITY_AXIS = [
    ("{base} of left side", ("Left-sided {base}",)),
    ("{base} of right side", ("Right-sided {base}",)),
    ("Bilateral {base}", ("{base}, both sides",)),
]


def is_abbreviation(term: str) -> bool:
    """
    Heuristic: does this synonym read as a clinical abbreviation (COPD, HFrEF,
    T2DM, NSTEMI) rather than a descriptive term?

    Abbreviations are kept in the release - they are genuinely how clinicians
    search - but they are marked *acceptable* rather than *preferred* so the
    sidecar's displayTerm stays human-readable.
    """
    word = term.strip()
    if not word or " " in word:
        return False
    letters = [ch for ch in word if ch.isalpha()]
    if not letters:
        return False
    upper = sum(1 for ch in letters if ch.isupper())
    # Mostly-uppercase single tokens of modest length, e.g. COPD, AF, HFrEF,
    # NSTEMI, T2DM, SpO2. "Asthma" / "Diabetes" are unaffected (1 capital).
    return len(word) <= 8 and upper >= max(2, len(letters) - 2)


def lowercase_base(fsn: str) -> str:
    """Strip the semantic tag and lowercase the leading letter for composition."""
    core = fsn.rsplit("(", 1)[0].strip()
    if not core:
        return core
    return core[0].lower() + core[1:]


def expand_axes(
    targets: list[dict],
    axes: list[list[tuple[str, tuple[str, ...]]]],
) -> int:
    """Mint synthetic refinement children beneath each target concept."""
    created = 0
    for target in targets:
        tag = target["fsn"].rsplit("(", 1)[-1].rstrip(")").strip() or "disorder"
        base = lowercase_base(target["fsn"])
        for axis in axes:
            for template, syn_templates in axis:
                fsn_core = template.format(base=base)
                fsn_core = fsn_core[0].upper() + fsn_core[1:]
                synonyms = tuple(
                    s.format(base=base)[0].upper() + s.format(base=base)[1:]
                    for s in syn_templates
                )
                node(None, f"{fsn_core} ({tag})", synonyms, [target["conceptId"]])
                created += 1
    return created


def _snapshot_descendants_of(roots: set[str]) -> list[dict]:
    """Nodes (snapshot of NODES so far) whose authored parents include a root."""
    by_id = {n["conceptId"]: n for n in NODES}
    out = []
    for n in NODES:
        stack = list(n["parents"])
        seen = set()
        while stack:
            p = stack.pop()
            if p in seen:
                continue
            seen.add(p)
            if p in roots:
                out.append(n)
                break
            parent_node = by_id.get(p)
            if parent_node:
                stack.extend(parent_node["parents"])
    return out


# Leaf-level refinement: pick concepts that are currently leaves within the
# major chronic-disease areas and give them a severity/course/status axis.
def _current_leaves() -> set[str]:
    has_child = {p for n in NODES for p in n["parents"]}
    return {n["conceptId"] for n in NODES} - has_child


_leaf_ids = _current_leaves()
_by_id_snapshot = {n["conceptId"]: n for n in NODES}

_refinement_roots = {
    ASTHMA,
    COPD,
    T2DM,
    T1DM,
    CKD,
    HEART_FAILURE,
    HYPERTENSION,
    ATRIAL_FIB,
    DEPRESSION,
    ANXIETY,
    OSTEOARTHRITIS,
    INFLAMMATORY_ARTHRITIS,
    IBD,
    DERMATITIS,
    PSORIASIS,
    ANAEMIA,
    EPILEPSY,
    MYOCARDIAL_INFARCTION,
    PNEUMONIA,
    STROKE,
}
_refine_targets = [
    n
    for n in _snapshot_descendants_of(_refinement_roots)
    if n["conceptId"] in _leaf_ids
]
# Severity for every chronic-disease leaf, but the control/remission status
# axis on a bounded slice - both axes everywhere overshot the 1500-3000 target.
_axis_created = expand_axes(_refine_targets, [SEVERITY_AXIS])
_axis_created += expand_axes(_refine_targets[:30], [STATUS_AXIS])

# Course axis for the symptomatic/general findings and the infectious tree.
_leaf_ids2 = _current_leaves()
_course_targets = [
    n
    for n in _snapshot_descendants_of({GENERAL_FINDING, INFECTIOUS, EYE, EAR_NOSE_THROAT})
    if n["conceptId"] in _leaf_ids2
][:120]
_axis_created += expand_axes(_course_targets, [COURSE_AXIS])

# Laterality axis for a slice of the musculoskeletal and ophthalmology trees.
_leaf_ids3 = _current_leaves()
_lateral_targets = [
    n
    for n in _snapshot_descendants_of({MUSCULOSKELETAL, EYE})
    if n["conceptId"] in _leaf_ids3
][:90]
_axis_created += expand_axes(_lateral_targets, [LATERALITY_AXIS])

# Oncology was the thinnest major area, and stage/grade is the axis that
# actually matters clinically there, so expand malignancies along it.
STAGE_AXIS = [
    ("{base}, stage I", ("Stage 1 {base}", "{base} stage I")),
    ("{base}, stage II", ("Stage 2 {base}", "{base} stage II")),
    ("{base}, stage III", ("Stage 3 {base}", "{base} stage III")),
    ("{base}, stage IV", ("Stage 4 {base}", "Metastatic {base}")),
]
RECURRENCE_AXIS = [
    ("Recurrent {base}", ("Relapsed {base}",)),
    ("Refractory {base}", ("Treatment-refractory {base}",)),
    ("{base} in complete remission", ("{base}, complete remission",)),
]
_leaf_ids4 = _current_leaves()
_onc_targets = [
    n
    for n in _snapshot_descendants_of({MALIGNANT_NEOPLASM})
    if n["conceptId"] in _leaf_ids4
]
# Stage a representative slice of malignancies rather than every leaf: staging
# every one (including each already-staged synthetic subtype) made oncology
# ~24% of the release and swamped the other specialties with near-identical
# "stage N" leaves. A bounded slice keeps ECL staging queries demonstrable
# while leaving the area distribution balanced.
_axis_created += expand_axes(_onc_targets[:60], [STAGE_AXIS])
_axis_created += expand_axes(_onc_targets[:20], [RECURRENCE_AXIS])

# Trauma, obstetric, paediatric, hepatic and immunology were also thin
# relative to the cardio/respiratory spine; give their leaves a severity and
# course axis so breadth is not concentrated in a handful of systems.
_leaf_ids5 = _current_leaves()
# NEOPLASM is deliberately excluded here: the stage axis above already ran on
# its leaves, and layering severity on top of each "stage N" leaf compounded
# (4 stages x 3 severities = 12+ leaves per tumour) and pushed oncology to a
# quarter of the release.
_thin_targets = [
    n
    for n in _snapshot_descendants_of(
        {INJURY, OBSTETRIC, PAEDIATRIC, HEPATIC, IMMUNE, BILIARY_PANCREAS}
    )
    if n["conceptId"] in _leaf_ids5
]
_axis_created += expand_axes(_thin_targets, [SEVERITY_AXIS])


# --------------------------------------------------------------------------
# Closure computation + consistency assertions
# --------------------------------------------------------------------------


def build_release(nodes: list[dict]) -> list[dict]:
    """Turn authored nodes into release concepts with a verified closure."""
    by_id = {n["conceptId"]: n for n in nodes}

    # 1. validate parent references
    for n in nodes:
        for p in n["parents"]:
            if p not in by_id:
                raise ValueError(f"{n['conceptId']} references unknown parent {p}")
            if p == n["conceptId"]:
                raise ValueError(f"{n['conceptId']} is its own parent")

    # 2. children map (reciprocal by construction)
    children: dict[str, set[str]] = {cid: set() for cid in by_id}
    for n in nodes:
        for p in n["parents"]:
            children[p].add(n["conceptId"])

    # 3. cycle detection via Kahn topological sort over parent edges
    indeg = {cid: len(set(by_id[cid]["parents"])) for cid in by_id}
    queue = deque(sorted(cid for cid, d in indeg.items() if d == 0))
    topo: list[str] = []
    while queue:
        cid = queue.popleft()
        topo.append(cid)
        for child in sorted(children[cid]):
            indeg[child] -= 1
            if indeg[child] == 0:
                queue.append(child)
    if len(topo) != len(by_id):
        stuck = sorted(set(by_id) - set(topo))
        raise ValueError(f"cycle detected in IS-A graph; involved: {stuck[:20]}")

    # 4. transitive closure in topological order (parents resolved first)
    ancestors: dict[str, set[str]] = {}
    for cid in topo:
        acc: set[str] = set()
        for p in by_id[cid]["parents"]:
            acc.add(p)
            acc |= ancestors[p]
        if cid in acc:
            raise ValueError(f"{cid} appears in its own ancestor closure")
        ancestors[cid] = acc

    # 5. emit concepts
    desc_seq = [100000000]
    rel_seq = [200000000]
    concepts: list[dict] = []
    for n in nodes:
        cid = n["conceptId"]
        descriptions = []

        def add_description(term: str, type_id: str, preferred: bool) -> None:
            desc_seq[0] += 1
            descriptions.append(
                {
                    "descriptionId": str(desc_seq[0]),
                    "conceptId": cid,
                    "active": True,
                    "effectiveTime": EFFECTIVE_TIME,
                    "moduleId": MODULE_ID,
                    "typeId": type_id,
                    "languageCode": "en",
                    "term": term,
                    "caseSignificanceId": CASE_SIGNIFICANCE,
                    "acceptabilityMap": {
                        LANG_REFSET: ACCEPT_PREFERRED if preferred else ACCEPT_ACCEPTABLE
                    },
                }
            )

        add_description(n["fsn"], TYPE_FSN, True)
        seen_terms = {n["fsn"].strip().lower()}
        # Mirror real SNOMED practice: the preferred synonym is a readable
        # descriptive term, while clinical abbreviations (COPD, AF, HFrEF) are
        # only *acceptable*. Marking an abbreviation preferred would make the
        # sidecar's displayTerm/preferredTerm an opaque acronym and degrade
        # search quality, so the first NON-abbreviation synonym wins the slot.
        candidates = []
        for synonym in n["synonyms"][:3]:
            key = synonym.strip().lower()
            if key in seen_terms:
                continue
            seen_terms.add(key)
            candidates.append(synonym)
        # sidecar.build_term_documents picks its displayTerm from the first
        # SYNONYM (preferred first, then shortest) and only falls back to the
        # FSN when there is no synonym at all. So a concept whose only synonym
        # is an abbreviation would still display as "ARDS". Guarantee a
        # readable descriptive synonym by deriving one from the FSN
        # (tag stripped) when every authored synonym is an abbreviation.
        if candidates and all(is_abbreviation(s) for s in candidates):
            descriptive = n["fsn"].rsplit("(", 1)[0].strip()
            if descriptive and descriptive.strip().lower() not in seen_terms:
                seen_terms.add(descriptive.strip().lower())
                candidates.insert(0, descriptive)
        preferred_index = next(
            (i for i, s in enumerate(candidates) if not is_abbreviation(s)), None
        )
        for index, synonym in enumerate(candidates):
            add_description(synonym, TYPE_SYNONYM, index == preferred_index)

        relationships = []
        for parent in n["parents"]:
            rel_seq[0] += 1
            relationships.append(
                {
                    "relationshipId": str(rel_seq[0]),
                    "sourceId": cid,
                    "destinationId": parent,
                    "typeId": IS_A,
                    "active": True,
                    "effectiveTime": EFFECTIVE_TIME,
                    "moduleId": MODULE_ID,
                    "characteristicTypeId": CHARACTERISTIC_INFERRED,
                    "modifierId": MODIFIER_SOME,
                    "groupId": 0,
                }
            )

        concepts.append(
            {
                "conceptId": cid,
                "active": True,
                "effectiveTime": EFFECTIVE_TIME,
                "moduleId": MODULE_ID,
                "definitionStatusId": DEFINED if n["defined"] else PRIMITIVE,
                "descriptions": descriptions,
                "relationships": relationships,
                "concreteRelationships": [],
                "inferredParentIds": sorted(set(n["parents"])),
                "inferredAncestorIds": sorted(ancestors[cid]),
                "inferredChildIds": sorted(children[cid]),
                "memberOfRefsetIds": [],
            }
        )

    # 6. in-generator assertions (the standalone verifier re-checks all of this)
    index = {c["conceptId"]: c for c in concepts}
    for c in concepts:
        for p in c["inferredParentIds"]:
            assert c["conceptId"] in index[p]["inferredChildIds"], (
                f"non-reciprocal edge {c['conceptId']} -> {p}"
            )
        for ch in c["inferredChildIds"]:
            assert c["conceptId"] in index[ch]["inferredParentIds"], (
                f"non-reciprocal edge {c['conceptId']} <- {ch}"
            )
        assert sorted(ancestors[c["conceptId"]]) == c["inferredAncestorIds"]
        fsn = next(d for d in c["descriptions"] if d["typeId"] == TYPE_FSN)
        assert fsn["term"].rstrip().endswith(")"), f"FSN missing semantic tag: {fsn['term']}"
    return concepts


def summarise(concepts: list[dict]) -> dict:
    index = {c["conceptId"]: c for c in concepts}
    depth_cache: dict[str, int] = {}

    def depth(cid: str) -> int:
        if cid in depth_cache:
            return depth_cache[cid]
        parents = index[cid]["inferredParentIds"]
        depth_cache[cid] = 0 if not parents else 1 + max(depth(p) for p in parents)
        return depth_cache[cid]

    max_depth = max(depth(c["conceptId"]) for c in concepts)
    area_counts = {}
    for cid, label in BODY_SYSTEM_AREAS.items():
        count = sum(1 for c in concepts if cid in c["inferredAncestorIds"])
        area_counts[label] = area_counts.get(label, 0) + count
    return {
        "total": len(concepts),
        "maxDepth": max_depth,
        "areaCounts": dict(sorted(area_counts.items(), key=lambda kv: -kv[1])),
        "realIds": len(REAL_IDS),
        "syntheticIds": len(SYNTH_IDS),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__ and __doc__.splitlines()[1])
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="output path (default: <repo>/kehrnel/.kehrnel/snomedct/releases/hdl-synthetic-2.json)",
    )
    args = parser.parse_args()

    out = args.out
    if out is None:
        out = Path(__file__).resolve().parents[1] / DEFAULT_OUT
    out.parent.mkdir(parents=True, exist_ok=True)

    concepts = build_release(NODES)
    # Stream the array to disk: json.dumps() materialises the whole document as
    # one string and hit MemoryError at a few thousand concepts.
    with out.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("[\n")
        for position, concept in enumerate(concepts):
            if position:
                handle.write(",\n")
            block = json.dumps(concept, indent=2, ensure_ascii=False)
            handle.write("\n".join("  " + line for line in block.splitlines()))
        handle.write("\n]\n")

    stats = summarise(concepts)
    print(f"releaseId  : {RELEASE_ID}")
    print(f"label      : {RELEASE_LABEL}")
    print(f"written    : {out}")
    print(f"concepts   : {stats['total']}")
    print(f"max depth  : {stats['maxDepth']}")
    print(f"real ids   : {stats['realIds']} (referenced for structural fidelity)")
    print(f"synth ids  : {stats['syntheticIds']} (9999xxxxx, openly fake)")
    print("area counts (descendants incl. the area parent's subtree):")
    for label, count in stats["areaCounts"].items():
        print(f"  {label:28s} {count}")
    print()
    print("REMINDER: this is NOT a SNOMED CT release and must not be used clinically.")


if __name__ == "__main__":
    main()
