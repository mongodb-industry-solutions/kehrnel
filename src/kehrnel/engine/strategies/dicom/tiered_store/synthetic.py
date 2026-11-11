from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from .common import fail


def generate(payload):
    count = payload.get("instances", 12)
    if type(count) is not int or not 1 <= count <= 100:
        fail("DICOM_SYNTHETIC_BOUND_EXCEEDED", "Generate between 1 and 100 instances per demo series.")
    series_count = payload.get("seriesPerStudy", 1)
    if type(series_count) is not int or not 1 <= series_count <= 32:
        fail("DICOM_SYNTHETIC_BOUND_EXCEEDED", "Generate between 1 and 32 series per study.")
    modalities = payload.get("modalities", ["MG", "CT"])
    if not isinstance(modalities, list) or len(modalities) != 2 or any(x not in {"MG", "CT", "MR", "US", "OT", "CR", "DX", "PT", "SR"} for x in modalities):
        fail("DICOM_SYNTHETIC_INVALID", "Provide two supported demo modality codes.")
    try:
        start = date.fromisoformat(payload.get("day", "2026-01-01"))
    except (ValueError, TypeError):
        fail("DICOM_DATE_INVALID", "The synthetic day must be a valid ISO calendar date.")
    # Deliberately include 1, 2, 10 and two centers with the same local patient ID.
    studies, instances = [], []
    for center_index, center in enumerate(("DEMO_A", "DEMO_B"), 1):
        study_uid = "2.25." + str(1000 + center_index)
        modality = modalities[center_index - 1]
        series_docs = []
        study_day = start if "day" in payload else start + timedelta(days=center_index - 1)
        studies.append({"_id": study_uid, "dm": center, "pi": "SYNTHETIC_PATIENT_1",
                        "pn": "SYNTHETIC^DEMO", "an": "SYNTHETIC_SHARED_ACCESSION",
                        "dt": datetime.combine(study_day, datetime.min.time(), tzinfo=timezone.utc),
                        "se": series_docs})
        for series_index in range(series_count):
            series_uid = "2.25." + str(2000 + center_index) + ("." + str(series_index) if series_index else "")
            series_docs.append({"si": series_uid, "ci": center, "sn": str(series_index + 1), "mo": modality,
                                "ni": count, "de": "Synthetic imaging metadata; no pixels"})
            for number in range(1, count + 1):
                sop_uid = "2.25." + str(center_index * 100000 + series_index * 1000 + number)
                header = {tag: {"vr": vr, "Value": [value]} for tag, vr, value in (
                    ("00080018", "UI", sop_uid), ("0020000D", "UI", study_uid),
                    ("0020000E", "UI", series_uid), ("00200013", "IS", str(number)),
                    ("00080060", "CS", modality), ("00080070", "LO", "SYNTHETIC_MANUFACTURER"),
                    ("00180050", "DS", str(number / 2)))}
                instances.append({"_id": sop_uid, "header": header, "sz": 0})
    return {"ok": True, "synthetic": True, "containsPixels": False,
            "generatorVersion": "1.0.0", "studies": studies, "instances": instances}
