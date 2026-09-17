"""Deterministic profile projections for canonical CDISC records.

The source ``data`` object remains authoritative. Everything produced here is
versioned, disposable, and can be rebuilt without changing the source record.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from .models import CdiscProfile, EntityReference


PROJECTION_VERSION = "1.0.0"


def _first(data: Dict[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        value = data.get(name)
        if value is not None and value != "":
            return value
    return None


def _put(target: Dict[str, Any], key: str, value: Any) -> None:
    if value is not None and value != "":
        target[key] = value


def _base_facets(profile: CdiscProfile, domain: str, data: Dict[str, Any]) -> Dict[str, Any]:
    facets: Dict[str, Any] = {
        "projectionVersion": PROJECTION_VERSION,
        "subjectType": "animal" if profile == CdiscProfile.SEND else "human",
    }
    candidates = {
        "subjectId": _first(data, ("USUBJID", "SUBJID")),
        "sex": data.get("SEX"),
        "species": data.get("SPECIES"),
        "strain": data.get("STRAIN"),
        "armCode": _first(data, ("ARMCD", "ACTARMCD")),
        "arm": _first(data, ("ARM", "ACTARM")),
        "treatmentGroup": _first(data, ("SPGRPCD", "GRPID", "SETCD")),
        "visitNumber": _first(data, ("AVISITN", "VISITNUM")),
        "visit": _first(data, ("AVISIT", "VISIT")),
        "epoch": data.get("EPOCH"),
        "sequence": _first(data, (f"{domain}SEQ", "ASEQ")),
        "studyDay": _first(data, ("ADY", f"{domain}DY", f"{domain}STDY", f"{domain}ENDY")),
        "testCode": _first(data, (f"{domain}TESTCD", "PARAMCD")),
        "test": _first(data, (f"{domain}TEST", "PARAM")),
        "category": _first(data, (f"{domain}CAT", "PARCAT1")),
        "specimen": _first(data, (f"{domain}SPEC", "SPEC")),
        "severity": _first(data, (f"{domain}SEV", "AESEV")),
        "resultCharacter": _first(data, (f"{domain}STRESC", f"{domain}ORRES", "AVALC")),
        "resultNumeric": _first(data, (f"{domain}STRESN", "AVAL")),
        "resultUnit": _first(data, (f"{domain}STRESU", f"{domain}ORRESU", "AVALU")),
    }
    for key, value in candidates.items():
        _put(facets, key, value)
    return facets


def _send_facets(domain: str, data: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    values = {
        "testArticle": _first(data, ("TRT", "EXTRT", "TXTRT", "SPTRT")),
        "doseLevel": _first(data, ("DOSE", "EXDOSE", "TXDOSE")),
        "doseUnit": _first(data, ("DOSU", "EXDOSU", "TXDOSU")),
        "route": _first(data, ("ROUTE", "EXROUTE", "TXROUTE")),
        "organ": _first(data, ("ORGAN", f"{domain}SPEC", "MISTRESC", "MAORRES")),
        "finding": _first(data, (f"{domain}STRESC", f"{domain}ORRES", f"{domain}TEST")),
        "laterality": _first(data, (f"{domain}LAT", "LAT")),
        "specimenId": _first(data, ("SPECID", "SPDEVID")),
    }
    for key, value in values.items():
        _put(result, key, value)
    return result


def _sdtm_facets(domain: str, data: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    values = {
        "eventTerm": _first(data, ("AEDECOD", "AETERM", "MHDECOD", "MHTERM", "DSDECOD")),
        "intervention": _first(data, ("EXTRT", "CMTRT", "PRTRT")),
        "startDateTime": _first(data, (f"{domain}STDTC", "ASTDT")),
        "endDateTime": _first(data, (f"{domain}ENDTC", "AENDT")),
        "referenceId": _first(data, ("SPDEVID", "REFID")),
    }
    for key, value in values.items():
        _put(result, key, value)
    return result


def _adam_facets(data: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    values = {
        "parameterCode": data.get("PARAMCD"),
        "parameter": data.get("PARAM"),
        "analysisValue": data.get("AVAL"),
        "analysisValueCharacter": data.get("AVALC"),
        "baselineType": _first(data, ("BASETYPE", "ABLFL")),
        "analysisRecordFlag": data.get("ANL01FL"),
        "populationFlags": {
            key: value
            for key, value in data.items()
            if key.endswith("FL") and key not in {"ABLFL", "ANL01FL"} and value not in (None, "")
        },
        "sourceDomain": data.get("SRCDOM"),
        "sourceVariable": data.get("SRCVAR"),
        "sourceSequence": data.get("SRCSEQ"),
    }
    for key, value in values.items():
        _put(result, key, value)
    return result


def _tig_facets(data: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {"subjectType": "evidence"}
    values = {
        "productId": _first(data, ("PRODUCTID", "PRODID", "TRT", "EXTRT")),
        "productName": _first(data, ("PRODUCT", "PRODNAME", "TRT", "EXTRT")),
        "batchId": _first(data, ("BATCHID", "LOTNO", "LOT")),
        "constituent": _first(data, ("CONSTITUENT", "INGREDIENT", "SUBSTANCE")),
        "evidenceType": _first(data, ("EVIDTYPE", "DOMAIN", "CATEGORY")),
        "evidenceId": _first(data, ("EVIDID", "REFID", "STUDYID")),
    }
    for key, value in values.items():
        _put(result, key, value)
    return result


def derive_facets(profile: CdiscProfile, domain: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Return stable cross-profile facets plus one profile overlay."""

    normalized_domain = domain.upper()
    facets = _base_facets(profile, normalized_domain, data)
    if profile == CdiscProfile.SEND:
        facets.update(_send_facets(normalized_domain, data))
    elif profile == CdiscProfile.ADAM:
        facets.update(_adam_facets(data))
    elif profile == CdiscProfile.TIG:
        facets.update(_tig_facets(data))
    else:
        facets.update(_sdtm_facets(normalized_domain, data))
    return facets


def derive_entity_refs(profile: CdiscProfile, data: Dict[str, Any]) -> List[EntityReference]:
    """Create de-duplicated references for graph/materialized views."""

    pairs: List[Tuple[str, Any]] = []
    subject_id = _first(data, ("USUBJID", "SUBJID"))
    if subject_id is not None:
        pairs.append(("animalSubject" if profile == CdiscProfile.SEND else "humanSubject", subject_id))
    pairs.append(("study", data.get("STUDYID")))
    pairs.extend(
        [
            ("treatmentGroup", _first(data, ("SPGRPCD", "GRPID", "SETCD", "ARMCD"))),
            ("specimen", _first(data, ("SPECID", "SPDEVID"))),
            ("testArticle", _first(data, ("TRT", "EXTRT", "TXTRT"))),
            ("product", _first(data, ("PRODUCTID", "PRODID", "PRODUCT"))),
            ("batch", _first(data, ("BATCHID", "LOTNO", "LOT"))),
            ("analysisParameter", data.get("PARAMCD")),
        ]
    )
    seen = set()
    refs = []
    for kind, value in pairs:
        if value is None or value == "":
            continue
        key = (kind, str(value))
        if key not in seen:
            refs.append(EntityReference(type=kind, id=str(value)))
            seen.add(key)
    return refs


def project_record(
    profile: CdiscProfile,
    domain: str,
    data: Dict[str, Any],
) -> tuple[Dict[str, Any], List[EntityReference]]:
    return derive_facets(profile, domain, data), derive_entity_refs(profile, data)
