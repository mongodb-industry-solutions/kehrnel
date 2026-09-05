"""Constraint-aware deterministic synthetic studies for all CDISC SDR profiles."""

from __future__ import annotations

import hashlib
import json
import random
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

PROFILE_DOMAINS = {
    "sdtm": ["DM", "AE", "LB", "VS"],
    "send": ["DM", "TX", "MI", "LB"],
    "adam": ["ADSL", "ADAE", "ADLB"],
    "tig": ["PROD", "BATCH", "EVID"],
}

SYNTHETIC_MODEL_CATALOG_VERSION = "1.1.0"


class SyntheticRecipe(BaseModel):
    study_id: str = Field(default="SYNTH-001", alias="studyId")
    profile: Literal["sdtm", "send", "adam", "tig"] = "sdtm"
    subjects: int = Field(default=20, ge=1, le=10_000)
    seed: int = 20260821
    domains: list[str] | None = None
    anomaly_rate: float = Field(default=0.0, ge=0.0, le=0.25, alias="anomalyRate")
    scenario: Literal["baseline", "safety-signal"] = "baseline"

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @model_validator(mode="after")
    def normalize_domains(self):
        if self.scenario == "safety-signal" and self.profile != "send":
            raise ValueError("the safety-signal scenario is only available for the SEND profile")
        requested = self.domains or PROFILE_DOMAINS[self.profile]
        normalized = list(dict.fromkeys(domain.upper() for domain in requested))
        unsupported = sorted(set(normalized) - set(PROFILE_DOMAINS[self.profile]))
        if unsupported:
            raise ValueError(f"unsupported synthetic {self.profile} domains: {unsupported}")
        if self.scenario == "safety-signal":
            missing = sorted(set(PROFILE_DOMAINS["send"]) - set(normalized))
            if missing:
                raise ValueError(
                    "the safety-signal scenario requires DM, TX, MI, and LB; "
                    f"missing: {missing}"
                )
        anchor = PROFILE_DOMAINS[self.profile][0]
        if anchor not in normalized:
            normalized.insert(0, anchor)
        self.domains = normalized
        return self


def _column(name: str, label: str, data_type: str = "string", length: int | None = None, key: int | None = None):
    value = {"itemOID": name, "name": name, "label": label, "dataType": data_type}
    if length:
        value["length"] = length
    if key:
        value["keySequence"] = key
    return value


class SyntheticStudyGenerator:
    version = "2.1.0"

    def generate(self, value: dict[str, Any]) -> dict[str, Any]:
        recipe = SyntheticRecipe.model_validate(value or {})
        recipe_data = recipe.model_dump(by_alias=True)
        digest = hashlib.sha256(json.dumps(
            {"recipe": recipe_data, "generatorVersion": self.version},
            sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()
        recipe_digest = f"sha256:{digest}"
        rng = random.Random(recipe.seed)
        subjects = [{
            "id": f"{recipe.study_id}-{index:04d}", "subjid": f"{index:04d}",
            "sex": "F" if index % 2 else "M", "armcd": "PBO" if index % 2 else "TRT",
            "arm": "Placebo" if index % 2 else "Investigational Treatment", "age": rng.randint(18, 80),
        } for index in range(1, recipe.subjects + 1)]
        expected_signals: list[dict[str, Any]] = []
        if recipe.profile == "send" and recipe.scenario == "safety-signal":
            documents, expected_signals = self._send_safety_signal(
                recipe, subjects, rng, recipe_digest
            )
        else:
            documents = {
                "sdtm": self._sdtm, "send": self._send, "adam": self._adam, "tig": self._tig,
            }[recipe.profile](recipe, subjects, rng, recipe_digest)
        model = {
            domain: {
                "itemGroupOID": document["itemGroupOID"],
                "variables": document["columns"],
            }
            for domain, document in sorted(documents.items())
        }
        model_digest = "sha256:" + hashlib.sha256(json.dumps(
            {"profile": recipe.profile, "catalogVersion": SYNTHETIC_MODEL_CATALOG_VERSION, "datasets": model},
            sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()
        for document in documents.values():
            document["sourceSystem"]["modelDigest"] = model_digest
        anomalies: list[dict[str, Any]] = []
        if recipe.anomaly_rate and "AE" in documents and documents["AE"]["rows"]:
            count = max(1, round(len(documents["AE"]["rows"]) * recipe.anomaly_rate))
            names = [column["name"] for column in documents["AE"]["columns"]]
            decoded_index, subject_index = names.index("AEDECOD"), names.index("USUBJID")
            for row in documents["AE"]["rows"][:count]:
                row[decoded_index] = ""
                anomalies.append({"domain": "AE", "ruleId": "SDR.AE.AEDECOD.REQUIRED", "subjectId": row[subject_index]})
        return {
            "recipe": recipe_data, "recipeDigest": recipe_digest, "generatorVersion": self.version,
            "synthetic": True,
            "modelSource": {
                "kind": "builtin-cdisc-profile-metamodel",
                "catalogVersion": SYNTHETIC_MODEL_CATALOG_VERSION,
                "profile": recipe.profile,
                "modelDigest": model_digest,
                "datasets": model,
            },
            "watermark": {
                "generator": "kehrnel-cdisc-synthetic",
                "recipeDigest": recipe_digest,
                "modelDigest": model_digest,
            },
            "datasets": documents, "expectedAnomalies": anomalies,
            "expectedSignals": expected_signals,
        }

    def _sdtm(self, recipe, subjects, rng, digest):
        documents = {}
        if "DM" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", length=20, key=1), _column("USUBJID", "Unique Subject Identifier", length=40, key=2),
                _column("DOMAIN", "Domain", length=2), _column("SUBJID", "Subject Identifier", length=20),
                _column("SEX", "Sex", length=1), _column("AGE", "Age", "integer"), _column("AGEU", "Age Units", length=8),
                _column("ARMCD", "Arm Code", length=8), _column("ARM", "Arm", length=40),
            ]
            rows = [[recipe.study_id, s["id"], "DM", s["subjid"], s["sex"], s["age"], "YEARS", s["armcd"], s["arm"]] for s in subjects]
            documents["DM"] = self._document(recipe, "DM", "Demographics", columns, rows, digest)
        if "AE" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", length=20, key=1), _column("USUBJID", "Subject", length=40, key=2),
                _column("AESEQ", "Sequence", "integer", key=3), _column("DOMAIN", "Domain", length=2),
                _column("AETERM", "Reported Term", length=80), _column("AEDECOD", "Dictionary Term", length=80),
                _column("AESEV", "Severity", length=8), _column("AESTDY", "Start Study Day", "integer"),
            ]
            rows = []
            for subject in subjects:
                if rng.random() < 0.65:
                    term, decoded = rng.choice([("Headache", "HEADACHE"), ("Nausea", "NAUSEA"), ("Fatigue", "FATIGUE")])
                    rows.append([recipe.study_id, subject["id"], 1, "AE", term, decoded, rng.choice(["MILD", "MODERATE"]), rng.randint(1, 56)])
            documents["AE"] = self._document(recipe, "AE", "Adverse Events", columns, rows, digest)
        for domain, test_code, label, low, high in [("LB", "ALT", "Laboratory Test Results", 8.0, 55.0), ("VS", "SYSBP", "Vital Signs", 95.0, 145.0)]:
            if domain not in recipe.domains:
                continue
            columns = [
                _column("STUDYID", "Study Identifier", length=20, key=1), _column("USUBJID", "Subject", length=40, key=2),
                _column(f"{domain}SEQ", "Sequence", "integer", key=3), _column("DOMAIN", "Domain", length=2),
                _column(f"{domain}TESTCD", "Test Code", length=8), _column(f"{domain}TEST", "Test", length=60),
                _column(f"{domain}STRESN", "Numeric Result", "float"), _column(f"{domain}DY", "Study Day", "integer"),
            ]
            rows = [[recipe.study_id, s["id"], 1, domain, test_code, label, round(rng.uniform(low, high), 2), 1] for s in subjects]
            documents[domain] = self._document(recipe, domain, label, columns, rows, digest)
        return documents

    def _send(self, recipe, subjects, rng, digest):
        animals = [{**s, "group": "CTRL" if index % 2 else "HIGH"} for index, s in enumerate(subjects, 1)]
        documents = {}
        if "DM" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1), _column("USUBJID", "Animal Identifier", key=2),
                _column("DOMAIN", "Domain"), _column("SUBJID", "Subject"), _column("SEX", "Sex"),
                _column("SPECIES", "Species"), _column("STRAIN", "Strain"), _column("SPGRPCD", "Group Code"),
            ]
            rows = [[recipe.study_id, s["id"], "DM", s["subjid"], s["sex"], "RAT", "WISTAR", s["group"]] for s in animals]
            documents["DM"] = self._document(recipe, "DM", "Animal Demographics", columns, rows, digest)
        if "TX" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1), _column("SETCD", "Set Code", key=2), _column("DOMAIN", "Domain"),
                _column("TXPARMCD", "Trial Parameter"), _column("TXVAL", "Value"), _column("TXVALN", "Numeric Value", "float"), _column("TXVALU", "Unit"),
            ]
            rows = [[recipe.study_id, "CTRL", "TX", "SPGRPCD", "Vehicle", 0.0, "mg/kg/day"], [recipe.study_id, "HIGH", "TX", "SPGRPCD", "Test Article", 100.0, "mg/kg/day"]]
            documents["TX"] = self._document(recipe, "TX", "Trial Sets", columns, rows, digest)
        if "MI" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1), _column("USUBJID", "Animal", key=2), _column("MISEQ", "Sequence", "integer", key=3),
                _column("DOMAIN", "Domain"), _column("MITESTCD", "Test Code"), _column("MITEST", "Test"), _column("MISPEC", "Specimen"),
                _column("MISTRESC", "Finding"), _column("MISEV", "Severity"), _column("MIDY", "Study Day", "integer"),
            ]
            rows = []
            for subject in animals:
                if rng.random() < (0.25 if subject["group"] == "CTRL" else 0.7):
                    rows.append([recipe.study_id, subject["id"], 1, "MI", "MIFIND", "Microscopic Finding", "LIVER", "HEPATOCELLULAR HYPERTROPHY", rng.choice(["MINIMAL", "SLIGHT"]), 29])
            documents["MI"] = self._document(recipe, "MI", "Microscopic Findings", columns, rows, digest)
        if "LB" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1), _column("USUBJID", "Animal", key=2), _column("LBSEQ", "Sequence", "integer", key=3),
                _column("DOMAIN", "Domain"), _column("LBTESTCD", "Test Code"), _column("LBTEST", "Test"),
                _column("LBSTRESN", "Result", "float"), _column("LBSTRESU", "Unit"), _column("LBDY", "Study Day", "integer"),
            ]
            rows = [[recipe.study_id, s["id"], 1, "LB", "ALT", "Alanine Aminotransferase", round(rng.uniform(20, 90), 2), "U/L", 29] for s in animals]
            documents["LB"] = self._document(recipe, "LB", "Laboratory Results", columns, rows, digest)
        return documents

    def _send_safety_signal(self, recipe, subjects, rng, digest):
        """Generate a coherent dose/pathology/laboratory SEND investigation.

        The scenario provides known truth for solution and query tests: a
        treated-only thymus finding with a related lymphocyte trajectory plus
        a background lung finding. It makes no toxicologic conclusion.
        """
        dose_groups = [
            ("G1", 0.0, "Vehicle control"),
            ("G2", 4.0, "Low dose"),
            ("G3", 6.0, "Low-mid dose"),
            ("G4", 8.0, "Mid-high dose"),
            ("G5", 12.0, "High dose"),
        ]
        animals = []
        for index, subject in enumerate(subjects):
            code, dose, label = dose_groups[index % len(dose_groups)]
            animals.append({**subject, "group": code, "dose": dose, "group_label": label})

        documents: dict[str, dict[str, Any]] = {}
        if "DM" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1),
                _column("USUBJID", "Animal Identifier", key=2),
                _column("DOMAIN", "Domain"),
                _column("SUBJID", "Subject"),
                _column("SEX", "Sex"),
                _column("SPECIES", "Species"),
                _column("STRAIN", "Strain"),
                _column("SPGRPCD", "Group Code"),
            ]
            rows = [[recipe.study_id, animal["id"], "DM", animal["subjid"], animal["sex"], "RAT", "WISTAR", animal["group"]] for animal in animals]
            documents["DM"] = self._document(recipe, "DM", "Animal Demographics", columns, rows, digest)

        if "TX" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1),
                _column("SETCD", "Set Code", key=2),
                _column("DOMAIN", "Domain"),
                _column("TXPARMCD", "Trial Parameter"),
                _column("TXVAL", "Value"),
                _column("TXVALN", "Numeric Value", "float"),
                _column("TXVALU", "Unit"),
            ]
            rows = [[recipe.study_id, code, "TX", "TRTDOS", label, dose, "mg/kg/day"] for code, dose, label in dose_groups]
            documents["TX"] = self._document(recipe, "TX", "Trial Sets", columns, rows, digest)

        affected_by_group: dict[str, list[str]] = {code: [] for code, _, _ in dose_groups}
        if "MI" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1),
                _column("USUBJID", "Animal", key=2),
                _column("MISEQ", "Sequence", "integer", key=3),
                _column("DOMAIN", "Domain"),
                _column("MITESTCD", "Test Code"),
                _column("MITEST", "Test"),
                _column("MISPEC", "Specimen"),
                _column("MISTRESC", "Finding"),
                _column("MISEV", "Severity"),
                _column("MIDY", "Study Day", "integer"),
            ]
            rows = []
            probabilities = {"G1": 0.0, "G2": 0.35, "G3": 0.60, "G4": 0.75, "G5": 0.90}
            severity_by_group = {"G2": "MINIMAL", "G3": "MINIMAL", "G4": "MILD", "G5": "MODERATE"}
            for animal in animals:
                sequence = 1
                if rng.random() < probabilities[animal["group"]]:
                    affected_by_group[animal["group"]].append(animal["id"])
                    rows.append([recipe.study_id, animal["id"], sequence, "MI", "MIFIND", "Microscopic Finding", "THYMUS", "DECREASED LYMPHOCYTES, CORTEX", severity_by_group[animal["group"]], 29])
                    sequence += 1
                if rng.random() < 0.55:
                    rows.append([recipe.study_id, animal["id"], sequence, "MI", "MIFIND", "Microscopic Finding", "LUNG", "MONONUCLEAR CELL INFILTRATION", "MINIMAL", 29])
            documents["MI"] = self._document(recipe, "MI", "Microscopic Findings", columns, rows, digest)

        if "LB" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1),
                _column("USUBJID", "Animal", key=2),
                _column("LBSEQ", "Sequence", "integer", key=3),
                _column("DOMAIN", "Domain"),
                _column("LBTESTCD", "Test Code"),
                _column("LBTEST", "Test"),
                _column("LBSTRESN", "Result", "float"),
                _column("LBSTRESU", "Unit"),
                _column("LBDY", "Study Day", "integer"),
            ]
            rows = []
            days = [-14, 8, 22, 29]
            for animal in animals:
                for sequence, day in enumerate(days, start=1):
                    baseline = 7.2 + rng.uniform(-0.7, 0.7)
                    exposure = max(day, 0) / 29.0
                    treatment_effect = (animal["dose"] / 12.0) * 3.7 * exposure
                    value = max(0.5, baseline - treatment_effect + rng.uniform(-0.35, 0.35))
                    rows.append([recipe.study_id, animal["id"], sequence, "LB", "LYM", "Lymphocytes", round(value, 2), "10^9/L", day])
            documents["LB"] = self._document(recipe, "LB", "Laboratory Results", columns, rows, digest)

        expected_signals = [{
            "signalId": "thymus-lymphocyte-decrease",
            "kind": "cross-domain-safety-signal",
            "profile": "send",
            "finding": "DECREASED LYMPHOCYTES, CORTEX",
            "specimen": "THYMUS",
            "correlatedTestCode": "LYM",
            "controlIncidence": len(affected_by_group["G1"]),
            "treatedIncidence": sum(len(affected_by_group[code]) for code in ("G2", "G3", "G4", "G5")),
            "groupAnimalIds": affected_by_group,
            "expectedQueryPath": ["TX", "DM", "MI", "LB"],
            "interpretation": "synthetic review hypothesis only",
        }]
        return documents, expected_signals

    def _adam(self, recipe, subjects, rng, digest):
        documents = {}
        if "ADSL" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1), _column("USUBJID", "Subject", key=2), _column("SUBJID", "Subject"),
                _column("SEX", "Sex"), _column("AGE", "Age", "integer"), _column("TRT01P", "Planned Treatment"),
                _column("SAFFL", "Safety Population"), _column("ITTFL", "ITT Population"),
            ]
            rows = [[recipe.study_id, s["id"], s["subjid"], s["sex"], s["age"], s["arm"], "Y", "Y"] for s in subjects]
            documents["ADSL"] = self._document(recipe, "ADSL", "Subject-Level Analysis", columns, rows, digest)
        if "ADAE" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1), _column("USUBJID", "Subject", key=2), _column("ASEQ", "Analysis Sequence", "integer", key=3),
                _column("PARAMCD", "Parameter Code"), _column("PARAM", "Parameter"), _column("AVALC", "Analysis Value"),
                _column("ADY", "Analysis Day", "integer"), _column("SRCDOM", "Source Domain"), _column("SRCVAR", "Source Variable"), _column("SRCSEQ", "Source Sequence", "integer"),
            ]
            rows = [[recipe.study_id, s["id"], 1, "AETERM", "Treatment-emergent adverse event", rng.choice(["HEADACHE", "NAUSEA"]), rng.randint(1, 56), "AE", "AESEQ", 1] for s in subjects if rng.random() < 0.65]
            documents["ADAE"] = self._document(recipe, "ADAE", "Adverse Event Analysis", columns, rows, digest)
        if "ADLB" in recipe.domains:
            columns = [
                _column("STUDYID", "Study Identifier", key=1), _column("USUBJID", "Subject", key=2), _column("ASEQ", "Analysis Sequence", "integer", key=3),
                _column("PARAMCD", "Parameter Code"), _column("PARAM", "Parameter"), _column("AVAL", "Analysis Value", "float"),
                _column("AVALU", "Unit"), _column("ADY", "Analysis Day", "integer"), _column("ANL01FL", "Analysis Record Flag"),
            ]
            rows = [[recipe.study_id, s["id"], 1, "ALT", "Alanine Aminotransferase", round(rng.uniform(8, 55), 2), "U/L", 1, "Y"] for s in subjects]
            documents["ADLB"] = self._document(recipe, "ADLB", "Laboratory Analysis", columns, rows, digest)
        return documents

    def _tig(self, recipe, subjects, rng, digest):
        product_id = f"{recipe.study_id}-PRODUCT"
        documents = {}
        if "PROD" in recipe.domains:
            columns = [_column("STUDYID", "Study Identifier", key=1), _column("PRODUCTID", "Product Identifier", key=2), _column("DOMAIN", "Domain"), _column("PRODUCT", "Product Name"), _column("CONSTITUENT", "Constituent")]
            documents["PROD"] = self._document(recipe, "PROD", "Product Description", columns, [[recipe.study_id, product_id, "PROD", "Synthetic Investigational Product", "ACTIVE SUBSTANCE"]], digest)
        if "BATCH" in recipe.domains:
            columns = [_column("STUDYID", "Study Identifier", key=1), _column("BATCHID", "Batch Identifier", key=2), _column("DOMAIN", "Domain"), _column("PRODUCTID", "Product Identifier"), _column("LOTNO", "Lot Number")]
            documents["BATCH"] = self._document(recipe, "BATCH", "Product Batch", columns, [[recipe.study_id, f"{product_id}-B01", "BATCH", product_id, "LOT-0001"]], digest)
        if "EVID" in recipe.domains:
            columns = [_column("STUDYID", "Study Identifier", key=1), _column("EVIDID", "Evidence Identifier", key=2), _column("DOMAIN", "Domain"), _column("PRODUCTID", "Product Identifier"), _column("EVIDTYPE", "Evidence Type"), _column("EVIDVAL", "Evidence Value", "float")]
            rows = [[recipe.study_id, f"EVID-{index:04d}", "EVID", product_id, rng.choice(["NONCLINICAL", "INDIVIDUAL_HEALTH", "POPULATION_HEALTH"]), round(rng.uniform(0.1, 1.0), 3)] for index in range(1, max(2, recipe.subjects // 4) + 1)]
            documents["EVID"] = self._document(recipe, "EVID", "Product Evidence", columns, rows, digest)
        return documents

    def _document(self, recipe, domain, label, columns, rows, recipe_digest):
        return {
            "datasetJSONCreationDateTime": "2000-01-01T00:00:00Z", "datasetJSONVersion": "1.1.0",
            "fileOID": f"{recipe.study_id}.{domain.lower()}", "studyOID": recipe.study_id,
            "metaDataVersionOID": f"SYNTHETIC.{recipe.profile.upper()}.1", "itemGroupOID": domain,
            "records": len(rows), "name": domain, "label": label, "columns": columns, "rows": rows,
            "sourceSystem": {"name": "kehrnel-cdisc-synthetic", "version": self.version, "recipeDigest": recipe_digest},
        }
