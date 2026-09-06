"""Patient-centred synthetic cohort blueprints for the FHIR Clinical CDR.

The blueprint layer describes intent and longitudinal distributions.  It does not
replace fhir-gen's release schemas, resource generators, or terminology assets.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import random
import re
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Any, Iterator

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr._paths import SPEC_DIR
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge

BLUEPRINTS_PATH = SPEC_DIR / "cohort_blueprints.json"
BLUEPRINT_SCHEMA_PATH = SPEC_DIR / "cohort_blueprint.schema.json"
BLUEPRINT_CONTRACT_VERSION = "fhir-cohort-blueprint/v1"
GENERATION_EVIDENCE_VERSION = "fhir-cohort-evidence/v1"
MAX_COHORT_PATIENTS = 10_000
MAX_PLANNED_RESOURCES = 500_000

_RELATIVE_REFERENCE = re.compile(r"^([A-Z][A-Za-z0-9]*)/([A-Za-z0-9\-.]{1,64})$")
_SUPPORTED_RULES = frozenset(
    {
        "longitudinal-dates-v1",
        "blood-pressure-panel-v1",
        "cardiometabolic-condition-v1",
        "oncology-pathway-v1",
        "financial-chain-v1",
    }
)
_SHARED_SAFE_RESOURCE_TYPES = frozenset(
    {
        "BiologicallyDerivedProduct",
        "ChargeItemDefinition",
        "Device",
        "Endpoint",
        "InsurancePlan",
        "Location",
        "Measure",
        "Medication",
        "Organization",
        "Practitioner",
        "Questionnaire",
        "Substance",
    }
)


def _invalid(message: str, **details: Any) -> KehrnelError:
    return KehrnelError(
        code="FHIR_COHORT_BLUEPRINT_INVALID",
        status=400,
        message=message,
        details=details,
    )


@lru_cache(maxsize=1)
def _library() -> dict[str, Any]:
    try:
        raw = json.loads(BLUEPRINTS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise KehrnelError(
            code="FHIR_COHORT_CATALOG_UNAVAILABLE",
            status=500,
            message="The bundled FHIR cohort catalog could not be loaded",
            details={"path": str(BLUEPRINTS_PATH), "error": str(exc)},
        ) from exc
    if raw.get("contract_version") != BLUEPRINT_CONTRACT_VERSION:
        raise KehrnelError(
            code="FHIR_COHORT_CATALOG_UNAVAILABLE",
            status=500,
            message="The bundled FHIR cohort catalog uses an unsupported contract",
            details={"contract_version": raw.get("contract_version")},
        )
    assets = raw.get("assets")
    if not isinstance(assets, list) or not assets:
        raise KehrnelError(
            code="FHIR_COHORT_CATALOG_UNAVAILABLE",
            status=500,
            message="The bundled FHIR cohort catalog contains no assets",
        )
    for asset in assets:
        _validate_blueprint(asset)
    return raw


def _weighted_choice(rng: random.Random, values: list[tuple[Any, float]]) -> Any:
    total = sum(weight for _, weight in values)
    if total <= 0:
        raise _invalid("A cohort distribution must contain a positive weight")
    marker = rng.random() * total
    running = 0.0
    for value, weight in values:
        running += weight
        if marker <= running:
            return value
    return values[-1][0]


def _validate_resource_type(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z][A-Za-z0-9]{0,63}", value):
        raise _invalid("Invalid FHIR resource type", field=field, value=value)
    return value


def _validate_resource_spec(resource_type: str, value: Any) -> None:
    if not isinstance(value, dict):
        raise _invalid(
            "Per-patient resource distribution must be an object",
            resource_type=resource_type,
        )
    minimum = value.get("min")
    maximum = value.get("max")
    probability = value.get("probability", 1.0)
    if (
        not isinstance(minimum, int)
        or isinstance(minimum, bool)
        or not isinstance(maximum, int)
        or isinstance(maximum, bool)
        or minimum < 0
        or maximum < minimum
    ):
        raise _invalid(
            "Resource distribution requires integers with 0 <= min <= max",
            resource_type=resource_type,
            distribution=value,
        )
    if (
        not isinstance(probability, (int, float))
        or isinstance(probability, bool)
        or not 0 <= float(probability) <= 1
    ):
        raise _invalid(
            "Resource distribution probability must be between 0 and 1",
            resource_type=resource_type,
            probability=probability,
        )


def _validate_blueprint(blueprint: Any) -> None:
    if not isinstance(blueprint, dict):
        raise _invalid("A cohort blueprint must be an object")
    from jsonschema import Draft7Validator

    try:
        contract_schema = json.loads(
            BLUEPRINT_SCHEMA_PATH.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise KehrnelError(
            code="FHIR_COHORT_CATALOG_UNAVAILABLE",
            status=500,
            message="The FHIR cohort blueprint schema could not be loaded",
            details={"path": str(BLUEPRINT_SCHEMA_PATH), "error": str(exc)},
        ) from exc
    contract_errors = sorted(
        Draft7Validator(contract_schema).iter_errors(blueprint),
        key=lambda error: list(error.absolute_path),
    )
    if contract_errors:
        error = contract_errors[0]
        raise _invalid(
            "Cohort blueprint does not satisfy the public contract",
            path=".".join(str(part) for part in error.absolute_path) or "$",
            validation_message=error.message,
        )
    blueprint_id = blueprint.get("id")
    if not isinstance(blueprint_id, str) or not re.fullmatch(
        r"[a-z][a-z0-9-]{1,63}", blueprint_id
    ):
        raise _invalid("A cohort blueprint requires a stable lowercase id")
    defaults = blueprint.get("defaults")
    if not isinstance(defaults, dict):
        raise _invalid("A cohort blueprint requires defaults", blueprint_id=blueprint_id)
    patients = defaults.get("patients")
    years = defaults.get("history_years")
    if not isinstance(patients, int) or isinstance(patients, bool) or not 1 <= patients <= MAX_COHORT_PATIENTS:
        raise _invalid("Default patient count is outside the supported range", blueprint_id=blueprint_id)
    if not isinstance(years, int) or isinstance(years, bool) or not 1 <= years <= 20:
        raise _invalid("Default history length must be between 1 and 20 years", blueprint_id=blueprint_id)
    try:
        date.fromisoformat(str(defaults.get("reference_date")))
    except ValueError as exc:
        raise _invalid("Default reference_date must be ISO YYYY-MM-DD", blueprint_id=blueprint_id) from exc
    for resource_type, count in (blueprint.get("shared_resources") or {}).items():
        _validate_resource_type(resource_type, field="shared_resources")
        if resource_type not in _SHARED_SAFE_RESOURCE_TYPES:
            raise _invalid(
                "Shared resources must not depend on a patient",
                resource_type=resource_type,
                allowed=sorted(_SHARED_SAFE_RESOURCE_TYPES),
            )
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise _invalid("Shared resource count must be a non-negative integer", resource_type=resource_type)
    per_patient = blueprint.get("per_patient_resources")
    if not isinstance(per_patient, dict) or not per_patient:
        raise _invalid("A cohort blueprint requires per_patient_resources", blueprint_id=blueprint_id)
    for resource_type, spec in per_patient.items():
        _validate_resource_type(resource_type, field="per_patient_resources")
        if resource_type == "Patient":
            raise _invalid(
                "Patient is implicit and cannot appear in per_patient_resources",
                blueprint_id=blueprint_id,
            )
        _validate_resource_spec(resource_type, spec)
    population = blueprint.get("population") or {}
    age_bands = population.get("age_bands")
    genders = population.get("gender_distribution")
    if not isinstance(age_bands, list) or not age_bands:
        raise _invalid("A cohort blueprint requires at least one age band", blueprint_id=blueprint_id)
    for band in age_bands:
        if not isinstance(band, dict) or not 0 <= band.get("min", -1) <= band.get("max", -1) <= 120 or float(band.get("weight", 0)) <= 0:
            raise _invalid("Invalid age-band distribution", blueprint_id=blueprint_id, age_band=band)
    if not isinstance(genders, dict) or not genders or sum(float(v) for v in genders.values()) <= 0:
        raise _invalid("A cohort blueprint requires a positive gender distribution", blueprint_id=blueprint_id)
    releases = blueprint.get("fhir_releases")
    if not isinstance(releases, list) or not releases or any(release not in {"R5", "R6"} for release in releases):
        raise _invalid("Cohort blueprints currently support R5 and R6", blueprint_id=blueprint_id)
    rules = blueprint.get("clinical_rules")
    if not isinstance(rules, list):
        raise _invalid("clinical_rules must be an array", blueprint_id=blueprint_id)
    unknown_rules = sorted(
        str(rule.get("id"))
        for rule in rules
        if not isinstance(rule, dict) or rule.get("id") not in _SUPPORTED_RULES
    )
    if unknown_rules:
        raise _invalid("Cohort blueprint contains unsupported rules", blueprint_id=blueprint_id, rules=unknown_rules)


def _asset_by_id(blueprint_id: str) -> dict[str, Any]:
    for asset in _library()["assets"]:
        if asset["id"] == blueprint_id:
            return copy.deepcopy(asset)
    raise KehrnelError(
        code="FHIR_COHORT_BLUEPRINT_NOT_FOUND",
        status=404,
        message=f"FHIR cohort blueprint '{blueprint_id}' was not found",
        details={"available": sorted(item["id"] for item in _library()["assets"])},
    )


def _cohort_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = payload or {}
    cohort = payload.get("cohort")
    if cohort is None:
        cohort = payload
    if not isinstance(cohort, dict):
        raise _invalid("cohort must be an object")
    return cohort


def _resolve_blueprint(payload: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any]]:
    cohort = _cohort_payload(payload)
    inline = cohort.get("blueprint")
    if inline is not None:
        blueprint = copy.deepcopy(inline)
        _validate_blueprint(blueprint)
    else:
        blueprint_id = cohort.get("blueprint_id")
        if not isinstance(blueprint_id, str) or not blueprint_id.strip():
            raise _invalid("cohort.blueprint_id is required")
        blueprint = _asset_by_id(blueprint_id.strip())

    shared_overrides = cohort.get("shared_resources") or {}
    if not isinstance(shared_overrides, dict):
        raise _invalid("cohort.shared_resources must be an object")
    for resource_type, count in shared_overrides.items():
        _validate_resource_type(resource_type, field="cohort.shared_resources")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise _invalid("Shared resource override must be a non-negative integer", resource_type=resource_type)
        blueprint.setdefault("shared_resources", {})[resource_type] = count

    distribution_overrides = cohort.get("per_patient_resources") or {}
    if not isinstance(distribution_overrides, dict):
        raise _invalid("cohort.per_patient_resources must be an object")
    for resource_type, spec in distribution_overrides.items():
        _validate_resource_type(resource_type, field="cohort.per_patient_resources")
        _validate_resource_spec(resource_type, spec)
        blueprint.setdefault("per_patient_resources", {})[resource_type] = copy.deepcopy(spec)

    if "population" in cohort:
        population_override = cohort["population"]
        if not isinstance(population_override, dict):
            raise _invalid("cohort.population must be an object")
        blueprint["population"] = copy.deepcopy(population_override)
    if "clinical_rules" in cohort:
        rules_override = cohort["clinical_rules"]
        if not isinstance(rules_override, list):
            raise _invalid("cohort.clinical_rules must be an array")
        blueprint["clinical_rules"] = copy.deepcopy(rules_override)
    _validate_blueprint(blueprint)
    return blueprint, cohort


def resolve_cohort_blueprint(
    payload: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a validated, copied blueprint and its request overrides."""
    return _resolve_blueprint(payload)


def _seed_salt(blueprint_id: str) -> int:
    return int(hashlib.sha256(blueprint_id.encode("utf-8")).hexdigest()[:12], 16)


def iter_patient_resource_counts(
    blueprint: dict[str, Any], patients: int, seed: int
) -> Iterator[dict[str, int]]:
    rng = random.Random(seed ^ _seed_salt(str(blueprint["id"])))
    distributions = blueprint["per_patient_resources"]
    for _ in range(patients):
        counts: dict[str, int] = {}
        for resource_type in sorted(distributions):
            spec = distributions[resource_type]
            if rng.random() > float(spec.get("probability", 1.0)):
                count = 0
            else:
                count = rng.randint(int(spec["min"]), int(spec["max"]))
            counts[resource_type] = count
        yield counts


def _plan_digest(plan: dict[str, Any]) -> str:
    encoded = json.dumps(plan, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def compile_cohort_plan(
    ctx: StrategyContext, payload: dict[str, Any] | None
) -> dict[str, Any]:
    cfg = bridge.resolve_strategy_config(ctx)
    blueprint, cohort = _resolve_blueprint(payload)
    release = str(cohort.get("schema_version") or cfg.get("schema_version") or "R5").upper()
    if release not in blueprint["fhir_releases"]:
        raise KehrnelError(
            code="FHIR_COHORT_RELEASE_UNSUPPORTED",
            status=400,
            message=f"Blueprint '{blueprint['id']}' does not support {release}",
            details={"supported": blueprint["fhir_releases"]},
        )

    from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.capabilities import (
        resolve_resource_capabilities,
    )
    from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.query import (
        build_search_converter,
    )

    capability_sets = resolve_resource_capabilities(
        cfg, build_search_converter(ctx).config_loader
    )
    blueprint_resource_types = (
        set(blueprint.get("shared_resources") or {})
        | set(blueprint.get("per_patient_resources") or {})
        | {"Patient"}
    )
    unsupported_generation = sorted(
        blueprint_resource_types - set(capability_sets.generatable)
    )
    if unsupported_generation:
        raise _invalid(
            "Cohort blueprint uses resource types unavailable to the active release generator",
            release=release,
            resource_types=unsupported_generation,
        )
    preview_only_types = sorted(
        blueprint_resource_types - set(capability_sets.synthetic_writable)
    )

    defaults = blueprint["defaults"]
    patients = cohort.get("patients", defaults["patients"])
    history_years = cohort.get("history_years", defaults["history_years"])
    seed = cohort.get("seed", defaults["seed"])
    reference_date = cohort.get("reference_date", defaults["reference_date"])
    if not isinstance(patients, int) or isinstance(patients, bool) or not 1 <= patients <= MAX_COHORT_PATIENTS:
        raise _invalid("patients must be between 1 and 10000", patients=patients)
    if not isinstance(history_years, int) or isinstance(history_years, bool) or not 1 <= history_years <= 20:
        raise _invalid("history_years must be between 1 and 20", history_years=history_years)
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise _invalid("seed must be an integer", seed=seed)
    try:
        date.fromisoformat(str(reference_date))
    except ValueError as exc:
        raise _invalid("reference_date must be ISO YYYY-MM-DD", reference_date=reference_date) from exc

    patient_totals: Counter[str] = Counter()
    per_patient_samples: dict[str, list[int]] = {
        resource_type: [] for resource_type in blueprint["per_patient_resources"]
    }
    for counts in iter_patient_resource_counts(blueprint, patients, seed):
        patient_totals.update(counts)
        for resource_type, count in counts.items():
            per_patient_samples[resource_type].append(count)

    shared = {key: int(value) for key, value in blueprint["shared_resources"].items() if int(value) > 0}
    planned: Counter[str] = Counter(shared)
    planned.update(patient_totals)
    planned["Patient"] += patients
    total = sum(planned.values())
    if total > MAX_PLANNED_RESOURCES:
        raise KehrnelError(
            code="FHIR_COHORT_PLAN_TOO_LARGE",
            status=400,
            message="The cohort exceeds the safe planned resource limit",
            details={"planned_resources": total, "maximum": MAX_PLANNED_RESOURCES},
        )

    distributions = []
    for resource_type in sorted(per_patient_samples):
        values = per_patient_samples[resource_type]
        distributions.append(
            {
                "resource_type": resource_type,
                "configured": copy.deepcopy(blueprint["per_patient_resources"][resource_type]),
                "planned_total": patient_totals[resource_type],
                "planned_per_patient": {
                    "min": min(values),
                    "max": max(values),
                    "mean": round(sum(values) / patients, 3),
                },
            }
        )

    plan: dict[str, Any] = {
        "contract_version": BLUEPRINT_CONTRACT_VERSION,
        "blueprint": {
            key: copy.deepcopy(blueprint.get(key))
            for key in (
                "id",
                "version",
                "title",
                "description",
                "purpose",
                "maturity",
                "tags",
                "learning_objectives",
                "disclaimer",
            )
        },
        "release": release,
        "cohort": {
            "patients": patients,
            "history_years": history_years,
            "reference_date": str(reference_date),
            "seed": seed,
        },
        "population": copy.deepcopy(blueprint["population"]),
        "shared_resources": shared,
        "per_patient_distributions": distributions,
        "clinical_rules": copy.deepcopy(blueprint["clinical_rules"]),
        "execution": {
            "generatable": True,
            "persistable": not preview_only_types,
            "preview_only_resource_types": preview_only_types,
        },
        "planned": dict(sorted(planned.items())),
        "planned_total_resources": total,
        "quality_contract": {
            "count_fidelity": "reported",
            "relative_reference_integrity": "checked",
            "patient_linkage": "checked-for-blueprint-resource-types",
            "longitudinal_dates": "deterministic-within-requested-history",
            "clinical_rules": "reported-with-measurements",
            "profile_validation": "not-implemented-base-schema-only",
            "epidemiological_validity": "not-claimed",
        },
        "limitations": [
            "Curated demonstration cohorts are not epidemiological or actuarial simulations.",
            "FHIR schema validity and active import validation do not establish clinical correctness.",
            "Custom profiles can further constrain generated resources and may require customer-specific rules.",
            "Configured profile selection is discoverable but is not yet enforced during generation or import.",
        ],
    }
    plan["plan_digest"] = _plan_digest(plan)
    return plan


def fhir_cohort_catalog(
    ctx: StrategyContext, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    cfg = bridge.resolve_strategy_config(ctx)
    release = str((payload or {}).get("schema_version") or cfg.get("schema_version") or "R5").upper()
    include_blueprints = bool((payload or {}).get("include_blueprints", True))
    assets: list[dict[str, Any]] = []
    for item in _library()["assets"]:
        if release not in item["fhir_releases"]:
            continue
        summary = copy.deepcopy(item) if include_blueprints else {
            key: copy.deepcopy(item.get(key))
            for key in ("id", "version", "title", "description", "purpose", "maturity", "tags", "defaults", "fhir_releases")
        }
        assets.append(summary)
    return {
        "ok": True,
        "contract_version": BLUEPRINT_CONTRACT_VERSION,
        "release": release,
        "asset_count": len(assets),
        "assets": assets,
        "generation_levels": {
            "structural": "Generated from the selected FHIR release schema.",
            "enriched": "Uses resource enrichers and terminology-backed values.",
            "curated-demo": "Adds a deterministic patient cohort, longitudinal rules, and measured quality evidence.",
        },
        "custom_blueprints_supported": True,
        "maximum_patients": MAX_COHORT_PATIENTS,
        "maximum_planned_resources": MAX_PLANNED_RESOURCES,
    }


def fhir_cohort_plan(
    ctx: StrategyContext, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {"ok": True, **compile_cohort_plan(ctx, payload)}


def _iso_datetime(value: date, hour: int = 12) -> str:
    return datetime.combine(value, time(hour=hour), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _reference(resource: dict[str, Any]) -> dict[str, str]:
    resource_type = str(resource["resourceType"])
    resource_id = str(resource["id"])
    return {"reference": f"{resource_type}/{resource_id}", "type": resource_type}


def _patient_age_and_gender(
    patient: dict[str, Any], blueprint: dict[str, Any], reference_date: date, rng: random.Random
) -> tuple[int, str]:
    population = blueprint["population"]
    age_band = _weighted_choice(
        rng,
        [(band, float(band["weight"])) for band in population["age_bands"]],
    )
    age = rng.randint(int(age_band["min"]), int(age_band["max"]))
    gender = str(
        _weighted_choice(
            rng,
            [(key, float(weight)) for key, weight in population["gender_distribution"].items()],
        )
    )
    try:
        birthday = reference_date.replace(year=reference_date.year - age)
    except ValueError:
        # 29 February cannot be represented in every target birth year.
        birthday = reference_date.replace(
            year=reference_date.year - age, month=2, day=28
        )
    birthday -= timedelta(days=rng.randint(0, 364))
    patient["birthDate"] = birthday.isoformat()
    patient["gender"] = gender
    patient["active"] = True
    return age, gender


def _dated_resources(resources: list[dict[str, Any]], start: date, end: date, rng: random.Random) -> None:
    span = max(1, (end - start).days)
    dated = [resource for resource in resources if resource.get("resourceType") != "Patient"]
    offsets = sorted(rng.randint(0, span) for _ in dated)
    for resource, offset in zip(dated, offsets):
        event_date = start + timedelta(days=offset)
        stamp = _iso_datetime(event_date, rng.randint(8, 17))
        resource_type = resource.get("resourceType")
        if resource_type == "Encounter":
            resource["status"] = "completed"
            resource["actualPeriod"] = {
                "start": stamp,
                "end": _iso_datetime(min(end, event_date + timedelta(days=rng.randint(0, 3))), rng.randint(12, 20)),
            }
        elif resource_type == "Condition":
            resource["onsetDateTime"] = stamp
            resource["recordedDate"] = stamp
        elif resource_type == "Observation":
            resource["effectiveDateTime"] = stamp
            resource["issued"] = stamp
        elif resource_type == "Procedure":
            resource.pop("occurredDateTime", None)
            resource["occurrenceDateTime"] = stamp
        elif resource_type == "DiagnosticReport":
            resource["effectiveDateTime"] = stamp
            resource["issued"] = stamp
        elif resource_type in {"MedicationRequest", "ServiceRequest"}:
            resource["authoredOn"] = stamp
        elif resource_type == "MedicationAdministration":
            temporal_field = "occurenceDateTime" if any(
                key.startswith("occurence") for key in resource
            ) else "occurrenceDateTime"
            for key in list(resource):
                if key.startswith("occurred") or key.startswith("occurrence") or key.startswith("occurence"):
                    resource.pop(key, None)
            resource[temporal_field] = stamp
        elif resource_type in {
            "Claim",
            "ClaimResponse",
            "ExplanationOfBenefit",
            "CoverageEligibilityRequest",
            "CoverageEligibilityResponse",
        }:
            resource["created"] = stamp
        elif resource_type == "Coverage":
            resource["status"] = "active"
            resource["period"] = {"start": start.isoformat(), "end": end.isoformat()}
        elif resource_type == "CarePlan":
            resource["period"] = {"start": event_date.isoformat(), "end": end.isoformat()}


def _coding(system: str, code: str, display: str) -> dict[str, Any]:
    return {"coding": [{"system": system, "code": code, "display": display}], "text": display}


def _apply_cardiometabolic_rules(
    resources: list[dict[str, Any]], rules: dict[str, dict[str, Any]], age: int, rng: random.Random
) -> dict[str, int]:
    evidence: Counter[str] = Counter()
    condition_rule = rules.get("cardiometabolic-condition-v1")
    elevated = bool(condition_rule and rng.random() < float(condition_rule.get("prevalence", 0.5)))
    conditions = [item for item in resources if item.get("resourceType") == "Condition"]
    if elevated and conditions:
        conditions[0]["code"] = _coding("http://snomed.info/sct", "38341003", "Hypertensive disorder")
        evidence["cardiometabolic_conditions"] += 1

    bp_rule = rules.get("blood-pressure-panel-v1")
    observations = [item for item in resources if item.get("resourceType") == "Observation"]
    if not bp_rule or not observations:
        return dict(evidence)
    count = max(
        1,
        min(
            len(observations),
            int(math.ceil(len(observations) * float(bp_rule.get("fraction", 0.35)))),
        ),
    )
    selected = list(observations[:count])
    for observation in observations:
        codings = (observation.get("code") or {}).get("coding") or []
        if any(
            isinstance(coding, dict) and coding.get("code") == "85354-9"
            for coding in codings
        ) and observation not in selected:
            selected.append(observation)
    for observation in selected:
        systolic = round(max(80, min(210, 106 + 0.32 * max(0, age - 30) + (18 if elevated else 0) + rng.gauss(0, 7))))
        diastolic = round(max(45, min(125, 66 + 0.16 * max(0, age - 30) + (9 if elevated else 0) + rng.gauss(0, 5))))
        systolic = max(systolic, diastolic + 20)
        for key in list(observation):
            if key.startswith("value"):
                observation.pop(key, None)
        observation["status"] = "final"
        observation["category"] = [
            _coding("http://terminology.hl7.org/CodeSystem/observation-category", "vital-signs", "Vital Signs")
        ]
        observation["code"] = _coding("http://loinc.org", "85354-9", "Blood pressure panel")
        observation["component"] = [
            {
                "code": _coding("http://loinc.org", "8480-6", "Systolic blood pressure"),
                "valueQuantity": {"value": systolic, "unit": "mmHg", "system": "http://unitsofmeasure.org", "code": "mm[Hg]"},
                "referenceRange": [{"low": {"value": 90, "unit": "mmHg"}, "high": {"value": 140, "unit": "mmHg"}}],
            },
            {
                "code": _coding("http://loinc.org", "8462-4", "Diastolic blood pressure"),
                "valueQuantity": {"value": diastolic, "unit": "mmHg", "system": "http://unitsofmeasure.org", "code": "mm[Hg]"},
                "referenceRange": [{"low": {"value": 60, "unit": "mmHg"}, "high": {"value": 90, "unit": "mmHg"}}],
            },
        ]
        evidence["blood_pressure_panels"] += 1
    return dict(evidence)


def _apply_oncology_rule(resources: list[dict[str, Any]]) -> dict[str, int]:
    evidence: Counter[str] = Counter()
    conditions = [item for item in resources if item.get("resourceType") == "Condition"]
    if conditions:
        conditions[0]["code"] = _coding("http://snomed.info/sct", "363346000", "Malignant neoplastic disease")
        evidence["oncology_conditions"] += 1
    reports = [item for item in resources if item.get("resourceType") == "DiagnosticReport"]
    for report in reports:
        report["status"] = "final"
        report["code"] = _coding("http://loinc.org", "60568-3", "Pathology synoptic report")
        report["conclusion"] = "Synthetic pathology result for demonstration; not for clinical use."
        evidence["pathology_reports"] += 1
    procedures = [item for item in resources if item.get("resourceType") == "Procedure"]
    if procedures:
        procedures[0]["status"] = "completed"
        procedures[0]["code"] = _coding("http://snomed.info/sct", "86273004", "Biopsy")
        evidence["biopsy_procedures"] += 1
    return dict(evidence)


def _apply_financial_rule(resources: list[dict[str, Any]]) -> dict[str, int]:
    evidence: Counter[str] = Counter()
    claims = [item for item in resources if item.get("resourceType") == "Claim"]
    coverages = [item for item in resources if item.get("resourceType") == "Coverage"]
    for coverage in coverages:
        coverage["status"] = "active"
    for index, response in enumerate(item for item in resources if item.get("resourceType") == "ClaimResponse"):
        if claims:
            response["request"] = _reference(claims[index % len(claims)])
            evidence["claim_response_links"] += 1
    for index, explanation in enumerate(item for item in resources if item.get("resourceType") == "ExplanationOfBenefit"):
        if claims:
            explanation["claim"] = _reference(claims[index % len(claims)])
            evidence["explanation_of_benefit_links"] += 1
        if coverages:
            explanation["insurance"] = [{"focal": True, "coverage": _reference(coverages[index % len(coverages)])}]
    return dict(evidence)


def apply_patient_rules(
    resources: list[dict[str, Any]],
    blueprint: dict[str, Any],
    *,
    patient_index: int,
    seed: int,
    history_years: int,
    reference_date: str,
) -> dict[str, Any]:
    """Apply deterministic cohort semantics to one patient's resource graph."""
    rng = random.Random(seed + (patient_index + 1) * 104_729 + _seed_salt(blueprint["id"]))
    end = date.fromisoformat(reference_date)
    start = end - timedelta(days=history_years * 365)
    patients = [item for item in resources if item.get("resourceType") == "Patient"]
    if not patients:
        raise KehrnelError(
            code="FHIR_COHORT_GENERATION_FAILED",
            status=500,
            message="Patient-centred generation produced no Patient resource",
        )
    age, gender = _patient_age_and_gender(patients[0], blueprint, end, rng)
    rules = {str(rule["id"]): rule for rule in blueprint["clinical_rules"]}
    evidence: Counter[str] = Counter()
    if "longitudinal-dates-v1" in rules:
        _dated_resources(resources, start, end, rng)
        evidence["longitudinal_resources"] += max(0, len(resources) - 1)
    if "blood-pressure-panel-v1" in rules or "cardiometabolic-condition-v1" in rules:
        evidence.update(_apply_cardiometabolic_rules(resources, rules, age, rng))
    if "oncology-pathway-v1" in rules:
        evidence.update(_apply_oncology_rule(resources))
    if "financial-chain-v1" in rules:
        evidence.update(_apply_financial_rule(resources))
    return {"age": age, "gender": gender, "measurements": dict(evidence)}


def _remove_at_path(document: Any, path: list[Any]) -> bool:
    if not path:
        return False
    parent = document
    for part in path[:-1]:
        try:
            parent = parent[part]
        except (KeyError, IndexError, TypeError):
            return False
    key = path[-1]
    if isinstance(parent, dict) and key in parent:
        parent.pop(key, None)
        return True
    if isinstance(parent, list) and isinstance(key, int) and 0 <= key < len(parent):
        parent.pop(key)
        return True
    return False


def conform_resources_to_base_schema(
    resources: list[dict[str, Any]], release: str
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Remove invalid optional generator output before the import boundary.

    fhir-gen owns schema coverage and enrichment.  This conservative cohort-only
    pass never invents missing required content: it removes optional fields or
    optional backbone entries rejected by the selected bundled base schema and
    reports every removal.  A remaining required/root error fails closed later at
    the normal import boundary.
    """
    from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.validation import (
        _base_validator,
    )

    curated = copy.deepcopy(resources)
    removals: Counter[str] = Counter()
    unresolved: list[dict[str, Any]] = []
    for resource in curated:
        resource_type = str(resource.get("resourceType") or "")
        validator = _base_validator(release, resource_type)
        if validator is None:
            unresolved.append(
                {
                    "resource_type": resource_type,
                    "resource_id": resource.get("id"),
                    "path": "$",
                    "message": "Resource type is absent from the selected base schema",
                }
            )
            continue
        for _ in range(1_000):
            errors = sorted(
                validator.iter_errors(resource),
                key=lambda error: len(list(error.absolute_path)),
                reverse=True,
            )
            if not errors:
                break
            error = errors[0]
            path = list(error.absolute_path)
            removed_paths: list[str] = []
            if error.validator == "additionalProperties" and isinstance(
                error.instance, dict
            ):
                allowed = set((error.schema.get("properties") or {}).keys())
                extra = sorted(set(error.instance) - allowed)
                for key in extra:
                    error.instance.pop(key, None)
                    removed_paths.append(
                        ".".join(str(part) for part in [*path, key]) or "$"
                    )
            elif error.validator == "required" and path:
                if _remove_at_path(resource, path):
                    removed_paths.append(
                        ".".join(str(part) for part in path) or "$"
                    )
            elif path and _remove_at_path(resource, path):
                removed_paths.append(".".join(str(part) for part in path) or "$")

            if not removed_paths:
                unresolved.append(
                    {
                        "resource_type": resource_type,
                        "resource_id": resource.get("id"),
                        "path": ".".join(str(part) for part in path) or "$",
                        "message": error.message,
                    }
                )
                break
            for removed_path in removed_paths:
                removals[f"{resource_type}.{removed_path}"] += 1
        else:
            unresolved.append(
                {
                    "resource_type": resource_type,
                    "resource_id": resource.get("id"),
                    "path": "$",
                    "message": "Schema cleanup exceeded its bounded iteration limit",
                }
            )
    return curated, {
        "release": release,
        "passed": not unresolved,
        "resources_checked": len(curated),
        "optional_values_removed": sum(removals.values()),
        "removals_by_path": dict(removals.most_common(50)),
        "unresolved_count": len(unresolved),
        "unresolved_sample": unresolved[:20],
        "policy": "remove-invalid-optional-content;never-invent-required-content",
    }


def _walk_references(node: Any) -> Iterator[str]:
    if isinstance(node, dict):
        reference = node.get("reference")
        if isinstance(reference, str) and _RELATIVE_REFERENCE.fullmatch(reference):
            yield reference
        for value in node.values():
            yield from _walk_references(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk_references(value)


def _temporal_values(resource: dict[str, Any]) -> list[str]:
    """Return the cohort-controlled dates for a supported longitudinal resource."""
    resource_type = str(resource.get("resourceType") or "")
    direct_fields = {
        "Condition": ("onsetDateTime", "recordedDate"),
        "Observation": ("effectiveDateTime", "issued"),
        "Procedure": ("occurrenceDateTime",),
        "DiagnosticReport": ("effectiveDateTime", "issued"),
        "MedicationRequest": ("authoredOn",),
        "ServiceRequest": ("authoredOn",),
        "MedicationAdministration": (
            "occurrenceDateTime",
            # R5's published JSON schema retains this historical misspelling.
            "occurenceDateTime",
        ),
        "Claim": ("created",),
        "ClaimResponse": ("created",),
        "ExplanationOfBenefit": ("created",),
        "CoverageEligibilityRequest": ("created",),
        "CoverageEligibilityResponse": ("created",),
    }
    period_fields = {
        "Encounter": "actualPeriod",
        "Coverage": "period",
        "CarePlan": "period",
    }
    values = [
        str(resource[field])
        for field in direct_fields.get(resource_type, ())
        if isinstance(resource.get(field), str)
    ]
    period = resource.get(period_fields.get(resource_type, ""))
    if isinstance(period, dict):
        values.extend(
            str(period[key])
            for key in ("start", "end")
            if isinstance(period.get(key), str)
        )
    return values


def _as_date(value: str) -> date | None:
    try:
        # FHIR dateTime values may include a time zone; the cohort contract is
        # concerned with the deterministic calendar window.
        return date.fromisoformat(value[:10])
    except (TypeError, ValueError):
        return None


def build_quality_report(
    resources: list[dict[str, Any]],
    plan: dict[str, Any],
    patient_evidence: list[dict[str, Any]],
    schema_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    actual = Counter(str(item.get("resourceType")) for item in resources if item.get("resourceType"))
    known = {
        f"{item.get('resourceType')}/{item.get('id')}"
        for item in resources
        if item.get("resourceType") and item.get("id")
    }
    references = list(_walk_references(resources))
    unresolved = sorted({reference for reference in references if reference not in known})
    planned = plan["planned"]
    count_deltas = {
        resource_type: actual.get(resource_type, 0) - int(expected)
        for resource_type, expected in planned.items()
    }
    requested_types = {
        entry["resource_type"] for entry in plan["per_patient_distributions"]
    }
    patient_links = Counter()
    for resource in resources:
        if resource.get("resourceType") not in requested_types:
            continue
        if any(reference.startswith("Patient/") for reference in _walk_references(resource)):
            patient_links[str(resource["resourceType"])] += 1
    linkage_expected = {
        resource_type: actual.get(resource_type, 0)
        for resource_type in sorted(requested_types)
    }
    linkage_missing = {
        resource_type: expected - patient_links.get(resource_type, 0)
        for resource_type, expected in linkage_expected.items()
        if expected - patient_links.get(resource_type, 0) > 0
    }
    linkage_passed = not linkage_missing

    longitudinal_start = date.fromisoformat(plan["cohort"]["reference_date"]) - timedelta(
        days=int(plan["cohort"]["history_years"]) * 365
    )
    longitudinal_end = date.fromisoformat(plan["cohort"]["reference_date"])
    temporal_values_checked = 0
    temporal_resources_checked = 0
    temporal_missing: Counter[str] = Counter()
    temporal_outside: list[dict[str, Any]] = []
    for resource in resources:
        resource_type = str(resource.get("resourceType") or "")
        values = _temporal_values(resource)
        if not values:
            if resource_type in {
                "Encounter",
                "Condition",
                "Observation",
                "Procedure",
                "DiagnosticReport",
                "MedicationRequest",
                "ServiceRequest",
                "MedicationAdministration",
                "Claim",
                "ClaimResponse",
                "ExplanationOfBenefit",
                "CoverageEligibilityRequest",
                "CoverageEligibilityResponse",
                "Coverage",
                "CarePlan",
            } and resource_type in requested_types:
                temporal_missing[resource_type] += 1
            continue
        temporal_resources_checked += 1
        for raw_value in values:
            temporal_values_checked += 1
            parsed = _as_date(raw_value)
            if parsed is None or not longitudinal_start <= parsed <= longitudinal_end:
                temporal_outside.append(
                    {
                        "resource_type": resource_type,
                        "resource_id": resource.get("id"),
                        "value": raw_value,
                    }
                )
    longitudinal_passed = not temporal_missing and not temporal_outside
    measurements: Counter[str] = Counter()
    demographics: Counter[str] = Counter()
    for evidence in patient_evidence:
        measurements.update(evidence.get("measurements") or {})
        demographics[str(evidence.get("gender") or "unknown")] += 1
    bp_rule_applies = any(
        rule.get("id") == "blood-pressure-panel-v1"
        for rule in plan.get("clinical_rules") or []
        if isinstance(rule, dict)
    )
    bp_consistent = 0
    bp_total = 0
    if bp_rule_applies:
        for observation in (
            item for item in resources if item.get("resourceType") == "Observation"
        ):
            codings = (observation.get("code") or {}).get("coding") or []
            if not any(
                code.get("code") == "85354-9"
                for code in codings
                if isinstance(code, dict)
            ):
                continue
            values = [
                component.get("valueQuantity", {}).get("value")
                for component in observation.get("component") or []
                if isinstance(component, dict)
            ]
            bp_total += 1
            if (
                len(values) >= 2
                and all(isinstance(value, (int, float)) for value in values[:2])
                and values[0] > values[1]
            ):
                bp_consistent += 1
    exact_counts = all(delta == 0 for delta in count_deltas.values())
    schema_evidence = schema_evidence or {
        "passed": False,
        "unresolved_count": 1,
        "policy": "not-run",
    }
    bp_passed = not bp_rule_applies or (bp_total > 0 and bp_consistent == bp_total)
    return {
        "contract_version": GENERATION_EVIDENCE_VERSION,
        "plan_digest": plan["plan_digest"],
        "generation_level": plan["blueprint"]["maturity"],
        "status": (
            "passed"
            if (
                exact_counts
                and not unresolved
                and schema_evidence.get("passed")
                and linkage_passed
                and longitudinal_passed
                and bp_passed
            )
            else "attention"
        ),
        "checks": {
            "base_schema_conformance": schema_evidence,
            "count_fidelity": {
                "passed": exact_counts,
                "planned": planned,
                "actual_requested_types": {resource_type: actual.get(resource_type, 0) for resource_type in planned},
                "deltas": count_deltas,
            },
            "relative_reference_integrity": {
                "passed": not unresolved,
                "references_checked": len(references),
                "unresolved_count": len(unresolved),
                "sample": unresolved[:20],
            },
            "patient_linkage": {
                "passed": linkage_passed,
                "expected_by_resource_type": linkage_expected,
                "linked_by_resource_type": dict(sorted(patient_links.items())),
                "missing_by_resource_type": linkage_missing,
                "scope": sorted(requested_types),
            },
            "longitudinal_window": {
                "passed": longitudinal_passed,
                "start": longitudinal_start.isoformat(),
                "end": longitudinal_end.isoformat(),
                "resources_checked": temporal_resources_checked,
                "values_checked": temporal_values_checked,
                "missing_by_resource_type": dict(sorted(temporal_missing.items())),
                "outside_window_count": len(temporal_outside),
                "outside_window_sample": temporal_outside[:20],
            },
            "blood_pressure_consistency": {
                "passed": bp_passed,
                "applicable": bp_rule_applies,
                "panels_checked": bp_total,
                "systolic_greater_than_diastolic": bp_consistent,
            },
        },
        "measurements": dict(sorted(measurements.items())),
        "population_observed": {
            "patients": actual.get("Patient", 0),
            "gender": dict(sorted(demographics.items())),
        },
        "actual_resource_counts": dict(sorted(actual.items())),
        "auto_generated_dependencies": {
            resource_type: count
            for resource_type, count in sorted(actual.items())
            if resource_type not in planned
        },
        "claim": "curated-demonstration-data-not-clinical-or-epidemiological-evidence",
    }
