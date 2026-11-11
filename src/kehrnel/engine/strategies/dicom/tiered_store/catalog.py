"""Read-only, activation-specific discovery for query builders and tools."""
from __future__ import annotations

import copy
import json

from .common import ROOT, config, version
from .query import QUERY_SCHEMA, scoped

LABELS = {"00080070": "Manufacturer", "00180050": "Slice thickness",
          "00080060": "Modality", "00180080": "Repetition time",
          "00180081": "Echo time", "00181210": "Convolution kernel"}
EXAMPLES = json.loads((ROOT / "examples.json").read_text())


def describe(ctx, manifest):
    cfg = config(ctx)
    allowed = scoped(ctx, cfg)["_kehrnel.centerId"]["$in"]
    research = cfg["projections"]["enabled"]
    promoted = {rule["tag"]: rule for rule in cfg["projections"]["tags"]}
    fields = QUERY_SCHEMA["properties"]["filter"]["properties"]
    catalogs = ["patientId", "studyUid", "day", "fromDay", "toDay", "modality", "description", "descriptionMode"]
    definitions = [
        ("studies", "Study catalog", "study", catalogs + ["seriesDescription"], True),
        ("series", "Series catalog", "series", catalogs + ["seriesUid"], True),
        ("instances", "Instances in one series", "instance", ["studyUid", "seriesUid"], True),
        ("research", "Selected instance metadata", "instance", ["patientId", "studyUid", "seriesUid", "day", "fromDay", "toDay", "modality", "tag", "tags"], research),
    ]
    examples = []
    for source in EXAMPLES["queries"]:
        example = copy.deepcopy(source)
        request = example["query"]
        requested_centers = request.get("centers", [])
        required = example.get("requiredTags", [])
        is_research = request["resource"] == "research"
        enabled = (not requested_centers or set(requested_centers) <= set(allowed))
        enabled = enabled and (not is_research or research)
        enabled = enabled and all(promoted.get(item["tag"], {}).get("type") == item["type"] for item in required)
        modality = request.get("filter", {}).get("modality")
        enabled = enabled and (not is_research or not modality or not cfg["projections"]["modalities"] or modality in cfg["projections"]["modalities"])
        example["enabled"] = bool(enabled)
        example["dataAvailability"] = "not_checked; synthetic identifiers only"
        if not enabled:
            example["disabledReason"] = "Example center, research activation, or promoted-field policy is unavailable."
        examples.append(example)
    return {"ok": True, "catalogVersion": "dicom-query-catalog/v1",
            "strategyId": manifest.id, "strategyVersion": manifest.version,
            "queryContract": "dicom-query/v1", "querySchema": copy.deepcopy(QUERY_SCHEMA),
            "authorizedCenters": allowed,
            "resources": [{"id": resource, "title": title, "resultGrain": grain, "enabled": enabled,
                           "filters": {name: copy.deepcopy(fields[name]) for name in names},
                           "requiresSeriesUid": resource == "instances",
                           "requiresAllowPartial": resource == "research",
                           "coverage": "partial" if resource == "research" else "catalog"}
                          for resource, title, grain, names, enabled in definitions],
            "promotedFields": [{**copy.deepcopy(rule), "label": LABELS.get(rule["tag"], rule["tag"]),
                                "enabled": research, "operators": ["equals"] if rule["type"] == "string" else ["equals", "gte", "lt"],
                                "missingMeans": "unknown or not projected"}
                               for rule in cfg["projections"]["tags"]],
            "researchPredicates": {"maxTags": 16, "combination": "AND",
                                   "correlation": "same SOP instance; tag/value bounds share one attrs element",
                                   "nestedSequences": False, "projectionVersion": version(cfg)},
            "limits": {"defaultPageSize": cfg["query"]["default_limit"], "maxPageSize": cfg["query"]["max_limit"],
                       "descriptionPrefix": "one UTC source day", "maxHeaderInstances": 100,
                       "maxHeaderTags": 32, "maxHeaderResponseBytes": cfg["headers"]["max_response_bytes"]},
            "headerOperation": "dicom_get_headers",
            "headerInputSchema": copy.deepcopy(next(op.input_schema for op in manifest.ops if op.name == "dicom_get_headers")),
            "examples": examples, "exampleDataset": copy.deepcopy(EXAMPLES["dataset"]),
            "executionSurfaces": {"sdk": "StrategyHandle.dispatch(run_op, ctx, operation, payload)", "http": "POST /v1/environments/{env}/run",
                                  "cli": "kehrnel run <operation> --env <env> --domain dicom"},
            "unsupportedInteractions": ["complete corpus cohort publication", "Atlas Search query execution",
                                        "nested sequence predicate correlation", "condition inference"],
            "scaleValidated": False}
