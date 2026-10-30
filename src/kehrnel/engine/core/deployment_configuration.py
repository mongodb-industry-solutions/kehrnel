"""Compile a portable deployment contract without runtime activation or bindings."""
from __future__ import annotations

import importlib
import json
import re
from copy import deepcopy
from pathlib import Path

from kehrnel.contracts.deployment_configuration import DeploymentCompilationRequest
from kehrnel.engine.core.deployment_identity import content_digest, package_identity, strategy_package_root
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.contracts.package_digest import compute_package_digest
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.config import build_coding_opts, build_flattener_config, normalize_config
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener


CONTRACT = "kehrnel.deployment-configuration/v1"
COMPILER_VERSION = "2"
SUPPORTED_STRATEGIES = {"openehr.rps_dual", "openehr.rps_dual_ibm"}
RPS_ROOT = Path(__file__).resolve().parents[1] / "strategies/openehr/rps_dual"
SECRET_KEY = re.compile(r"^(?:uri|connectionstring|password|passwd|credentials?|secrets?|apikey|servicekey|privatekey|token|accesstoken|refreshtoken|clientsecret|authorization|bindings|bindingsref|__proto__|prototype|constructor)$", re.I)


def invalid(code, message, details=None):
    raise KehrnelError(code=code, status=422, message=message, details=details or {})


def reject_secrets(value, path="input"):
    if isinstance(value, dict):
        for key, child in value.items():
            if SECRET_KEY.fullmatch(re.sub(r"[_-]", "", str(key))) or str(key) == "__proto__":
                invalid("DEPLOYMENT_SECRET_FORBIDDEN", f"Connection credentials and bindings are not allowed at {path}.{key}")
            reject_secrets(child, f"{path}.{key}")
    elif isinstance(value, list):
        for child in value:
            reject_secrets(child, path)
    elif isinstance(value, int) and not isinstance(value, bool) and abs(value) > 9007199254740991:
        invalid("DEPLOYMENT_JSON_INVALID", "Deployment numbers must be safe integers")
    elif isinstance(value, str) and (re.search(r"mongodb(?:\+srv)?://|[a-z][a-z0-9+.-]*://[^/\s]*@|-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----", value, re.I)):
        invalid("DEPLOYMENT_SECRET_FORBIDDEN", "MongoDB connection strings cannot be exported")


def merge(defaults, overrides):
    result = deepcopy(defaults)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def dictionary_snapshot(kind, config, defaults, supplied, package_root):
    if kind in supplied:
        payload = deepcopy(supplied[kind])
        if not isinstance(payload, dict):
            invalid("DEPLOYMENT_DICTIONARY_INVALID", f"{kind} must be an inline dictionary snapshot")
        return payload, "supplied"
    reference = (config.get("collections", {}).get(kind) or {}).get("seed")
    expected = (defaults.get("collections", {}).get(kind) or {}).get("seed")
    if isinstance(reference, dict):
        return deepcopy(reference), "inline"
    if not reference:
        return {}, "unresolved"
    if reference != expected or not str(reference).startswith("file://bundles/"):
        invalid("DEPLOYMENT_DICTIONARY_UNRESOLVED", f"Supply the {kind} snapshot instead of a live or external seed reference")
    # Resolve only the curated seed selected by this installed manifest, never arbitrary caller paths.
    path = (package_root / reference[len("file://"):]).resolve()
    if package_root not in path.parents:
        invalid("DEPLOYMENT_DICTIONARY_UNRESOLVED", "Dictionary seed escapes the installed package")
    payload = json.loads(path.read_text())
    if isinstance(payload, list):
        wanted = "ar_code" if kind == "codes" else "shortcuts"
        matches = [doc for doc in payload if isinstance(doc, dict) and doc.get("_id") == wanted]
        if len(matches) != 1:
            invalid("DEPLOYMENT_DICTIONARY_INVALID", f"The packaged {kind} snapshot is ambiguous")
        payload = matches[0]
    if not isinstance(payload, dict):
        invalid("DEPLOYMENT_DICTIONARY_INVALID", f"The packaged {kind} snapshot is invalid")
    return payload, "package"


def code_book(doc):
    if isinstance(doc.get("ar_code"), dict):
        return {"ar_code": deepcopy(doc["ar_code"]), "at": deepcopy(doc.get("at") or {})}
    ar = {}
    for rm, subtree in doc.items():
        if rm in {"_id", "_max", "_min", "unknown", "at"} or not isinstance(subtree, dict):
            continue
        for name, versions in subtree.items():
            if isinstance(versions, dict):
                for version, number in versions.items():
                    ar[f"{rm}.{name}.{version}"] = number
    return {"ar_code": ar, "at": deepcopy(doc.get("at") or {})}


def index_entry(entry, *, kind):
    result = deepcopy(entry)
    result["kind"] = kind
    if kind == "btree":
        result["keys"] = [[str(field), direction] for field, direction in result.get("keys", [])]
        options = result.setdefault("options", {})
        options.setdefault("name", "_".join(f"{field}_{direction}" for field, direction in result["keys"]))
        result["name"] = options["name"]
    result.setdefault("requirement", "required")
    result.setdefault("autoCreate", True)
    result["digest"] = content_digest(result)
    return result


async def compile_deployment_configuration(manifest, body: DeploymentCompilationRequest):
    if manifest.id not in SUPPORTED_STRATEGIES:
        invalid("DEPLOYMENT_EXPORT_UNSUPPORTED", "This strategy does not support portable deployment compilation")
    installed_identity = package_identity(manifest)
    package_root = strategy_package_root(manifest)
    if body.version is not None and body.version != manifest.version:
        raise KehrnelError(code="DEPLOYMENT_VERSION_MISMATCH", status=409, message="The requested exact strategy version is not installed")
    if body.packageDigest is not None and body.packageDigest != installed_identity["packageDigest"]:
        raise KehrnelError(code="DEPLOYMENT_PACKAGE_MISMATCH", status=409, message="The installed strategy package changed; refresh its manifest before publishing")
    reject_secrets(body.model_dump())
    try:
        content_digest(body.model_dump(mode="json"))
    except TypeError as exc:
        invalid("DEPLOYMENT_JSON_INVALID", str(exc))
    if set(body.config) - {"database", "collections", "ids", "paths", "fields", "transform", "bootstrap"}:
        invalid("DEPLOYMENT_INPUT_INVALID", "Only portable strategy configuration is allowed")
    if set(body.dictionaries) - {"codes", "shortcuts"}:
        invalid("DEPLOYMENT_DICTIONARY_INVALID", "Only codes and shortcuts snapshots are supported")
    template_ids = [template.templateId for template in body.templates]
    if len(template_ids) != len(set(template_ids)):
        invalid("DEPLOYMENT_TEMPLATE_AMBIGUOUS", "Each templateId must identify exactly one selected template")
    config = merge(manifest.default_config or {}, body.config)
    module, class_name = manifest.entrypoint.split(":")
    plugin = getattr(importlib.import_module(module), class_name)(deepcopy(manifest))
    if hasattr(plugin, "_coerce_ibm_config"):
        config = plugin._coerce_ibm_config(config)
    normalized = normalize_config(config).model_dump(mode="json")
    normalized["database"] = config.get("database")
    if not isinstance(normalized["database"], str) or not normalized["database"].strip():
        invalid("DEPLOYMENT_DATABASE_REQUIRED", "A target database name is required; no connection credentials are needed")
    templates = sorted([template.model_dump(mode="json") for template in body.templates], key=lambda item: item["templateId"])
    mappings = {"templates": templates}
    normalized["transform"]["mappings"] = mappings
    codes, codes_source = dictionary_snapshot("codes", config, manifest.default_config or {}, body.dictionaries, package_root)
    shortcuts, shortcuts_source = dictionary_snapshot("shortcuts", config, manifest.default_config or {}, body.dictionaries, package_root)
    for section in ("items", "keys", "values"):
        if section in shortcuts and not isinstance(shortcuts[section], dict):
            invalid("DEPLOYMENT_DICTIONARY_INVALID", f"Shortcut {section} must be a dictionary")
    keys = {**deepcopy(shortcuts.get("items") or {}), **deepcopy(shortcuts.get("keys") or {})}
    values = deepcopy(shortcuts.get("values") or {})
    if normalized["transform"]["apply_shortcuts"] and not keys:
        invalid("DEPLOYMENT_SHORTCUTS_UNRESOLVED", "An explicit shortcut mapping is required when shortcuts are enabled")
    for role, snapshot in (("codes", codes), ("shortcuts", shortcuts)):
        normalized["collections"][role]["seed"] = snapshot
    if not isinstance(keys, dict) or not isinstance(values, dict) or any(not isinstance(alias, str) or not alias or "." in alias or "$" in alias or "\0" in alias for alias in keys.values()):
        invalid("DEPLOYMENT_DICTIONARY_INVALID", "Shortcut keys and values must be dictionaries with valid field aliases")
    if len(set(keys.values())) != len(keys):
        invalid("DEPLOYMENT_DICTIONARY_INVALID", "Shortcut field aliases must be unique")
    cfg = normalize_config(normalized)
    if cfg.collections.search.enabled and (not cfg.collections.search.atlasIndex or not cfg.collections.search.atlasIndex.name.strip()):
        invalid("DEPLOYMENT_SEARCH_INDEX_REQUIRED", "Enabled search requires a named Atlas Search index")
    physical_names = [cfg.collections.compositions.name, cfg.collections.codes.name, cfg.collections.shortcuts.name]
    if cfg.collections.search.enabled:
        physical_names.append(cfg.collections.search.name)
    if any(not name or "$" in name or "\0" in name for name in physical_names) or len(set(physical_names)) != len(physical_names):
        invalid("DEPLOYMENT_COLLECTION_INVALID", "Enabled stores need distinct valid collection names")
    ctx = StrategyContext(environment_id="deployment-compilation", config=normalized, adapters={}, manifest=manifest,
                          meta={"mappings": mappings, "strict_index_planning": True, "include_optional_indexes": True,
                                "dict_cache": {"shortcuts": keys}, "index_shortcut_keys": keys,
                                "archetype_search_type": "token" if manifest.id.endswith("_ibm") else None})
    await plugin.validate_config(ctx)
    plan = await plugin.plan(ctx)
    indexes = {"btree": [index_entry(item, kind="btree") for item in plan.artifacts.get("indexes", [])],
               "atlasSearch": [index_entry(item, kind="atlas_search") for item in plan.artifacts.get("search_indexes", [])]}
    for kind, entries in indexes.items():
        seen = {}
        for entry in entries:
            identity = (entry["collection"], entry["name"])
            if identity in seen and seen[identity] != entry["digest"]:
                invalid("DEPLOYMENT_INDEX_CONFLICT", "Two incompatible definitions share an index name", {"collection": identity[0], "name": identity[1]})
            seen[identity] = entry["digest"]
        indexes[kind] = sorted({entry["digest"]: entry for entry in entries}.values(), key=lambda item: (item["collection"], item["name"]))
    atlas_config = normalized["collections"]["search"].get("atlasIndex")
    if atlas_config is not None:
        atlas_config["definition"] = deepcopy(indexes["atlasSearch"][0]["definition"]) if indexes["atlasSearch"] else {}
    flattened_codes = code_book(codes)
    flattener_config = build_flattener_config(cfg)
    flattener_config["role"] = "secondary"
    flattener_class = CompositionFlattener
    if manifest.id.endswith("_ibm"):
        from kehrnel.engine.strategies.openehr.rps_dual_ibm.ingest.flattener import IBMCompositionFlattener
        flattener_class = IBMCompositionFlattener
        flattened_codes["ar_code"] = {key: str(value) for key, value in flattened_codes["ar_code"].items()}
    flattener = flattener_class(db=None, config=flattener_config, mappings_path="", mappings_content=mappings, coding_opts=build_coding_opts(cfg))
    flattener.code_book = deepcopy(flattened_codes)
    flattener.shortcut_keys = keys
    flattener.shortcut_vals = values
    flattener._refresh_codec()
    compiled_rules = {}
    diagnostics = []
    for template in templates:
        rules = flattener._compiled_rules_for_template(template["templateId"])
        compiled_rules[template["templateId"]] = rules
        if len(rules) != len(template["fields"]):
            diagnostics.append({"code": "PROJECTION_DICTIONARY_COVERAGE_INCOMPLETE", "templateId": template["templateId"],
                                "fieldCount": len(template["fields"]), "compiledRuleCount": len(rules),
                                "message": "Prepare an immutable dictionary snapshot covering these template paths before running the projector"})
    result = {"contract": CONTRACT, "compilerVersion": COMPILER_VERSION, "executionMode": "external",
              "input": body.model_dump(mode="json"), "inputDigest": content_digest(body.model_dump(mode="json")),
              "strategy": {"id": manifest.id, "version": manifest.version, "manifestDigest": installed_identity["manifestDigest"],
                           "packageDigest": installed_identity["packageDigest"],
                           "implementationDigest": content_digest({"rpsDualPackage": compute_package_digest(RPS_ROOT),
                                                                  "compilerSource": Path(__file__).read_text(),
                                                                  "identitySource": Path(__file__).with_name("deployment_identity.py").read_text()})},
              "config": normalized, "templates": templates, "collections": plan.artifacts.get("collections", []),
              "projection": {"contract": "openehr.rps-dual.search-projection/v1", "algorithmVersion": "1", "flattenerClass": f"{flattener_class.__module__}:{flattener_class.__name__}",
                             "envelope": normalized["transform"]["envelope"], "shortcutValuePrefix": "$>" if manifest.id.endswith("_ibm") else "",
                             "archetypeCodeValueType": "string" if manifest.id.endswith("_ibm") else "integer",
                             "mappings": mappings, "fields": normalized["fields"], "ids": normalized["ids"],
                             "pathSeparator": cfg.paths.separator, "coding": normalized["transform"]["coding"],
                             "compositionEncodingProfile": cfg.collections.compositions.encodingProfile,
                             "searchEncodingProfile": cfg.collections.search.encodingProfile,
                             "shortcuts": {"keys": keys, "values": values}, "codes": flattened_codes,
                             "compiledRulesByTemplate": compiled_rules},
              "dictionaries": {"codes": {"snapshot": codes, "source": codes_source, "digest": content_digest(codes)},
                               "shortcuts": {"snapshot": shortcuts, "source": shortcuts_source, "digest": content_digest(shortcuts)}},
              "indexes": indexes, "diagnostics": diagnostics,
              "readiness": {"indexes": True, "projection": not diagnostics, "runtime": "not_observed"}}
    # JSON normalization preserves ordered index keys and makes rule tuples portable.
    result = json.loads(json.dumps(result, ensure_ascii=False))
    result["digest"] = content_digest(result)
    return result
