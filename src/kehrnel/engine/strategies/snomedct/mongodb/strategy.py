"""SNOMED CT on MongoDB strategy pack."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import ASCENDING, DESCENDING, ReplaceOne
from pymongo.errors import DuplicateKeyError

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.plugin import StrategyPlugin
from kehrnel.engine.core.types import ApplyPlan, ApplyResult, QueryPlan, QueryResult, StrategyContext, TransformResult
from kehrnel.engine.domains.snomedct import build_term_documents, iter_concepts_from_json, normalize_concept, normalize_text

PACK_ROOT = Path(__file__).resolve().parent
MANIFEST_PATH = PACK_ROOT / "manifest.json"
SCHEMA_PATH = PACK_ROOT / "schema.json"
DEFAULTS_PATH = PACK_ROOT / "defaults.json"
SNOMED_SYSTEM_URI = "http://snomed.info/sct"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST = StrategyManifest(**_load_json(MANIFEST_PATH))

_KNOWN_OPS = {
    "snomed_list_releases",
    "snomed_inspect_release",
    "snomed_diff_release",
    "snomed_ingest_release",
    "snomed_rebuild_sidecar",
    "snomed_ensure_indexes",
    "snomed_readiness",
    "snomed_capabilities",
    "snomed_lookup",
    "snomed_validate_code",
    "snomed_subsumes",
    "snomed_search",
    "snomed_suggest",
    "snomed_concept_history",
    "snomed_ecl",
    "snomed_parse_ecl",
    "snomed_compile_ecl",
    "snomed_hybrid_search",
    "snomed_concept_children",
    "snomed_concept_descendants",
    "snomed_concept_ancestors",
    "snomed_expand_value_set",
    "snomed_list_value_sets",
    "snomed_put_value_set",
    "snomed_validate_value_set",
    "snomed_relationship_search",
    "snomed_semantic_facets",
    "snomed_ground_note",
    "snomed_save_grounding_review",
    "snomed_query_grounded_corpus",
    "snomed_benchmark_retrieval",
}


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    result = dict(base or {})
    for key, value in (overlay or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _config(ctx: StrategyContext, manifest: StrategyManifest | None = None) -> dict[str, Any]:
    defaults = {}
    if manifest and manifest.default_config:
        defaults = dict(manifest.default_config)
    elif DEFAULTS_PATH.exists():
        defaults = _load_json(DEFAULTS_PATH)
    return _deep_merge(defaults, ctx.config or {})


def _release_id(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> str:
    release = cfg.get("release") if isinstance(cfg.get("release"), dict) else {}
    value = (payload or {}).get("release_id") or release.get("id")
    value = str(value or "").strip()
    if not value:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="release_id is required")
    return value


def _terminology_release_id(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> str:
    """Resolve a release from the native release id or a FHIR SNOMED version URI."""
    payload = payload or {}
    release_id = str(payload.get("release_id") or payload.get("releaseId") or "").strip()
    if release_id:
        return release_id
    version = str(payload.get("version") or "").strip().rstrip("/")
    if "/version/" in version:
        release_id = version.rsplit("/version/", 1)[-1].strip()
    elif version and "://" not in version:
        release_id = version
    if release_id:
        return release_id
    return _release_id(cfg, payload)


def _validate_snomed_system(payload: dict[str, Any]) -> str:
    system = str(payload.get("system") or SNOMED_SYSTEM_URI).strip().rstrip("/")
    if system != SNOMED_SYSTEM_URI:
        raise KehrnelError(
            code="TERMINOLOGY_SYSTEM_UNSUPPORTED",
            status=400,
            message=f"SNOMED CT provider does not support system {system!r}.",
            details={"supported_system": SNOMED_SYSTEM_URI},
        )
    return system


def _display_candidates(concept: dict[str, Any], language: str | None = None) -> list[str]:
    language = str(language or "").strip().lower()
    descriptions = concept.get("descriptions") if isinstance(concept.get("descriptions"), list) else []
    candidates: list[str] = []
    for description in descriptions:
        if not isinstance(description, dict) or not bool(description.get("active", True)):
            continue
        if language and str(description.get("languageCode") or "").lower() != language:
            continue
        term = str(description.get("term") or "").strip()
        if term and term not in candidates:
            candidates.append(term)
    return candidates


def _collections(cfg: dict[str, Any]) -> tuple[str, str, bool]:
    coll = cfg.get("collections") if isinstance(cfg.get("collections"), dict) else {}
    concepts = str(coll.get("concepts") or "").strip()
    terms = str(coll.get("terms") or "").strip()
    sidecar_enabled = bool(coll.get("sidecar_enabled", True))
    if not concepts:
        raise KehrnelError(code="INVALID_CONFIG", status=400, message="collections.concepts is required")
    if sidecar_enabled and not terms:
        raise KehrnelError(code="INVALID_CONFIG", status=400, message="collections.terms is required when sidecar is enabled")
    return concepts, terms, sidecar_enabled


def _value_sets_collection(cfg: dict[str, Any]) -> str:
    collections = cfg.get("collections") if isinstance(cfg.get("collections"), dict) else {}
    name = str(collections.get("value_sets") or "snomed_value_sets").strip()
    if not name:
        raise KehrnelError(code="INVALID_CONFIG", status=400, message="collections.value_sets is required")
    return name


def _grounding_collection(cfg: dict[str, Any]) -> str:
    collections = cfg.get("collections") if isinstance(cfg.get("collections"), dict) else {}
    name = str(collections.get("grounding_reviews") or "snomed_grounding_reviews").strip()
    if not name:
        raise KehrnelError(code="INVALID_CONFIG", status=400, message="collections.grounding_reviews is required")
    return name


def _value_set_definition(payload: dict[str, Any]) -> dict[str, Any]:
    expression = str(payload.get("expression") or payload.get("ecl") or "").strip()
    raw_codes = payload.get("concepts") or payload.get("codes") or []
    codes = sorted({str(value).strip() for value in raw_codes if str(value).strip()}) if isinstance(raw_codes, list) else []
    if bool(expression) == bool(codes):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Provide exactly one ValueSet definition: expression or concepts.",
        )
    invalid_codes = [code for code in codes if not re.fullmatch(r"[0-9]{6,18}", code)]
    if invalid_codes:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Explicit ValueSet concepts must be SNOMED CT identifiers.",
            details={"invalid_concepts": invalid_codes[:20]},
        )
    return {"type": "ecl", "expression": expression} if expression else {"type": "concepts", "concepts": codes}


def _limit(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> int:
    search = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    default_limit = int(search.get("default_limit") or 20)
    max_limit = int(search.get("max_limit") or 100)
    requested = int((payload or {}).get("limit") or default_limit)
    return max(1, min(requested, max_limit))


def _page(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> tuple[int, int]:
    payload = payload or {}
    try:
        offset = int(payload.get("offset") or 0)
    except (TypeError, ValueError) as exc:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="offset must be an integer") from exc
    if offset < 0:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="offset must be zero or greater")
    return offset, _limit(cfg, payload)


def _page_metadata(rows: list[dict[str, Any]], offset: int, limit: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    has_more = len(rows) > limit
    visible = rows[:limit]
    return visible, {
        "offset": offset,
        "limit": limit,
        "returned": len(visible),
        "hasMore": has_more,
        "nextOffset": offset + len(visible) if has_more else None,
    }


def _batch_size(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> int:
    ingest = cfg.get("ingest") if isinstance(cfg.get("ingest"), dict) else {}
    return max(1, int((payload or {}).get("batch_size") or ingest.get("batch_size") or 1000))


def _db(ctx: StrategyContext):
    storage = (ctx.adapters or {}).get("storage")
    db = getattr(storage, "db", None)
    if db is None:
        raise KehrnelError(
            code="MONGODB_ADAPTER_REQUIRED",
            status=500,
            message="SNOMED CT strategy requires MongoDB storage bindings.",
        )
    return db


def _source_config(cfg: dict[str, Any]) -> dict[str, Any]:
    return cfg.get("source") if isinstance(cfg.get("source"), dict) else {}


def _local_release_dir(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> Path:
    source = _source_config(cfg)
    raw = source.get("local_dir") or ".kehrnel/snomedct/releases"
    return Path(str(raw)).expanduser()


def _list_release_files(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> list[Path]:
    source = _source_config(cfg)
    local_dir = _local_release_dir(cfg, payload).resolve()
    pattern = str((payload or {}).get("file_pattern") or source.get("file_pattern") or "*.json")
    if Path(pattern).is_absolute() or ".." in Path(pattern).parts:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="file_pattern must stay inside the configured SNOMED CT release directory",
        )
    if not local_dir.exists():
        return []
    return sorted(
        path
        for path in local_dir.glob(pattern)
        if path.is_file() and path.resolve().is_relative_to(local_dir)
    )


def _resolve_release_path(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> Path:
    payload = payload or {}
    local_dir = _local_release_dir(cfg, payload).resolve()
    if payload.get("path"):
        path = Path(str(payload["path"])).expanduser()
        path = path.resolve() if path.is_absolute() else (local_dir / path).resolve()
        if not path.is_relative_to(local_dir):
            raise KehrnelError(
                code="RELEASE_PATH_OUTSIDE_STAGING",
                status=400,
                message="SNOMED CT release files must be inside the configured source.local_dir staging directory.",
            )
        if not path.exists():
            raise KehrnelError(code="RELEASE_FILE_NOT_FOUND", status=404, message=f"SNOMED CT release file not found: {path}")
        return path

    source = _source_config(cfg)
    file_name = str(payload.get("file_name") or source.get("file_name") or "").strip()
    if file_name:
        path = (local_dir / file_name).resolve()
        if not path.is_relative_to(local_dir):
            raise KehrnelError(
                code="RELEASE_PATH_OUTSIDE_STAGING",
                status=400,
                message="SNOMED CT release file_name must stay inside the configured source.local_dir staging directory.",
            )
        if not path.exists():
            raise KehrnelError(code="RELEASE_FILE_NOT_FOUND", status=404, message=f"SNOMED CT release file not found: {path}")
        return path

    files = _list_release_files(cfg, payload)
    if len(files) == 1:
        return files[0]
    if not files:
        raise KehrnelError(
            code="RELEASE_FILE_NOT_FOUND",
            status=404,
            message=f"No SNOMED CT JSON release files found in {local_dir}. Place the licensed JSON file there or pass path.",
        )
    raise KehrnelError(
        code="RELEASE_FILE_AMBIGUOUS",
        status=400,
        message="Multiple SNOMED CT JSON files found. Set source.file_name or pass path.",
        details={"local_dir": str(local_dir), "files": [path.name for path in files[:25]]},
    )


def _canonical_hash(doc: dict[str, Any]) -> str:
    payload = {k: v for k, v in doc.items() if k not in {"_id", "releaseAppliedAt"}}
    blob = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _projection_for_search() -> dict[str, int]:
    return {
        "_id": 0,
        "releaseId": 1,
        "conceptId": 1,
        "descriptionId": 1,
        "languageCode": 1,
        "term": 1,
        "matchedTerm": 1,
        "preferredTerm": 1,
        "fsn": 1,
        "termType": 1,
        "preferred": 1,
        "semanticTag": 1,
        "areaTags": 1,
        "topRoots": 1,
        "termRank": 1,
        "score": 1,
    }


def _projection_for_concept_summary() -> dict[str, Any]:
    return {
        "_id": 0,
        "releaseId": 1,
        "conceptId": 1,
        "active": 1,
        "effectiveTime": 1,
        "definitionStatusId": 1,
        "moduleId": 1,
        "memberOfRefsetIds": 1,
        "inferredParentIds": 1,
        "inferredAncestorIds": 1,
        "inferredChildIds": 1,
        "relationshipAttributeKeys": 1,
        "descriptions": {"$slice": ["$descriptions", 8]},
    }


def _search_pipeline(cfg: dict[str, Any], query: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    _, terms_collection, sidecar_enabled = _collections(cfg)
    if not sidecar_enabled:
        raise KehrnelError(
            code="SNOMED_SIDECAR_REQUIRED",
            status=409,
            message="SNOMED CT search and grounding require collections.sidecar_enabled=true.",
        )
    raw_q = str(query.get("q") or query.get("term") or "").strip()
    normalized = normalize_text(raw_q)
    if not normalized:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="q must be a non-empty string")
    search_cfg = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    language = str(query.get("language") or query.get("language_code") or search_cfg.get("default_language") or "es").lower()
    release_id = _release_id(cfg, query)
    offset, limit = _page(cfg, query)
    pattern = re.escape(normalized)
    match_pattern = f"^{pattern}" if bool(query.get("prefix_only")) else pattern
    pipeline = [
        {
            "$match": {
                "releaseId": release_id,
                "languageCode": language,
                "active": True,
                "conceptActive": True,
                "normalizedTerm": {"$regex": match_pattern, "$options": "i"},
            }
        },
        {
            "$addFields": {
                "score": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$normalizedTerm", normalized]}, "then": 100},
                            {
                                "case": {
                                    "$regexMatch": {
                                        "input": "$normalizedTerm",
                                        "regex": f"^{pattern}",
                                        "options": "i",
                                    }
                                },
                                "then": 80,
                            },
                        ],
                        "default": 50,
                    }
                }
            }
        },
        {"$sort": {"score": DESCENDING, "termRank": DESCENDING, "term": ASCENDING}},
        {"$skip": offset},
        {"$limit": limit + 1},
        {"$project": _projection_for_search()},
    ]
    return terms_collection, pipeline


def _hybrid_search_pipeline(cfg: dict[str, Any], query: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    _, terms_collection, sidecar_enabled = _collections(cfg)
    if not sidecar_enabled:
        raise KehrnelError(
            code="SNOMED_SIDECAR_REQUIRED",
            status=409,
            message="SNOMED CT hybrid search requires collections.sidecar_enabled=true.",
        )
    raw_q = str(query.get("q") or query.get("term") or "").strip()
    normalized = normalize_text(raw_q)
    search_cfg = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    language = str(query.get("language") or query.get("language_code") or search_cfg.get("default_language") or "es").lower()
    release_id = _release_id(cfg, query)
    offset, limit = _page(cfg, query)
    ancestor_id = str(query.get("ancestor_id") or query.get("ancestorId") or "").strip()
    area_tag = str(query.get("area_tag") or query.get("areaTag") or "").strip()
    semantic_tag = str(query.get("semantic_tag") or query.get("semanticTag") or "").strip()
    semantic_tag_key = str(query.get("semantic_tag_key") or query.get("semanticTagKey") or "").strip()
    if semantic_tag and not semantic_tag_key:
        semantic_tag_key = re.sub(r"[^a-z0-9]+", "-", normalize_text(semantic_tag)).strip("-")

    match: dict[str, Any] = {
        "releaseId": release_id,
        "languageCode": language,
        "active": True,
        "conceptActive": True,
    }
    if normalized:
        match["normalizedTerm"] = {"$regex": re.escape(normalized), "$options": "i"}
    if ancestor_id:
        match["ancestorIds"] = ancestor_id
    if area_tag:
        match["areaTags"] = area_tag
    if semantic_tag_key:
        match["semanticTagKey"] = semantic_tag_key
    if not any([normalized, ancestor_id, area_tag, semantic_tag_key]):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Hybrid search requires at least q, ancestor_id, area_tag, or semantic_tag.",
        )

    score_branches: list[dict[str, Any]] = []
    if normalized:
        escaped = re.escape(normalized)
        score_branches.extend(
            [
                {"case": {"$eq": ["$normalizedTerm", normalized]}, "then": 100},
                {
                    "case": {"$regexMatch": {"input": "$normalizedTerm", "regex": f"^{escaped}", "options": "i"}},
                    "then": 80,
                },
            ]
        )
    if ancestor_id:
        score_branches.append({"case": {"$in": [ancestor_id, {"$ifNull": ["$ancestorIds", []]}]}, "then": 20})
    if area_tag:
        score_branches.append({"case": {"$in": [area_tag, {"$ifNull": ["$areaTags", []]}]}, "then": 10})

    pipeline = [
        {"$match": match},
        {
            "$addFields": {
                "score": {
                    "$add": [
                        {"$switch": {"branches": score_branches, "default": 50 if normalized else 10}},
                        {"$ifNull": ["$termRank", 0]},
                    ]
                }
            }
        },
        {"$sort": {"score": DESCENDING, "termRank": DESCENDING, "term": ASCENDING}},
        {"$skip": offset},
        {"$limit": limit + 1},
        {"$project": _projection_for_search()},
    ]
    explain = {
        "mode": "hybrid_search",
        "usesExistingCollections": [terms_collection],
        "filters": {
            "q": raw_q or None,
            "ancestorId": ancestor_id or None,
            "areaTag": area_tag or None,
            "semanticTagKey": semantic_tag_key or None,
            "language": language,
            "releaseId": release_id,
        },
        "page": {"offset": offset, "limit": limit},
        "pipeline": pipeline,
    }
    return terms_collection, pipeline, explain


_ECL_TOKEN_RE = re.compile(
    r"""
    (?P<term>\|[^|]*\|) |
    (?P<op><<|>>|<|>|\^|=|:|,|\{|\}|\(|\)) |
    (?P<bool>\bAND\b|\bOR\b|\bMINUS\b) |
    (?P<star>\*) |
    (?P<number>\d{5,18}) |
    (?P<ws>\s+)
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _tokenize_ecl(expression: str) -> list[dict[str, str]]:
    tokens: list[dict[str, str]] = []
    cursor = 0
    while cursor < len(expression):
        match = _ECL_TOKEN_RE.match(expression, cursor)
        if not match:
            raise KehrnelError(
                code="ECL_PARSE_ERROR",
                status=400,
                message=f"Unsupported ECL token near: {expression[cursor:cursor + 24]!r}",
                details={"offset": cursor},
            )
        kind = match.lastgroup or ""
        value = match.group()
        cursor = match.end()
        if kind in {"ws", "term"}:
            continue
        if kind == "bool":
            tokens.append({"type": "bool", "value": value.upper()})
        elif kind == "number":
            tokens.append({"type": "concept", "value": value})
        else:
            tokens.append({"type": kind, "value": value})
    return tokens


class _ECLParser:
    def __init__(self, tokens: list[dict[str, str]]):
        self.tokens = tokens
        self.pos = 0

    def peek(self) -> dict[str, str] | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def consume(self, value: str | None = None, token_type: str | None = None) -> dict[str, str]:
        token = self.peek()
        if token is None:
            raise KehrnelError(code="ECL_PARSE_ERROR", status=400, message="Unexpected end of ECL expression")
        if value is not None and token["value"] != value:
            raise KehrnelError(code="ECL_PARSE_ERROR", status=400, message=f"Expected {value!r}, got {token['value']!r}")
        if token_type is not None and token["type"] != token_type:
            raise KehrnelError(code="ECL_PARSE_ERROR", status=400, message=f"Expected {token_type}, got {token['type']}")
        self.pos += 1
        return token

    def match(self, value: str | None = None, token_type: str | None = None) -> bool:
        token = self.peek()
        if token is None:
            return False
        if value is not None and token["value"] != value:
            return False
        if token_type is not None and token["type"] != token_type:
            return False
        return True

    def parse(self) -> dict[str, Any]:
        ast = self.parse_or()
        if self.peek() is not None:
            raise KehrnelError(code="ECL_PARSE_ERROR", status=400, message=f"Unexpected token {self.peek()['value']!r}")
        return ast

    def parse_or(self) -> dict[str, Any]:
        node = self.parse_and_minus()
        while self.match(token_type="bool") and self.peek()["value"] == "OR":
            self.consume(token_type="bool")
            node = {"type": "or", "children": [node, self.parse_and_minus()]}
        return node

    def parse_and_minus(self) -> dict[str, Any]:
        node = self.parse_primary()
        while self.match(token_type="bool") and self.peek()["value"] in {"AND", "MINUS"}:
            op = self.consume(token_type="bool")["value"].lower()
            node = {"type": op, "children": [node, self.parse_primary()]}
        return node

    def parse_primary(self) -> dict[str, Any]:
        if self.match("("):
            self.consume("(")
            node = self.parse_or()
            self.consume(")")
        else:
            node = self.parse_focus()
        if self.match(":"):
            self.consume(":")
            node = {"type": "refined", "focus": node, "refinement": self.parse_refinement()}
        return node

    def parse_focus(self) -> dict[str, Any]:
        operator = None
        if self.match(token_type="op") and self.peek()["value"] in {"<", "<<", ">", ">>", "^"}:
            operator = self.consume(token_type="op")["value"]
        if self.match(token_type="star"):
            self.consume(token_type="star")
            return {"type": "focus", "operator": operator or "*", "conceptId": "*"}
        concept = self.consume(token_type="concept")["value"]
        return {"type": "focus", "operator": operator or "self", "conceptId": concept}

    def parse_refinement(self) -> dict[str, Any]:
        node = self.parse_refinement_atom()
        while self.match(token_type="bool") and self.peek()["value"] in {"AND", "OR", "MINUS"}:
            op = self.consume(token_type="bool")["value"].lower()
            node = {"type": f"refinement_{op}", "children": [node, self.parse_refinement_atom()]}
        return node

    def parse_refinement_atom(self) -> dict[str, Any]:
        if self.match("{"):
            self.consume("{")
            items = [self.parse_refinement()]
            while self.match(","):
                self.consume(",")
                items.append(self.parse_refinement())
            self.consume("}")
            return {"type": "group", "attributes": items}
        return self.parse_attribute()

    def parse_attribute(self) -> dict[str, Any]:
        attr = self.parse_focus()
        self.consume("=")
        value = self.parse_focus()
        return {"type": "attribute", "attribute": attr, "value": value}


def _parse_ecl(expression: str) -> dict[str, Any]:
    expression = str(expression or "").strip()
    if not expression:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="expression is required")
    return _ECLParser(_tokenize_ecl(expression)).parse()


def _focus_to_match(ast: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    operator = ast.get("operator")
    concept_id = ast.get("conceptId")
    if operator == "*" or concept_id == "*":
        return {}, []
    if operator == "self":
        return {"conceptId": concept_id}, []
    if operator == "<":
        return {"inferredAncestorIds": concept_id}, []
    if operator == "<<":
        return {"$or": [{"conceptId": concept_id}, {"inferredAncestorIds": concept_id}]}, []
    if operator == "^":
        return {"memberOfRefsetIds": concept_id}, []
    if operator in {">", ">>"}:
        raise KehrnelError(
            code="ECL_RUNTIME_PIPELINE_REQUIRED",
            status=400,
            message=f"Operator {operator} is supported for simple focus expressions and compiles to a target-first aggregation pipeline.",
            details={"operator": operator, "conceptId": concept_id},
        )
    raise KehrnelError(code="ECL_UNSUPPORTED", status=400, message=f"Unsupported ECL focus operator: {operator}")


def _attribute_to_match(ast: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    attr = ast.get("attribute") or {}
    value = ast.get("value") or {}
    if attr.get("operator") != "self" or value.get("operator") != "self":
        warnings.append("Attribute type/value subsumption is parsed but exact relationship compilation is used only for bare concept ids.")
        raise KehrnelError(
            code="ECL_UNSUPPORTED_REFINEMENT",
            status=400,
            message="Only exact attribute refinements like 363698007 = 39057004 are compiled in the canonical-only planner.",
            details={"ast": ast, "warnings": warnings},
        )
    return {"relationshipAttributeKeys": f"{attr['conceptId']}|{value['conceptId']}"}, warnings


def _refinement_to_match(ast: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    node_type = ast.get("type")
    if node_type == "attribute":
        return _attribute_to_match(ast)
    if node_type == "group":
        clauses = []
        warnings: list[str] = []
        for item in ast.get("attributes", []):
            clause, child_warnings = _refinement_to_match(item)
            if clause:
                clauses.append(clause)
            warnings.extend(child_warnings)
        return ({"$and": clauses} if clauses else {}, warnings)
    if node_type in {"refinement_and", "refinement_or", "refinement_minus"}:
        children = ast.get("children") or []
        left, left_warnings = _refinement_to_match(children[0] if children else {})
        right, right_warnings = _refinement_to_match(children[1] if len(children) > 1 else {})
        if node_type == "refinement_and":
            return {"$and": [left or {}, right or {}]}, left_warnings + right_warnings
        if node_type == "refinement_or":
            return {"$or": [left or {}, right or {}]}, left_warnings + right_warnings
        return {"$and": [left or {}, {"$nor": [right or {}]}]}, left_warnings + right_warnings
    raise KehrnelError(code="ECL_UNSUPPORTED_REFINEMENT", status=400, message=f"Unsupported ECL refinement node: {node_type}")


def _ast_to_match(ast: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    node_type = ast.get("type")
    if node_type == "focus":
        return _focus_to_match(ast)
    if node_type == "and":
        warnings: list[str] = []
        clauses = []
        for child in ast.get("children", []):
            clause, child_warnings = _ast_to_match(child)
            if clause:
                clauses.append(clause)
            warnings.extend(child_warnings)
        return ({"$and": clauses} if clauses else {}, warnings)
    if node_type == "or":
        warnings = []
        clauses = []
        for child in ast.get("children", []):
            clause, child_warnings = _ast_to_match(child)
            clauses.append(clause or {})
            warnings.extend(child_warnings)
        return {"$or": clauses}, warnings
    if node_type == "minus":
        left, left_warnings = _ast_to_match((ast.get("children") or [{}])[0])
        right, right_warnings = _ast_to_match((ast.get("children") or [{}, {}])[1])
        return {"$and": [left or {}, {"$nor": [right or {}]}]}, left_warnings + right_warnings
    if node_type == "refined":
        focus, focus_warnings = _ast_to_match(ast.get("focus") or {})
        clause, warnings = _refinement_to_match(ast.get("refinement") or {})
        return {"$and": [focus or {}, clause]}, focus_warnings + warnings
    raise KehrnelError(code="ECL_UNSUPPORTED", status=400, message=f"Unsupported ECL AST node: {node_type}")


def _simple_ancestor_focus_pipeline(
    concepts_collection: str,
    release_id: str,
    ast: dict[str, Any],
    limit: int,
    offset: int = 0,
) -> list[dict[str, Any]] | None:
    if ast.get("type") != "focus" or ast.get("operator") not in {">", ">>"}:
        return None
    concept_id = ast.get("conceptId")
    include_self = ast.get("operator") == ">>"
    candidate_expr: Any = {"$ifNull": ["$inferredAncestorIds", []]}
    if include_self:
        candidate_expr = {"$concatArrays": [{"$ifNull": ["$inferredAncestorIds", []]}, ["$conceptId"]]}
    return [
        {"$match": {"releaseId": release_id, "active": True, "conceptId": concept_id}},
        {"$project": {"candidateIds": candidate_expr}},
        {
            "$lookup": {
                "from": concepts_collection,
                "let": {"ids": "$candidateIds", "releaseId": "$releaseId"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$releaseId", "$$releaseId"]},
                                    {"$eq": ["$active", True]},
                                    {"$in": ["$conceptId", "$$ids"]},
                                ]
                            }
                        }
                    },
                    {"$project": _projection_for_concept_summary()},
                ],
                "as": "matches",
            }
        },
        {"$unwind": "$matches"},
        {"$replaceRoot": {"newRoot": "$matches"}},
        {"$sort": {"conceptId": ASCENDING}},
        {"$skip": offset},
        {"$limit": limit + 1},
    ]


def _compile_ecl(cfg: dict[str, Any], query: dict[str, Any]) -> dict[str, Any]:
    concepts_collection, _, _ = _collections(cfg)
    release_id = _release_id(cfg, query)
    expression = str(query.get("expression") or query.get("ecl") or "").strip()
    offset, limit = _page(cfg, query)
    ast = _parse_ecl(expression)
    runtime_pipeline = _simple_ancestor_focus_pipeline(concepts_collection, release_id, ast, limit, offset)
    if runtime_pipeline is not None:
        return {
            "collection": concepts_collection,
            "pipeline": runtime_pipeline,
            "ast": ast,
            "warnings": [],
            "supportedSubset": _supported_ecl_subset(),
            "planner": "target_first_ancestor_lookup",
            "page": {"offset": offset, "limit": limit},
        }
    compiled_match, warnings = _ast_to_match(ast)
    match: dict[str, Any] = {"releaseId": release_id, "active": True}
    if compiled_match:
        match = {"$and": [match, compiled_match]}
    pipeline = [
        {"$match": match},
        {"$sort": {"conceptId": ASCENDING}},
        {"$skip": offset},
        {"$limit": limit + 1},
        {
            "$project": _projection_for_concept_summary()
        },
    ]
    return {
        "collection": concepts_collection,
        "pipeline": pipeline,
        "ast": ast,
        "warnings": warnings,
        "supportedSubset": _supported_ecl_subset(),
        "planner": "indexed_match",
        "page": {"offset": offset, "limit": limit},
    }


def _supported_ecl_subset() -> list[str]:
    return [
        "*",
        "conceptId",
        "< conceptId",
        "<< conceptId",
        "> conceptId",
        ">> conceptId",
        "^ refsetId",
        "AND",
        "OR",
        "MINUS",
        "parentheses",
        "exact attribute refinements",
        "grouped exact attribute refinements",
        "AND/OR/MINUS inside refinements",
    ]


def _ecl_pipeline(cfg: dict[str, Any], query: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    compiled = _compile_ecl(cfg, query)
    return compiled["collection"], compiled["pipeline"], compiled


def _lookup_pipeline(cfg: dict[str, Any], query: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    concepts_collection, _, _ = _collections(cfg)
    concept_id = str(query.get("concept_id") or query.get("conceptId") or "").strip()
    if not concept_id:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="concept_id is required")
    return concepts_collection, [{"$match": {"releaseId": _release_id(cfg, query), "conceptId": concept_id}}, {"$limit": 1}]


def _concept_id(payload: dict[str, Any]) -> str:
    concept_id = str(payload.get("concept_id") or payload.get("conceptId") or "").strip()
    if not concept_id:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="concept_id is required")
    return concept_id


def _children_pipeline(cfg: dict[str, Any], payload: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    concepts_collection, _, _ = _collections(cfg)
    release_id = _release_id(cfg, payload)
    concept_id = _concept_id(payload)
    offset, limit = _page(cfg, payload)
    pipeline = [
        {"$match": {"releaseId": release_id, "active": True, "inferredParentIds": concept_id}},
        {"$sort": {"conceptId": ASCENDING}},
        {"$skip": offset},
        {"$limit": limit + 1},
        {"$project": _projection_for_concept_summary()},
    ]
    return concepts_collection, pipeline, {"mode": "children", "conceptId": concept_id, "releaseId": release_id, "page": {"offset": offset, "limit": limit}, "pipeline": pipeline}


def _descendants_pipeline(cfg: dict[str, Any], payload: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    concepts_collection, _, _ = _collections(cfg)
    release_id = _release_id(cfg, payload)
    concept_id = _concept_id(payload)
    offset, limit = _page(cfg, payload)
    include_self = bool(payload.get("include_self") or payload.get("includeSelf"))
    concept_match: dict[str, Any] = {"releaseId": release_id, "active": True, "inferredAncestorIds": concept_id}
    if include_self:
        concept_match = {
            "releaseId": release_id,
            "active": True,
            "$or": [{"conceptId": concept_id}, {"inferredAncestorIds": concept_id}],
        }
    pipeline = [
        {"$match": concept_match},
        {"$sort": {"conceptId": ASCENDING}},
        {"$skip": offset},
        {"$limit": limit + 1},
        {"$project": _projection_for_concept_summary()},
    ]
    return concepts_collection, pipeline, {"mode": "descendants", "conceptId": concept_id, "releaseId": release_id, "includeSelf": include_self, "page": {"offset": offset, "limit": limit}, "pipeline": pipeline}


def _ancestors_pipeline(cfg: dict[str, Any], payload: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    concepts_collection, _, _ = _collections(cfg)
    release_id = _release_id(cfg, payload)
    concept_id = _concept_id(payload)
    offset, limit = _page(cfg, payload)
    include_self = bool(payload.get("include_self") or payload.get("includeSelf"))
    ast = {"type": "focus", "operator": ">>" if include_self else ">", "conceptId": concept_id}
    pipeline = _simple_ancestor_focus_pipeline(concepts_collection, release_id, ast, limit, offset) or []
    return concepts_collection, pipeline, {"mode": "ancestors", "conceptId": concept_id, "releaseId": release_id, "includeSelf": include_self, "page": {"offset": offset, "limit": limit}, "pipeline": pipeline}


def _relationship_search_pipeline(cfg: dict[str, Any], payload: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    concepts_collection, _, _ = _collections(cfg)
    release_id = _release_id(cfg, payload)
    type_id = str(payload.get("type_id") or payload.get("typeId") or "").strip()
    destination_id = str(payload.get("destination_id") or payload.get("destinationId") or "").strip()
    if not type_id and not destination_id:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="type_id or destination_id is required")
    offset, limit = _page(cfg, payload)
    match: dict[str, Any] = {"releaseId": release_id, "active": True}
    if type_id and destination_id:
        match["relationshipAttributeKeys"] = f"{type_id}|{destination_id}"
    else:
        elem_match: dict[str, Any] = {"active": True}
        if type_id:
            elem_match["typeId"] = type_id
        if destination_id:
            elem_match["destinationId"] = destination_id
        match["relationships"] = {"$elemMatch": elem_match}
    pipeline = [
        {"$match": match},
        {"$sort": {"conceptId": ASCENDING}},
        {"$skip": offset},
        {"$limit": limit + 1},
        {"$project": _projection_for_concept_summary()},
    ]
    return concepts_collection, pipeline, {
        "mode": "relationship_search",
        "releaseId": release_id,
        "typeId": type_id or None,
        "destinationId": destination_id or None,
        "usesRelationshipAttributeKey": bool(type_id and destination_id),
        "page": {"offset": offset, "limit": limit},
        "pipeline": pipeline,
    }


def _semantic_facets_pipeline(cfg: dict[str, Any], payload: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    _, terms_collection, sidecar_enabled = _collections(cfg)
    if not sidecar_enabled:
        raise KehrnelError(code="SNOMED_SIDECAR_REQUIRED", status=409, message="SNOMED CT semantic facets require collections.sidecar_enabled=true.")
    release_id = _release_id(cfg, payload)
    search_cfg = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    language = str(payload.get("language") or payload.get("language_code") or search_cfg.get("default_language") or "es").lower()
    raw_q = str(payload.get("q") or payload.get("term") or "").strip()
    normalized = normalize_text(raw_q)
    ancestor_id = str(payload.get("ancestor_id") or payload.get("ancestorId") or "").strip()
    match: dict[str, Any] = {"releaseId": release_id, "languageCode": language, "active": True, "conceptActive": True}
    if normalized:
        match["normalizedTerm"] = {"$regex": re.escape(normalized), "$options": "i"}
    if ancestor_id:
        match["ancestorIds"] = ancestor_id
    pipeline = [
        {"$match": match},
        {
            "$facet": {
                "areaTags": [
                    {"$unwind": "$areaTags"},
                    {"$group": {"_id": "$areaTags", "count": {"$sum": 1}}},
                    {"$sort": {"count": DESCENDING, "_id": ASCENDING}},
                    {"$limit": 25},
                ],
                "semanticTags": [
                    {"$match": {"semanticTagKey": {"$exists": True, "$ne": ""}}},
                    {"$group": {"_id": "$semanticTagKey", "label": {"$first": "$semanticTag"}, "count": {"$sum": 1}}},
                    {"$sort": {"count": DESCENDING, "_id": ASCENDING}},
                    {"$limit": 25},
                ],
                "topRoots": [
                    {"$unwind": "$topRoots"},
                    {"$group": {"_id": "$topRoots", "count": {"$sum": 1}}},
                    {"$sort": {"count": DESCENDING, "_id": ASCENDING}},
                    {"$limit": 25},
                ],
            }
        },
    ]
    return terms_collection, pipeline, {"mode": "semantic_facets", "releaseId": release_id, "language": language, "q": raw_q or None, "ancestorId": ancestor_id or None, "pipeline": pipeline}


async def _aggregate(ctx: StrategyContext, collection: str, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    storage = (ctx.adapters or {}).get("storage")
    if storage and hasattr(storage, "aggregate"):
        return await storage.aggregate(collection, pipeline)
    cursor = _db(ctx)[collection].aggregate(pipeline, allowDiskUse=True)
    return [doc async for doc in cursor]


class SNOMEDCTMongoDBStrategy(StrategyPlugin):
    """Canonical SNOMED CT document store plus derived term sidecar on MongoDB."""

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        if SCHEMA_PATH.exists():
            self.manifest.config_schema = _load_json(SCHEMA_PATH)
        if DEFAULTS_PATH.exists():
            self.manifest.default_config = _load_json(DEFAULTS_PATH)

    async def validate_config(self, ctx: StrategyContext | dict[str, Any]) -> bool:
        raw_config = ctx.config if isinstance(ctx, StrategyContext) else ctx
        cfg = _deep_merge(self.manifest.default_config or {}, raw_config or {})
        _collections(cfg)
        _value_sets_collection(cfg)
        _grounding_collection(cfg)
        _release_id(cfg)
        languages = cfg.get("languages") or []
        if not isinstance(languages, list) or not languages:
            raise KehrnelError(code="INVALID_CONFIG", status=400, message="languages must be a non-empty list")
        return True

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        cfg = _config(ctx, self.manifest)
        concepts, terms, sidecar_enabled = _collections(cfg)
        return ApplyPlan(
            artifacts={
                "action": "ensure_indexes",
                "collections": [concepts, _value_sets_collection(cfg)]
                + [_grounding_collection(cfg)]
                + ([terms] if sidecar_enabled else []),
            }
        )

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan | dict[str, Any]) -> ApplyResult:
        result = await self.snomed_ensure_indexes(ctx, {})
        return ApplyResult(created=result.get("created", []), warnings=result.get("warnings", []))

    async def transform(self, ctx: StrategyContext, payload: dict[str, Any]) -> TransformResult:
        cfg = _config(ctx, self.manifest)
        release_id = _release_id(cfg, payload)
        release_label = payload.get("release_label") or (cfg.get("release") or {}).get("label")
        concept = payload.get("concept")
        concepts = payload.get("concepts")
        if concept:
            base = normalize_concept(concept, release_id=release_id, release_label=release_label)
            search_docs = build_term_documents(base, release_id=release_id, language_codes=cfg.get("languages") or ["es"])
            return TransformResult(base=base, search={"terms": search_docs}, meta={"concept_count": 1, "term_count": len(search_docs)})
        if not isinstance(concepts, list):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="concept or concepts is required")
        base_docs = [normalize_concept(item, release_id=release_id, release_label=release_label) for item in concepts if isinstance(item, dict)]
        term_docs = [
            term
            for item in base_docs
            for term in build_term_documents(item, release_id=release_id, language_codes=cfg.get("languages") or ["es"])
        ]
        return TransformResult(base={"concepts": base_docs}, search={"terms": term_docs}, meta={"concept_count": len(base_docs), "term_count": len(term_docs)})

    async def reverse_transform(self, ctx: StrategyContext, payload: dict[str, Any]) -> TransformResult:
        raise NotImplementedError("snomedct.mongodb reverse_transform is not implemented")

    async def ingest(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        return await self.snomed_ingest_release(ctx, payload)

    async def compile_query(self, ctx: StrategyContext, domain: str, query: dict[str, Any]) -> QueryPlan:
        if (domain or "").lower() not in {"snomedct", "snomed", "snomed-ct"}:
            raise KehrnelError(code="DOMAIN_MISMATCH", status=400, message=f"Unsupported SNOMED CT domain: {domain}")
        cfg = _config(ctx, self.manifest)
        payload = dict(query or {})
        mode = str(payload.get("mode") or payload.get("type") or ("search" if payload.get("q") else "lookup")).lower()
        extra_explain: dict[str, Any] = {}
        pagination: dict[str, int] | None = None
        if mode == "search":
            collection, pipeline = _search_pipeline(cfg, payload)
            offset, limit = _page(cfg, payload)
            pagination = {"offset": offset, "limit": limit}
        elif mode in {"ecl", "subsumption"}:
            collection, pipeline, compiled = _ecl_pipeline(cfg, payload)
            offset, limit = _page(cfg, payload)
            pagination = {"offset": offset, "limit": limit}
            extra_explain = {
                "ast": compiled.get("ast"),
                "warnings": compiled.get("warnings", []),
                "supportedSubset": compiled.get("supportedSubset", []),
                "planner": compiled.get("planner"),
            }
        elif mode == "lookup":
            collection, pipeline = _lookup_pipeline(cfg, payload)
        else:
            raise KehrnelError(code="INVALID_INPUT", status=400, message=f"Unsupported SNOMED CT query mode: {mode}")
        return QueryPlan(
            engine="snomedct_mongodb",
            plan={"collection": collection, "pipeline": pipeline, "mode": mode, "pagination": pagination},
            explain={"domain": "snomedct", "mode": mode, "collection": collection, **extra_explain},
        )

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan | dict[str, Any]) -> QueryResult:
        if isinstance(plan, QueryPlan):
            inner = plan.plan or {}
            explain = plan.explain or {}
        else:
            inner = plan.get("plan") if isinstance(plan.get("plan"), dict) else plan
            explain = inner.get("explain") if isinstance(inner.get("explain"), dict) else {}
        collection = inner.get("collection")
        pipeline = inner.get("pipeline")
        if not collection or not isinstance(pipeline, list):
            raise KehrnelError(code="INVALID_PLAN", status=400, message="SNOMED CT plan requires collection and pipeline")
        rows = await _aggregate(ctx, collection, pipeline)
        explain = dict(explain or {})
        explain.setdefault("engine", "snomedct_mongodb")
        pagination = inner.get("pagination") if isinstance(inner.get("pagination"), dict) else None
        if pagination:
            rows, page = _page_metadata(rows, int(pagination["offset"]), int(pagination["limit"]))
            explain["page"] = page
        explain["returned"] = len(rows)
        return QueryResult(engine_used="snomedct_mongodb", rows=rows, explain=explain)

    async def run_op(self, ctx: StrategyContext, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        if op not in _KNOWN_OPS:
            raise ValueError(f"Strategy op '{op}' not supported")
        return await getattr(self, op)(ctx, payload or {})

    async def snomed_list_releases(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        local_dir = _local_release_dir(cfg, payload)
        files = _list_release_files(cfg, payload)
        source = _source_config(cfg)
        return {
            "ok": True,
            "local_dir": str(local_dir),
            "file_pattern": str(payload.get("file_pattern") or source.get("file_pattern") or "*.json"),
            "count": len(files),
            "files": [
                {
                    "path": str(path),
                    "name": path.name,
                    "bytes": path.stat().st_size,
                    "modified_at": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(),
                }
                for path in files
            ],
        }

    async def snomed_inspect_release(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        path = _resolve_release_path(cfg, payload)
        limit = payload.get("limit")
        count = active = inactive = 0
        max_effective_time = ""
        fields: set[str] = set()
        description_languages: set[str] = set()
        sample_ids: list[str] = []
        for concept in iter_concepts_from_json(path, limit=int(limit) if limit else None):
            count += 1
            if str(concept.get("active")) in {"1", "true", "True", "TRUE"} or concept.get("active") is True:
                active += 1
            else:
                inactive += 1
            effective_time = str(concept.get("effectiveTime") or "")
            if effective_time > max_effective_time:
                max_effective_time = effective_time
            fields.update(concept.keys())
            if len(sample_ids) < 10 and concept.get("conceptId"):
                sample_ids.append(str(concept.get("conceptId")))
            for desc in concept.get("descriptions", []) or []:
                if isinstance(desc, dict) and desc.get("languageCode"):
                    description_languages.add(str(desc.get("languageCode")).lower())
        return {
            "ok": True,
            "path": str(path),
            "limited": bool(limit),
            "concepts": count,
            "active": active,
            "inactive": inactive,
            "max_effective_time": max_effective_time or None,
            "fields": sorted(fields),
            "description_languages": sorted(description_languages),
            "sample_concept_ids": sample_ids,
        }

    async def snomed_diff_release(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        concepts_collection, _, _ = _collections(cfg)
        path = _resolve_release_path(cfg, payload)
        release_id = _release_id(cfg, payload)
        release_label = payload.get("release_label") or (cfg.get("release") or {}).get("label")
        include_descendants = bool(payload.get("include_descendants", (cfg.get("ingest") or {}).get("include_descendants", False)))
        sample_limit = int(payload.get("sample_limit") or 20)
        limit = payload.get("limit")
        db = _db(ctx)
        stats = {"official_count": 0, "missing_in_mongo": 0, "changed": 0, "unchanged": 0}
        samples: dict[str, list[dict[str, Any]]] = {"missing": [], "changed": []}
        for concept in iter_concepts_from_json(path, limit=int(limit) if limit else None):
            stats["official_count"] += 1
            canonical = normalize_concept(
                concept,
                release_id=release_id,
                release_label=release_label,
                include_descendants=include_descendants,
            )
            existing = await db[concepts_collection].find_one({"releaseId": release_id, "conceptId": canonical["conceptId"]})
            if not existing:
                stats["missing_in_mongo"] += 1
                if len(samples["missing"]) < sample_limit:
                    samples["missing"].append({"conceptId": canonical["conceptId"], "effectiveTime": canonical.get("effectiveTime")})
                continue
            if _canonical_hash(existing) == _canonical_hash(canonical):
                stats["unchanged"] += 1
            else:
                stats["changed"] += 1
                if len(samples["changed"]) < sample_limit:
                    samples["changed"].append(
                        {
                            "conceptId": canonical["conceptId"],
                            "mongoEffectiveTime": existing.get("effectiveTime"),
                            "officialEffectiveTime": canonical.get("effectiveTime"),
                        }
                    )
        return {"ok": True, "releaseId": release_id, "collection": concepts_collection, "stats": stats, "samples": samples}

    async def snomed_ingest_release(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        concepts_collection, _, sidecar_enabled = _collections(cfg)
        path = _resolve_release_path(cfg, payload)
        release_id = _release_id(cfg, payload)
        release_label = payload.get("release_label") or (cfg.get("release") or {}).get("label")
        ingest_cfg = cfg.get("ingest") if isinstance(cfg.get("ingest"), dict) else {}
        include_descendants = bool(payload.get("include_descendants", ingest_cfg.get("include_descendants", False)))
        dry_run = bool(payload.get("dry_run"))
        if not dry_run and payload.get("license_acknowledged") is not True:
            raise KehrnelError(
                code="SNOMED_LICENSE_ACKNOWLEDGEMENT_REQUIRED",
                status=422,
                message=(
                    "Persisting a SNOMED CT release requires explicit acknowledgement "
                    "that the tenant is authorized to use the staged content."
                ),
            )
        batch_size = _batch_size(cfg, payload)
        limit = payload.get("limit")
        db = _db(ctx)
        if bool(ingest_cfg.get("create_indexes_before_ingest", True)) and not dry_run:
            await self.snomed_ensure_indexes(ctx, {})
        if bool(payload.get("drop_before_ingest", ingest_cfg.get("drop_before_ingest", False))) and not dry_run:
            await db[concepts_collection].delete_many({"releaseId": release_id})
        count = upserted = modified = 0
        ops: list[ReplaceOne] = []
        started = datetime.now(timezone.utc)

        async def flush() -> None:
            nonlocal upserted, modified, ops
            if not ops:
                return
            if dry_run:
                upserted += len(ops)
                ops = []
                return
            result = await db[concepts_collection].bulk_write(ops, ordered=False)
            upserted += int(result.upserted_count or 0)
            modified += int(result.modified_count or 0)
            ops = []

        for raw in iter_concepts_from_json(path, limit=int(limit) if limit else None):
            doc = normalize_concept(raw, release_id=release_id, release_label=release_label, include_descendants=include_descendants)
            doc["_id"] = f"{release_id}|{doc['conceptId']}"
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            count += 1
            if len(ops) >= batch_size:
                await flush()
        await flush()
        sidecar = None
        rebuild = bool(payload.get("rebuild_sidecar", ingest_cfg.get("rebuild_sidecar", True)))
        if sidecar_enabled and rebuild and not dry_run:
            sidecar = await self.snomed_rebuild_sidecar(ctx, {"release_id": release_id, "batch_size": batch_size, "drop_before_rebuild": True})
        return {
            "ok": True,
            "dry_run": dry_run,
            "licenseAcknowledged": payload.get("license_acknowledged") is True,
            "releaseId": release_id,
            "collection": concepts_collection,
            "concepts_seen": count,
            "upserted": upserted,
            "modified": modified,
            "started_at": started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "sidecar": sidecar,
        }

    async def snomed_rebuild_sidecar(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        concepts_collection, terms_collection, sidecar_enabled = _collections(cfg)
        if not sidecar_enabled:
            return {"ok": True, "skipped": True, "reason": "collections.sidecar_enabled=false"}
        release_id = _release_id(cfg, payload)
        languages = [str(item).lower() for item in (payload.get("languages") or cfg.get("languages") or ["es"])]
        dry_run = bool(payload.get("dry_run"))
        batch_size = _batch_size(cfg, payload)
        limit = payload.get("limit")
        db = _db(ctx)
        if bool(payload.get("drop_before_rebuild", False)) and not dry_run:
            await db[terms_collection].delete_many({"releaseId": release_id})
        cursor = db[concepts_collection].find({"releaseId": release_id, "active": True})
        if limit:
            cursor = cursor.limit(int(limit))
        seen = term_count = upserted = modified = 0
        ops: list[ReplaceOne] = []

        async def flush() -> None:
            nonlocal upserted, modified, ops
            if not ops:
                return
            if dry_run:
                upserted += len(ops)
                ops = []
                return
            result = await db[terms_collection].bulk_write(ops, ordered=False)
            upserted += int(result.upserted_count or 0)
            modified += int(result.modified_count or 0)
            ops = []

        async for concept in cursor:
            seen += 1
            terms = build_term_documents(concept, release_id=release_id, language_codes=languages)
            term_count += len(terms)
            for term in terms:
                ops.append(ReplaceOne({"_id": term["_id"]}, term, upsert=True))
                if len(ops) >= batch_size:
                    await flush()
        await flush()
        return {
            "ok": True,
            "dry_run": dry_run,
            "releaseId": release_id,
            "source_collection": concepts_collection,
            "sidecar_collection": terms_collection,
            "concepts_seen": seen,
            "term_documents": term_count,
            "upserted": upserted,
            "modified": modified,
            "languages": languages,
        }

    async def snomed_ensure_indexes(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        concepts_collection, terms_collection, sidecar_enabled = _collections(cfg)
        value_sets_collection = _value_sets_collection(cfg)
        grounding_collection = _grounding_collection(cfg)
        dry_run = bool(payload.get("dry_run"))
        db = _db(ctx)
        index_cfg = cfg.get("indexes") if isinstance(cfg.get("indexes"), dict) else {}
        canonical_indexes = bool(index_cfg.get("canonical", True))
        sidecar_indexes = bool(index_cfg.get("sidecar", True))
        text_indexes = bool(index_cfg.get("text", False))
        created: list[str] = []
        warnings: list[str] = []
        index_specs: list[tuple[str, str, list[tuple[str, int]], dict[str, Any]]] = []
        if canonical_indexes:
            index_specs.extend(
                [
                    (concepts_collection, "concept_release_unique", [("releaseId", ASCENDING), ("conceptId", ASCENDING)], {"unique": True}),
                    (concepts_collection, "ancestor_lookup", [("releaseId", ASCENDING), ("inferredAncestorIds", ASCENDING)], {}),
                    (concepts_collection, "parent_lookup", [("releaseId", ASCENDING), ("inferredParentIds", ASCENDING)], {}),
                    (concepts_collection, "relationship_attribute_lookup", [("releaseId", ASCENDING), ("relationshipAttributeKeys", ASCENDING)], {}),
                    (value_sets_collection, "valueset_url_version_unique", [("url", ASCENDING), ("version", ASCENDING)], {"unique": True}),
                    (value_sets_collection, "valueset_status_updated", [("status", ASCENDING), ("updatedAt", DESCENDING)], {}),
                    (grounding_collection, "grounding_review_unique", [("reviewId", ASCENDING)], {"unique": True}),
                    (grounding_collection, "grounding_source_updated", [("sourceRef", ASCENDING), ("reviewedAt", DESCENDING)], {}),
                    (grounding_collection, "grounding_ancestor_query", [("acceptedAncestorIds", ASCENDING), ("reviewedAt", DESCENDING)], {}),
                ]
            )
        if sidecar_enabled and sidecar_indexes:
            index_specs.extend(
                [
                    (terms_collection, "term_release_language", [("releaseId", ASCENDING), ("languageCode", ASCENDING), ("normalizedTerm", ASCENDING)], {}),
                    (terms_collection, "term_concept", [("releaseId", ASCENDING), ("conceptId", ASCENDING)], {}),
                    (terms_collection, "term_rank", [("releaseId", ASCENDING), ("languageCode", ASCENDING), ("termRank", DESCENDING)], {}),
                    (terms_collection, "term_area_lookup", [("releaseId", ASCENDING), ("languageCode", ASCENDING), ("areaTags", ASCENDING)], {}),
                    (terms_collection, "term_semantic_lookup", [("releaseId", ASCENDING), ("languageCode", ASCENDING), ("semanticTagKey", ASCENDING)], {}),
                ]
            )
            if text_indexes:
                index_specs.append((terms_collection, "term_text", [("term", "text"), ("preferredTerm", "text"), ("fsn", "text")], {}))
        for collection, name, keys, opts in index_specs:
            if dry_run:
                created.append(f"{collection}.{name}")
                continue
            try:
                index_name = await db[collection].create_index(keys, name=name, background=True, **opts)
                created.append(f"{collection}.{index_name}")
            except Exception as exc:
                warnings.append(f"{collection}.{name}: {exc}")
        return {"ok": True, "dry_run": dry_run, "created": created, "warnings": warnings}

    async def snomed_readiness(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        concepts_collection, terms_collection, sidecar_enabled = _collections(cfg)
        release_id = _release_id(cfg, payload)
        db = _db(ctx)
        concept_filter = {"releaseId": release_id}
        concept_count = await db[concepts_collection].count_documents(concept_filter)
        active_count = await db[concepts_collection].count_documents({**concept_filter, "active": True})
        sidecar_count = await db[terms_collection].count_documents({"releaseId": release_id}) if sidecar_enabled else None
        value_set_count = await db[_value_sets_collection(cfg)].count_documents(
            {"releaseId": release_id}
        )
        sample = await db[concepts_collection].find_one(concept_filter, {"_id": 0, "conceptId": 1, "releaseId": 1, "inferredDescendantIds": 1})
        descendants_retained = bool(sample and sample.get("inferredDescendantIds") is not None)
        return {
            "ok": True,
            "releaseId": release_id,
            "collections": {"concepts": concepts_collection, "terms": terms_collection if sidecar_enabled else None, "valueSets": _value_sets_collection(cfg), "groundingReviews": _grounding_collection(cfg)},
            "canonical": {"concept_count": concept_count, "active_count": active_count, "ready": concept_count > 0},
            "sidecar": {"enabled": sidecar_enabled, "term_count": sidecar_count, "ready": (sidecar_count or 0) > 0 if sidecar_enabled else False},
            "valueSets": {
                "collection": _value_sets_collection(cfg),
                "count": value_set_count,
            },
            "storage_policy": {"inferredDescendantIds_retained": descendants_retained},
            "features": {
                "lookup": concept_count > 0,
                "validate_code": concept_count > 0,
                "subsumes": concept_count > 0,
                "hierarchy_ecl": concept_count > 0,
                "search": sidecar_enabled and bool(sidecar_count),
                "grounding": sidecar_enabled and bool(sidecar_count),
                "reviewed_grounding": concept_count > 0,
                "value_sets": concept_count > 0,
                "retrieval_benchmark": sidecar_enabled and bool(sidecar_count),
            },
        }

    async def snomed_capabilities(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Describe the terminology contract actually supported by this activation."""
        cfg = _config(ctx, self.manifest)
        release_id = _terminology_release_id(cfg, payload)
        release = cfg.get("release") if isinstance(cfg.get("release"), dict) else {}
        languages = [str(value).lower() for value in (cfg.get("languages") or []) if str(value).strip()]
        readiness = None
        if bool(payload.get("include_readiness", payload.get("includeReadiness", True))):
            readiness = await self.snomed_readiness(ctx, {"release_id": release_id})
        return {
            "ok": True,
            "provider": {"type": "kehrnel-strategy", "strategyId": self.manifest.id},
            "system": SNOMED_SYSTEM_URI,
            "releaseId": release_id,
            "version": f"{SNOMED_SYSTEM_URI}/version/{release_id}",
            "label": release.get("label"),
            "languages": languages,
            "operations": [
                "lookup",
                "validate-code",
                "subsumes",
                "expand",
                "search",
                "ecl",
                "ground",
                "ground-review",
                "grounded-corpus",
                "history",
                "suggest",
                "benchmark-retrieval",
            ],
            "domainOperations": sorted(_KNOWN_OPS),
            "releaseLifecycle": {
                "automaticIngestOnActivation": False,
                "stagingDirectory": str(_local_release_dir(cfg).resolve()),
                "operations": [
                    "snomed_list_releases",
                    "snomed_inspect_release",
                    "snomed_diff_release",
                    "snomed_ingest_release",
                    "snomed_rebuild_sidecar",
                    "snomed_ensure_indexes",
                ],
            },
            "ecl": {"supported": True, "subset": _supported_ecl_subset()},
            "readiness": readiness,
        }

    async def snomed_validate_code(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Validate a SNOMED code and optional display against one activated release."""
        cfg = _config(ctx, self.manifest)
        system = _validate_snomed_system(payload)
        release_id = _terminology_release_id(cfg, payload)
        code = str(payload.get("code") or payload.get("concept_id") or payload.get("conceptId") or "").strip()
        if not code:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="code is required")
        language = str(payload.get("language") or payload.get("display_language") or "").strip().lower() or None
        supplied_display = str(payload.get("display") or "").strip() or None
        concepts_collection, _, _ = _collections(cfg)
        concept = await _db(ctx)[concepts_collection].find_one(
            {"releaseId": release_id, "conceptId": code},
            {
                "_id": 0,
                "conceptId": 1,
                "active": 1,
                "effectiveTime": 1,
                "moduleId": 1,
                "definitionStatusId": 1,
                "descriptions": 1,
            },
        )
        if not concept:
            return {
                "ok": True,
                "valid": False,
                "system": system,
                "version": f"{SNOMED_SYSTEM_URI}/version/{release_id}",
                "releaseId": release_id,
                "code": code,
                "message": f"Code {code} was not found in SNOMED CT release {release_id}.",
                "issues": [{"code": "not-found", "severity": "error"}],
            }

        displays = _display_candidates(concept, language)
        canonical_display = displays[0] if displays else None
        is_active = bool(concept.get("active"))
        display_valid = supplied_display is None or normalize_text(supplied_display) in {
            normalize_text(candidate) for candidate in displays
        }
        valid = is_active and display_valid
        issues: list[dict[str, str]] = []
        if not is_active:
            issues.append({"code": "inactive", "severity": "error"})
        if not display_valid:
            issues.append({"code": "display-mismatch", "severity": "error"})
        message = None
        if not is_active:
            message = f"Code {code} is inactive in SNOMED CT release {release_id}."
        elif not display_valid:
            message = f"Display {supplied_display!r} does not match an active description for code {code}."
        return {
            "ok": True,
            "valid": valid,
            "system": system,
            "version": f"{SNOMED_SYSTEM_URI}/version/{release_id}",
            "releaseId": release_id,
            "code": code,
            "display": canonical_display,
            "suppliedDisplay": supplied_display,
            "inactive": not is_active,
            "message": message,
            "issues": issues,
            "concept": concept,
        }

    async def snomed_subsumes(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Evaluate the standard SNOMED subsumption relationship between two codes."""
        cfg = _config(ctx, self.manifest)
        system = _validate_snomed_system(payload)
        release_id = _terminology_release_id(cfg, payload)
        code_a = str(payload.get("code_a") or payload.get("codeA") or "").strip()
        code_b = str(payload.get("code_b") or payload.get("codeB") or "").strip()
        if not code_a or not code_b:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="code_a and code_b are required")
        concepts_collection, _, _ = _collections(cfg)
        projection = {"_id": 0, "conceptId": 1, "active": 1, "inferredAncestorIds": 1}
        collection = _db(ctx)[concepts_collection]
        concept_a = await collection.find_one({"releaseId": release_id, "conceptId": code_a}, projection)
        concept_b = concept_a if code_a == code_b else await collection.find_one(
            {"releaseId": release_id, "conceptId": code_b}, projection
        )
        missing = [code for code, concept in ((code_a, concept_a), (code_b, concept_b)) if not concept]
        if missing:
            return {
                "ok": True,
                "valid": False,
                "system": system,
                "version": f"{SNOMED_SYSTEM_URI}/version/{release_id}",
                "releaseId": release_id,
                "codeA": code_a,
                "codeB": code_b,
                "outcome": None,
                "message": f"Unknown SNOMED CT code(s): {', '.join(missing)}.",
                "issues": [{"code": "not-found", "severity": "error", "details": code} for code in missing],
            }

        if code_a == code_b:
            outcome = "equivalent"
        elif code_a in set(concept_b.get("inferredAncestorIds") or []):
            outcome = "subsumes"
        elif code_b in set(concept_a.get("inferredAncestorIds") or []):
            outcome = "subsumed-by"
        else:
            outcome = "not-subsumed"
        return {
            "ok": True,
            "valid": True,
            "system": system,
            "version": f"{SNOMED_SYSTEM_URI}/version/{release_id}",
            "releaseId": release_id,
            "codeA": code_a,
            "codeB": code_b,
            "outcome": outcome,
            "issues": [],
        }

    async def snomed_lookup(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self.execute_query(ctx, await self.compile_query(ctx, "snomedct", {"mode": "lookup", **payload}))
        return {"ok": True, "concept": result.rows[0] if result.rows else None, "explain": result.explain}

    async def snomed_search(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self.execute_query(ctx, await self.compile_query(ctx, "snomedct", {"mode": "search", **payload}))
        return {"ok": True, "matches": result.rows, "page": (result.explain or {}).get("page"), "explain": result.explain}

    async def snomed_suggest(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Return bounded typeahead suggestions, de-duplicated by concept."""
        cfg = _config(ctx, self.manifest)
        requested = _limit(cfg, payload)
        response = await self.snomed_search(
            ctx,
            {
                **payload,
                "offset": 0,
                "limit": min(requested * 4, int((cfg.get("search") or {}).get("max_limit") or 100)),
                "prefix_only": True,
            },
        )
        suggestions: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in response.get("matches") or []:
            concept_id = str(row.get("conceptId") or "")
            if not concept_id or concept_id in seen:
                continue
            seen.add(concept_id)
            suggestions.append(row)
            if len(suggestions) >= requested:
                break
        return {
            "ok": True,
            "suggestions": suggestions,
            "matches": suggestions,
            "returned": len(suggestions),
            "explain": response.get("explain"),
        }

    async def snomed_concept_history(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Show the same concept across the releases retained in the tenant database."""
        cfg = _config(ctx, self.manifest)
        concept_id = _concept_id(payload)
        offset, limit = _page(cfg, payload)
        language = str(payload.get("language") or (cfg.get("search") or {}).get("default_language") or "es").lower()
        concepts_collection, _, _ = _collections(cfg)
        cursor = (
            _db(ctx)[concepts_collection]
            .find({"conceptId": concept_id}, _projection_for_concept_summary())
            .sort([("releaseId", DESCENDING), ("effectiveTime", DESCENDING)])
            .skip(offset)
            .limit(limit + 1)
        )
        rows = await cursor.to_list(length=limit + 1)
        rows, page = _page_metadata(rows, offset, limit)
        history = []
        for row in rows:
            displays = _display_candidates(row, language)
            history.append(
                {
                    **row,
                    "preferredTerm": displays[0] if displays else concept_id,
                    "parentCount": len(row.get("inferredParentIds") or []),
                    "childCount": len(row.get("inferredChildIds") or []),
                    "refsetCount": len(row.get("memberOfRefsetIds") or []),
                }
            )
        return {
            "ok": True,
            "conceptId": concept_id,
            "language": language,
            "history": history,
            "entries": history,
            "page": page,
            "explain": {
                "collection": concepts_collection,
                "filter": {"conceptId": concept_id},
                "sort": {"releaseId": -1, "effectiveTime": -1},
            },
        }

    async def snomed_hybrid_search(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _hybrid_search_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        offset, limit = _page(cfg, payload)
        rows, page = _page_metadata(rows, offset, limit)
        return {"ok": True, "matches": rows, "page": page, "explain": explain}

    async def snomed_concept_children(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _children_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        rows, page = _page_metadata(rows, *_page(cfg, payload))
        return {"ok": True, "concepts": rows, "matches": rows, "page": page, "explain": {**explain, "collection": collection}}

    async def snomed_concept_descendants(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _descendants_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        rows, page = _page_metadata(rows, *_page(cfg, payload))
        return {"ok": True, "concepts": rows, "matches": rows, "page": page, "explain": {**explain, "collection": collection}}

    async def snomed_concept_ancestors(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _ancestors_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        rows, page = _page_metadata(rows, *_page(cfg, payload))
        return {"ok": True, "concepts": rows, "matches": rows, "page": page, "explain": {**explain, "collection": collection}}

    async def snomed_expand_value_set(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        value_set = None
        if payload.get("url"):
            value_set_filter: dict[str, Any] = {"url": str(payload["url"]).strip()}
            if payload.get("value_set_version") or payload.get("valueSetVersion"):
                value_set_filter["version"] = str(
                    payload.get("value_set_version") or payload.get("valueSetVersion")
                ).strip()
            value_set = await _db(ctx)[_value_sets_collection(cfg)].find_one(
                value_set_filter, {"_id": 0}, sort=[("updatedAt", DESCENDING)]
            )
            if not value_set:
                raise KehrnelError(
                    code="SNOMED_VALUE_SET_NOT_FOUND",
                    status=404,
                    message=f"ValueSet {payload['url']!r} is not registered for this tenant.",
                )
            definition = value_set.get("definition") or {}
            payload = {**payload, "release_id": value_set.get("releaseId") or payload.get("release_id")}
            if definition.get("type") == "ecl":
                payload["expression"] = definition.get("expression")
            else:
                concept_ids = list(definition.get("concepts") or [])
                concepts_collection, _, _ = _collections(cfg)
                offset, limit = _page(cfg, payload)
                pipeline = [
                    {
                        "$match": {
                            "releaseId": _release_id(cfg, payload),
                            "active": True,
                            "conceptId": {"$in": concept_ids},
                        }
                    },
                    {"$sort": {"conceptId": ASCENDING}},
                    {"$skip": offset},
                    {"$limit": limit + 1},
                    {"$project": _projection_for_concept_summary()},
                ]
                rows = await _aggregate(ctx, concepts_collection, pipeline)
                rows, page = _page_metadata(rows, offset, limit)
                return {
                    "ok": True,
                    "url": value_set.get("url"),
                    "valueSetVersion": value_set.get("version"),
                    "releaseId": value_set.get("releaseId"),
                    "definitionDigest": value_set.get("definitionDigest"),
                    "concepts": rows,
                    "matches": rows,
                    "totalDefined": len(concept_ids),
                    "truncated": len(concept_ids) > len(rows),
                    "page": page,
                    "explain": {"collection": concepts_collection, "pipeline": pipeline, "definition": definition},
                }
        expression = str(payload.get("expression") or payload.get("ecl") or "").strip()
        if not expression and (payload.get("concept_id") or payload.get("conceptId")):
            operator = "<<" if bool(payload.get("include_self") or payload.get("includeSelf", True)) else "<"
            expression = f"{operator} {_concept_id(payload)}"
        if not expression:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="expression or concept_id is required")
        result = await self.execute_query(ctx, await self.compile_query(ctx, "snomedct", {"mode": "ecl", **payload, "expression": expression}))
        response = {
            "ok": True,
            "expression": expression,
            "concepts": result.rows,
            "matches": result.rows,
            "page": (result.explain or {}).get("page"),
            "explain": result.explain,
        }
        if value_set:
            response.update(
                {
                    "url": value_set.get("url"),
                    "valueSetVersion": value_set.get("version"),
                    "releaseId": value_set.get("releaseId"),
                    "definitionDigest": value_set.get("definitionDigest"),
                }
            )
        return response

    async def snomed_put_value_set(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Register an immutable named ValueSet backed by ECL or explicit concepts."""
        cfg = _config(ctx, self.manifest)
        url = str(payload.get("url") or "").strip()
        version = str(payload.get("version") or "").strip()
        if not url or not version:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="url and version are required")
        status = str(payload.get("status") or "active").strip().lower()
        if status not in {"draft", "active", "retired", "unknown"}:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="status must be draft, active, retired, or unknown",
            )
        definition = _value_set_definition(payload)
        # ``version`` identifies the ValueSet definition, not the SNOMED
        # edition. The terminology release is selected independently.
        release_id = _release_id(cfg, payload)
        digest_payload = {
            "url": url,
            "version": version,
            "releaseId": release_id,
            "definition": definition,
        }
        digest = hashlib.sha256(
            json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        collection = _db(ctx)[_value_sets_collection(cfg)]
        existing = await collection.find_one({"url": url, "version": version}, {"_id": 0})
        if existing:
            if existing.get("definitionDigest") == digest:
                return {"ok": True, "created": False, "valueSet": existing}
            raise KehrnelError(
                code="SNOMED_VALUE_SET_VERSION_CONFLICT",
                status=409,
                message="This ValueSet URL and version already exists with a different definition.",
                details={"url": url, "version": version},
            )
        now = datetime.now(timezone.utc).isoformat()
        document = {
            "url": url,
            "version": version,
            "name": str(payload.get("name") or url.rsplit("/", 1)[-1]).strip(),
            "status": status,
            "system": SNOMED_SYSTEM_URI,
            "releaseId": release_id,
            "definition": definition,
            "definitionDigest": digest,
            "createdAt": now,
            "updatedAt": now,
        }
        try:
            await collection.insert_one(document)
        except DuplicateKeyError as exc:
            winner = await collection.find_one(
                {"url": url, "version": version}, {"_id": 0}
            )
            if winner and winner.get("definitionDigest") == digest:
                return {"ok": True, "created": False, "valueSet": winner}
            raise KehrnelError(
                code="SNOMED_VALUE_SET_VERSION_CONFLICT",
                status=409,
                message="This ValueSet URL and version was concurrently registered with a different definition.",
                details={"url": url, "version": version},
            ) from exc
        return {"ok": True, "created": True, "valueSet": {key: value for key, value in document.items() if key != "_id"}}

    async def snomed_list_value_sets(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        limit = _limit(cfg, payload)
        query: dict[str, Any] = {}
        if payload.get("status"):
            query["status"] = str(payload["status"])
        cursor = _db(ctx)[_value_sets_collection(cfg)].find(query, {"_id": 0}).sort(
            [("url", ASCENDING), ("version", DESCENDING)]
        ).limit(limit)
        rows = await cursor.to_list(length=limit)
        return {"ok": True, "valueSets": rows, "returned": len(rows), "limit": limit}

    async def snomed_validate_value_set(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        url = str(payload.get("url") or "").strip()
        if not url:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="url is required")
        value_set_filter: dict[str, Any] = {"url": url}
        if payload.get("value_set_version") or payload.get("valueSetVersion"):
            value_set_filter["version"] = str(payload.get("value_set_version") or payload.get("valueSetVersion"))
        value_set = await _db(ctx)[_value_sets_collection(cfg)].find_one(
            value_set_filter, {"_id": 0}, sort=[("updatedAt", DESCENDING)]
        )
        if not value_set:
            raise KehrnelError(code="SNOMED_VALUE_SET_NOT_FOUND", status=404, message=f"ValueSet {url!r} is not registered for this tenant.")
        # The named ValueSet owns its release. Resolve it before validating the
        # code so membership cannot accidentally be evaluated against the
        # strategy's current default release.
        validation_payload = {
            **payload,
            "release_id": value_set.get("releaseId") or payload.get("release_id"),
        }
        code_result = await self.snomed_validate_code(ctx, validation_payload)
        if not code_result.get("valid"):
            return {
                **code_result,
                "inValueSet": False,
                "url": value_set.get("url"),
                "valueSetVersion": value_set.get("version"),
                "definitionDigest": value_set.get("definitionDigest"),
            }
        definition = value_set.get("definition") or {}
        code = str(payload.get("code") or "").strip()
        if definition.get("type") == "concepts":
            member = code in set(map(str, definition.get("concepts") or []))
            explain = {"mode": "explicit-concepts", "definitionDigest": value_set.get("definitionDigest")}
        else:
            compiled = _compile_ecl(
                cfg,
                {
                    "expression": definition.get("expression"),
                    "release_id": value_set.get("releaseId"),
                    "limit": 1,
                },
            )
            pipeline = list(compiled["pipeline"])
            limit_index = next((index for index, stage in enumerate(pipeline) if "$limit" in stage), len(pipeline))
            pipeline.insert(limit_index, {"$match": {"conceptId": code}})
            rows = await _aggregate(ctx, compiled["collection"], pipeline)
            member = bool(rows)
            explain = {**compiled, "pipeline": pipeline}
        return {
            **code_result,
            "valid": bool(code_result.get("valid") and member),
            "inValueSet": member,
            "url": value_set.get("url"),
            "valueSetVersion": value_set.get("version"),
            "definitionDigest": value_set.get("definitionDigest"),
            "message": None if member else f"Code {code} is not a member of ValueSet {url}.",
            "explain": explain,
        }

    async def snomed_relationship_search(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _relationship_search_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        rows, page = _page_metadata(rows, *_page(cfg, payload))
        return {"ok": True, "concepts": rows, "matches": rows, "page": page, "explain": {**explain, "collection": collection}}

    async def snomed_semantic_facets(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _semantic_facets_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        facets = rows[0] if rows else {"areaTags": [], "semanticTags": [], "topRoots": []}
        return {"ok": True, "facets": facets, "explain": {**explain, "collection": collection}}

    async def snomed_ecl(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self.execute_query(ctx, await self.compile_query(ctx, "snomedct", {"mode": "ecl", **payload}))
        return {"ok": True, "matches": result.rows, "page": (result.explain or {}).get("page"), "explain": result.explain}

    async def snomed_parse_ecl(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        expression = str(payload.get("expression") or payload.get("ecl") or "").strip()
        return {"ok": True, "expression": expression, "ast": _parse_ecl(expression)}

    async def snomed_compile_ecl(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        compiled = _compile_ecl(cfg, payload)
        return {
            "ok": True,
            "expression": str(payload.get("expression") or payload.get("ecl") or "").strip(),
            "collection": compiled["collection"],
            "pipeline": compiled["pipeline"],
            "ast": compiled["ast"],
            "warnings": compiled.get("warnings", []),
            "supportedSubset": compiled.get("supportedSubset", []),
            "planner": compiled.get("planner"),
        }

    async def snomed_ground_note(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        mentions = payload.get("mentions")
        if not mentions and payload.get("text"):
            mentions = [part.strip() for part in re.split(r"[,;\n]", str(payload.get("text"))) if part.strip()]
        if not isinstance(mentions, list) or not mentions:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="mentions or text is required")
        limit_per_mention = int(payload.get("limit_per_mention") or 5)
        grounded = []
        for mention in mentions:
            response = await self.snomed_search(ctx, {**payload, "q": str(mention), "limit": limit_per_mention})
            grounded.append({"mention": str(mention), "candidates": response.get("matches", [])})
        return {"ok": True, "grounded": grounded}

    async def snomed_save_grounding_review(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Persist reviewer decisions without retaining source text by default."""
        cfg = _config(ctx, self.manifest)
        release_id = _release_id(cfg, payload)
        source_ref = str(payload.get("source_ref") or payload.get("sourceRef") or "").strip()
        if not source_ref:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="source_ref is required")
        grounding_cfg = cfg.get("grounding") if isinstance(cfg.get("grounding"), dict) else {}
        max_codings = max(1, min(int(grounding_cfg.get("max_codings_per_review") or 50), 250))
        raw_codings = payload.get("codings")
        if not isinstance(raw_codings, list) or not raw_codings:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="codings must be a non-empty list")
        if len(raw_codings) > max_codings:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message=f"A grounding review supports at most {max_codings} codings.",
            )

        concepts_collection, _, _ = _collections(cfg)
        concepts = _db(ctx)[concepts_collection]
        codings: list[dict[str, Any]] = []
        for raw in raw_codings:
            if not isinstance(raw, dict):
                raise KehrnelError(code="INVALID_INPUT", status=400, message="Each coding must be an object")
            concept_id = str(raw.get("concept_id") or raw.get("conceptId") or "").strip()
            if not re.fullmatch(r"[0-9]{6,18}", concept_id):
                raise KehrnelError(code="INVALID_INPUT", status=400, message=f"Invalid SNOMED CT concept id {concept_id!r}")
            status = str(raw.get("status") or raw.get("decision") or "accepted").strip().lower()
            if status not in {"accepted", "rejected"}:
                raise KehrnelError(code="INVALID_INPUT", status=400, message="Coding status must be accepted or rejected")
            concept = await concepts.find_one(
                {"releaseId": release_id, "conceptId": concept_id},
                {"_id": 0, "conceptId": 1, "active": 1, "inferredAncestorIds": 1, "descriptions": 1},
            )
            if not concept:
                raise KehrnelError(
                    code="SNOMED_CONCEPT_NOT_FOUND",
                    status=404,
                    message=f"Concept {concept_id} was not found in release {release_id}.",
                )
            ancestor_ids = [concept_id, *[str(value) for value in (concept.get("inferredAncestorIds") or [])]]
            codings.append(
                {
                    "conceptId": concept_id,
                    "display": str(raw.get("display") or (_display_candidates(concept, payload.get("language")) or [concept_id])[0]),
                    "status": status,
                    "role": str(raw.get("role") or "secondary"),
                    "target": str(raw.get("target") or "Coding"),
                    "assertion": str(raw.get("assertion") or "present"),
                    "subject": str(raw.get("subject") or "patient"),
                    "evidence": raw.get("evidence"),
                    "ancestorIds": list(dict.fromkeys(ancestor_ids)),
                }
            )

        source_text = str(payload.get("text") or "")
        text_hash = hashlib.sha256(source_text.encode("utf-8")).hexdigest() if source_text else None
        reviewed_at = datetime.now(timezone.utc).isoformat()
        digest_payload = {"sourceRef": source_ref, "releaseId": release_id, "codings": codings, "textHash": text_hash}
        digest = hashlib.sha256(json.dumps(digest_payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
        review_id = str(payload.get("review_id") or payload.get("reviewId") or f"grd_{digest[:24]}").strip()
        document: dict[str, Any] = {
            "schemaVersion": 1,
            "reviewId": review_id,
            "sourceRef": source_ref,
            "sourceTextHash": text_hash,
            "releaseId": release_id,
            "language": str(payload.get("language") or (cfg.get("search") or {}).get("default_language") or "es"),
            "reviewer": str(payload.get("reviewer") or "").strip() or None,
            "codings": codings,
            "acceptedAncestorIds": sorted(
                {
                    ancestor_id
                    for coding in codings
                    if coding["status"] == "accepted"
                    and coding["assertion"] == "present"
                    and coding["subject"] == "patient"
                    for ancestor_id in coding["ancestorIds"]
                }
            ),
            "definitionDigest": digest,
            "reviewedAt": reviewed_at,
        }
        if bool(grounding_cfg.get("store_source_text", False)) and source_text:
            document["sourceText"] = source_text
        collection = _db(ctx)[_grounding_collection(cfg)]
        existing = await collection.find_one({"reviewId": review_id}, {"_id": 0})
        if existing:
            if existing.get("definitionDigest") == digest:
                return {"ok": True, "created": False, "review": existing}
            raise KehrnelError(
                code="SNOMED_GROUNDING_REVIEW_CONFLICT",
                status=409,
                message="This review_id already exists with different reviewer decisions.",
            )
        try:
            await collection.insert_one(document)
        except DuplicateKeyError as exc:
            raise KehrnelError(
                code="SNOMED_GROUNDING_REVIEW_CONFLICT",
                status=409,
                message="This review_id was concurrently created.",
            ) from exc
        return {"ok": True, "created": True, "review": document}

    async def snomed_query_grounded_corpus(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Query accepted, present, patient codings by exact concept or ancestor."""
        cfg = _config(ctx, self.manifest)
        concept_id = _concept_id(payload)
        offset, limit = _page(cfg, payload)
        elem_match: dict[str, Any] = {"ancestorIds": concept_id, "status": "accepted"}
        if not bool(payload.get("include_non_present", False)):
            elem_match["assertion"] = "present"
        if not bool(payload.get("include_family", False)):
            elem_match["subject"] = "patient"
        use_materialized_path = not bool(payload.get("include_non_present", False)) and not bool(payload.get("include_family", False))
        match = (
            {"acceptedAncestorIds": concept_id}
            if use_materialized_path
            else {"codings": {"$elemMatch": elem_match}}
        )
        projection: dict[str, Any] = {
            "_id": 0,
            "reviewId": 1,
            "sourceRef": 1,
            "releaseId": 1,
            "language": 1,
            "reviewer": 1,
            "reviewedAt": 1,
            "codings": 1,
        }
        if bool((cfg.get("grounding") or {}).get("store_source_text", False)) and bool(payload.get("include_text", False)):
            projection["sourceText"] = 1
        collection_name = _grounding_collection(cfg)
        pipeline = [
            {"$match": match},
            {"$sort": {"reviewedAt": DESCENDING, "reviewId": ASCENDING}},
            {"$skip": offset},
            {"$limit": limit + 1},
            {"$project": projection},
        ]
        rows = await _aggregate(ctx, collection_name, pipeline)
        rows, page = _page_metadata(rows, offset, limit)
        return {
            "ok": True,
            "conceptId": concept_id,
            "reviews": rows,
            "matches": rows,
            "page": page,
            "explain": {"collection": collection_name, "filter": match, "pipeline": pipeline},
        }

    async def snomed_benchmark_retrieval(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        """Evaluate deterministic terminology retrieval against governed cases."""
        cases = payload.get("cases")
        if not isinstance(cases, list) or not cases:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="cases must be a non-empty list")
        if len(cases) > 100:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="A benchmark run supports at most 100 cases")
        top_k = max(1, min(int(payload.get("top_k") or payload.get("topK") or 10), 100))
        outcomes: list[dict[str, Any]] = []
        reciprocal_rank = 0.0
        hits = 0
        for index, case in enumerate(cases):
            if not isinstance(case, dict):
                raise KehrnelError(code="INVALID_INPUT", status=400, message="Each benchmark case must be an object")
            query = str(case.get("query") or case.get("q") or "").strip()
            expected = {str(value) for value in (case.get("expected_concept_ids") or case.get("expectedConceptIds") or []) if str(value)}
            if not query or not expected:
                raise KehrnelError(code="INVALID_INPUT", status=400, message="Each benchmark case requires query and expected_concept_ids")
            response = await self.snomed_search(
                ctx,
                {
                    "q": query,
                    "language": case.get("language") or payload.get("language"),
                    "release_id": case.get("release_id") or payload.get("release_id"),
                    "limit": top_k,
                    "offset": 0,
                },
            )
            concept_ids = [str(row.get("conceptId") or "") for row in response.get("matches") or []]
            rank = next((rank for rank, concept_id in enumerate(concept_ids, start=1) if concept_id in expected), None)
            if rank:
                hits += 1
                reciprocal_rank += 1.0 / rank
            outcomes.append(
                {
                    "id": str(case.get("id") or f"case-{index + 1}"),
                    "query": query,
                    "expectedConceptIds": sorted(expected),
                    "returnedConceptIds": concept_ids,
                    "rank": rank,
                    "hit": rank is not None,
                }
            )
        total = len(outcomes)
        return {
            "ok": True,
            "benchmark": "snomed-terminology-retrieval",
            "topK": top_k,
            "summary": {"cases": total, "hits": hits, "hitRate": hits / total, "meanReciprocalRank": reciprocal_rank / total},
            "cases": outcomes,
            "executedAt": datetime.now(timezone.utc).isoformat(),
        }
