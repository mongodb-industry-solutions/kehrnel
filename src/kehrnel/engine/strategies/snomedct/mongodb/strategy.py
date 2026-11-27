"""SNOMED CT on MongoDB strategy pack."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import ASCENDING, DESCENDING, ReplaceOne

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.plugin import StrategyPlugin
from kehrnel.engine.core.types import ApplyPlan, ApplyResult, QueryPlan, QueryResult, StrategyContext, TransformResult
from kehrnel.engine.domains.snomedct import build_term_documents, iter_concepts_from_json, normalize_concept, normalize_text

PACK_ROOT = Path(__file__).resolve().parent
MANIFEST_PATH = PACK_ROOT / "manifest.json"
SCHEMA_PATH = PACK_ROOT / "schema.json"
DEFAULTS_PATH = PACK_ROOT / "defaults.json"


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
    "snomed_lookup",
    "snomed_search",
    "snomed_ecl",
    "snomed_parse_ecl",
    "snomed_compile_ecl",
    "snomed_hybrid_search",
    "snomed_concept_children",
    "snomed_concept_descendants",
    "snomed_concept_ancestors",
    "snomed_expand_value_set",
    "snomed_relationship_search",
    "snomed_semantic_facets",
    "snomed_ground_note",
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


def _limit(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> int:
    search = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    default_limit = int(search.get("default_limit") or 20)
    max_limit = int(search.get("max_limit") or 100)
    requested = int((payload or {}).get("limit") or default_limit)
    return max(1, min(requested, max_limit))


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
    raw = (payload or {}).get("local_dir") or source.get("local_dir") or ".kehrnel/snomedct/releases"
    return Path(str(raw)).expanduser()


def _list_release_files(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> list[Path]:
    source = _source_config(cfg)
    local_dir = _local_release_dir(cfg, payload)
    pattern = str((payload or {}).get("file_pattern") or source.get("file_pattern") or "*.json")
    if not local_dir.exists():
        return []
    return sorted(path for path in local_dir.glob(pattern) if path.is_file())


def _resolve_release_path(cfg: dict[str, Any], payload: dict[str, Any] | None = None) -> Path:
    payload = payload or {}
    if payload.get("path"):
        path = Path(str(payload["path"])).expanduser()
        if not path.exists():
            raise KehrnelError(code="RELEASE_FILE_NOT_FOUND", status=404, message=f"SNOMED CT release file not found: {path}")
        return path

    source = _source_config(cfg)
    file_name = str(payload.get("file_name") or source.get("file_name") or "").strip()
    local_dir = _local_release_dir(cfg, payload)
    if file_name:
        path = local_dir / file_name
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
    limit = _limit(cfg, query)
    pattern = re.escape(normalized)
    pipeline = [
        {
            "$match": {
                "releaseId": release_id,
                "languageCode": language,
                "active": True,
                "conceptActive": True,
                "normalizedTerm": {"$regex": pattern, "$options": "i"},
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
        {"$limit": limit},
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
    limit = _limit(cfg, query)
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
        {"$limit": limit},
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
        {"$limit": limit},
    ]


def _compile_ecl(cfg: dict[str, Any], query: dict[str, Any]) -> dict[str, Any]:
    concepts_collection, _, _ = _collections(cfg)
    release_id = _release_id(cfg, query)
    expression = str(query.get("expression") or query.get("ecl") or "").strip()
    limit = _limit(cfg, query)
    ast = _parse_ecl(expression)
    runtime_pipeline = _simple_ancestor_focus_pipeline(concepts_collection, release_id, ast, limit)
    if runtime_pipeline is not None:
        return {
            "collection": concepts_collection,
            "pipeline": runtime_pipeline,
            "ast": ast,
            "warnings": [],
            "supportedSubset": _supported_ecl_subset(),
            "planner": "target_first_ancestor_lookup",
        }
    compiled_match, warnings = _ast_to_match(ast)
    match: dict[str, Any] = {"releaseId": release_id, "active": True}
    if compiled_match:
        match = {"$and": [match, compiled_match]}
    pipeline = [
        {"$match": match},
        {"$limit": limit},
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
    limit = _limit(cfg, payload)
    pipeline = [
        {"$match": {"releaseId": release_id, "active": True, "inferredParentIds": concept_id}},
        {"$sort": {"conceptId": ASCENDING}},
        {"$limit": limit},
        {"$project": _projection_for_concept_summary()},
    ]
    return concepts_collection, pipeline, {"mode": "children", "conceptId": concept_id, "releaseId": release_id, "pipeline": pipeline}


def _descendants_pipeline(cfg: dict[str, Any], payload: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    concepts_collection, _, _ = _collections(cfg)
    release_id = _release_id(cfg, payload)
    concept_id = _concept_id(payload)
    limit = _limit(cfg, payload)
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
        {"$limit": limit},
        {"$project": _projection_for_concept_summary()},
    ]
    return concepts_collection, pipeline, {"mode": "descendants", "conceptId": concept_id, "releaseId": release_id, "includeSelf": include_self, "pipeline": pipeline}


def _ancestors_pipeline(cfg: dict[str, Any], payload: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    concepts_collection, _, _ = _collections(cfg)
    release_id = _release_id(cfg, payload)
    concept_id = _concept_id(payload)
    limit = _limit(cfg, payload)
    include_self = bool(payload.get("include_self") or payload.get("includeSelf"))
    ast = {"type": "focus", "operator": ">>" if include_self else ">", "conceptId": concept_id}
    pipeline = _simple_ancestor_focus_pipeline(concepts_collection, release_id, ast, limit) or []
    return concepts_collection, pipeline, {"mode": "ancestors", "conceptId": concept_id, "releaseId": release_id, "includeSelf": include_self, "pipeline": pipeline}


def _relationship_search_pipeline(cfg: dict[str, Any], payload: dict[str, Any]) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    concepts_collection, _, _ = _collections(cfg)
    release_id = _release_id(cfg, payload)
    type_id = str(payload.get("type_id") or payload.get("typeId") or "").strip()
    destination_id = str(payload.get("destination_id") or payload.get("destinationId") or "").strip()
    if not type_id and not destination_id:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="type_id or destination_id is required")
    limit = _limit(cfg, payload)
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
        {"$limit": limit},
        {"$project": _projection_for_concept_summary()},
    ]
    return concepts_collection, pipeline, {
        "mode": "relationship_search",
        "releaseId": release_id,
        "typeId": type_id or None,
        "destinationId": destination_id or None,
        "usesRelationshipAttributeKey": bool(type_id and destination_id),
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
        _release_id(cfg)
        languages = cfg.get("languages") or []
        if not isinstance(languages, list) or not languages:
            raise KehrnelError(code="INVALID_CONFIG", status=400, message="languages must be a non-empty list")
        return True

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        cfg = _config(ctx, self.manifest)
        concepts, terms, sidecar_enabled = _collections(cfg)
        return ApplyPlan(artifacts={"action": "ensure_indexes", "collections": [concepts] + ([terms] if sidecar_enabled else [])})

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
        if mode == "search":
            collection, pipeline = _search_pipeline(cfg, payload)
        elif mode in {"ecl", "subsumption"}:
            collection, pipeline, compiled = _ecl_pipeline(cfg, payload)
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
            plan={"collection": collection, "pipeline": pipeline, "mode": mode},
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
        sample = await db[concepts_collection].find_one(concept_filter, {"_id": 0, "conceptId": 1, "releaseId": 1, "inferredDescendantIds": 1})
        descendants_retained = bool(sample and sample.get("inferredDescendantIds") is not None)
        return {
            "ok": True,
            "releaseId": release_id,
            "collections": {"concepts": concepts_collection, "terms": terms_collection if sidecar_enabled else None},
            "canonical": {"concept_count": concept_count, "active_count": active_count, "ready": concept_count > 0},
            "sidecar": {"enabled": sidecar_enabled, "term_count": sidecar_count, "ready": (sidecar_count or 0) > 0 if sidecar_enabled else False},
            "storage_policy": {"inferredDescendantIds_retained": descendants_retained},
            "features": {
                "lookup": concept_count > 0,
                "hierarchy_ecl": concept_count > 0,
                "search": sidecar_enabled and bool(sidecar_count),
                "grounding": sidecar_enabled and bool(sidecar_count),
            },
        }

    async def snomed_lookup(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self.execute_query(ctx, await self.compile_query(ctx, "snomedct", {"mode": "lookup", **payload}))
        return {"ok": True, "concept": result.rows[0] if result.rows else None, "explain": result.explain}

    async def snomed_search(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self.execute_query(ctx, await self.compile_query(ctx, "snomedct", {"mode": "search", **payload}))
        return {"ok": True, "matches": result.rows, "explain": result.explain}

    async def snomed_hybrid_search(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _hybrid_search_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        return {"ok": True, "matches": rows, "explain": explain}

    async def snomed_concept_children(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _children_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        return {"ok": True, "concepts": rows, "matches": rows, "explain": {**explain, "collection": collection}}

    async def snomed_concept_descendants(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _descendants_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        return {"ok": True, "concepts": rows, "matches": rows, "explain": {**explain, "collection": collection}}

    async def snomed_concept_ancestors(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _ancestors_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        return {"ok": True, "concepts": rows, "matches": rows, "explain": {**explain, "collection": collection}}

    async def snomed_expand_value_set(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        expression = str(payload.get("expression") or payload.get("ecl") or "").strip()
        if not expression and (payload.get("concept_id") or payload.get("conceptId")):
            operator = "<<" if bool(payload.get("include_self") or payload.get("includeSelf", True)) else "<"
            expression = f"{operator} {_concept_id(payload)}"
        if not expression:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="expression or concept_id is required")
        result = await self.execute_query(ctx, await self.compile_query(ctx, "snomedct", {"mode": "ecl", **payload, "expression": expression}))
        return {
            "ok": True,
            "expression": expression,
            "concepts": result.rows,
            "matches": result.rows,
            "explain": result.explain,
        }

    async def snomed_relationship_search(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _relationship_search_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        return {"ok": True, "concepts": rows, "matches": rows, "explain": {**explain, "collection": collection}}

    async def snomed_semantic_facets(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = _config(ctx, self.manifest)
        collection, pipeline, explain = _semantic_facets_pipeline(cfg, payload)
        rows = await _aggregate(ctx, collection, pipeline)
        facets = rows[0] if rows else {"areaTags": [], "semanticTags": [], "topRoots": []}
        return {"ok": True, "facets": facets, "explain": {**explain, "collection": collection}}

    async def snomed_ecl(self, ctx: StrategyContext, payload: dict[str, Any]) -> dict[str, Any]:
        result = await self.execute_query(ctx, await self.compile_query(ctx, "snomedct", {"mode": "ecl", **payload}))
        return {"ok": True, "matches": result.rows, "explain": result.explain}

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
