"""Lexical tiering, query variants, vector retrieval, and rank fusion.

The baseline strategy searches with a normalized regex, which is deterministic
and works on every MongoDB deployment. This module adds the retrieval quality
layer on top of it:

* **Query variants** — a small set of spelling/lemma variants so "diabetis" and
  "diabético" still reach "diabetes".
* **Tiered rerank** — an explicit, explainable ordering (exact SCTID down to
  fuzzy contains) instead of relying on the database's row order.
* **Vector retrieval** — an optional Atlas ``$vectorSearch`` branch, used only
  when the activation configures a vector index.
* **Rank fusion** — reciprocal rank fusion so a concept found by both branches
  outranks one found by only one, with optional cross-encoder reranking.
"""

from __future__ import annotations

import re
from typing import Any

from .model import normalize_text

# Small ES/EN normalizations applied per token before comparison.
_TOKEN_ALIASES = {
    "diabetis": "diabetes",
    "diabete": "diabetes",
    "hipertension": "hipertension",
    "obesidad": "obesidad",
    "cardiopatia": "cardiopatia",
}

_SUFFIX_RULES = (
    ("aciones", "acion"),
    ("ciones", "cion"),
    ("idades", "idad"),
    ("ments", "ment"),
    ("ing", ""),
    ("es", ""),
    ("s", ""),
)


def _lemma(token: str) -> str:
    """Very small, deterministic singularization (never a stemmer)."""
    if token in _TOKEN_ALIASES:
        return _TOKEN_ALIASES[token]
    for suffix, replacement in _SUFFIX_RULES:
        if len(token) > len(suffix) + 3 and token.endswith(suffix):
            return token[: -len(suffix)] + replacement
    return token


def build_search_query_variants(query: str, max_variants: int = 6) -> list[str]:
    """Return de-duplicated normalized spellings of a query, best first."""
    normalized = normalize_text(query)
    if not normalized:
        return []
    tokens = [token for token in normalized.split(" ") if token]
    variants = [normalized]

    lemma_tokens = [_lemma(token) for token in tokens]
    lemma_variant = " ".join(lemma_tokens)
    if lemma_variant != normalized:
        variants.append(lemma_variant)

    for index, token in enumerate(tokens):
        lemmatized = _lemma(token)
        if lemmatized != token:
            variant = " ".join(tokens[:index] + [lemmatized] + tokens[index + 1 :])
            if variant not in variants:
                variants.append(variant)

    if len(tokens) > 1:
        trimmed = " ".join(tokens[:-1])
        if trimmed not in variants:
            variants.append(trimmed)

    return variants[:max_variants]


def dedupe_by_concept(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the highest-ranked row per concept (the sidecar is term-level)."""
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows or []:
        concept_id = str(row.get("conceptId") or "")
        if not concept_id or concept_id in seen:
            continue
        seen.add(concept_id)
        deduped.append(row)
    return deduped


def _match_tier(row: dict[str, Any], normalized_query: str, variants: list[str]) -> tuple[int, str, str]:
    """Classify how a sidecar row matched the query.

    Returns ``(tier, reason, matched_text)``; lower tiers are stronger.
    """
    concept_id = str(row.get("conceptId") or "")
    if normalized_query and concept_id == normalized_query.strip():
        return 0, "SCTID exact", concept_id

    term = normalize_text(row.get("term"))
    display = normalize_text(row.get("displayTerm") or row.get("preferredTerm"))
    preferred = normalize_text(row.get("preferredTerm"))
    fsn = normalize_text(row.get("fsn"))
    synonyms = [normalize_text(value) for value in (row.get("synonyms") or []) if normalize_text(value)]

    if normalized_query and normalized_query == display:
        return 1, "Preferred term exact", row.get("displayTerm") or row.get("term") or ""
    if normalized_query and normalized_query == term:
        return 1, "Term exact", row.get("term") or ""
    if normalized_query and normalized_query == preferred:
        return 1, "Preferred term exact", row.get("preferredTerm") or ""

    for variant in variants:
        if variant == display or variant == term or variant == preferred:
            return 2, "Near-exact term", row.get("term") or ""

    if normalized_query and normalized_query in synonyms:
        return 3, "Synonym exact", row.get("term") or ""
    if normalized_query and normalized_query == fsn:
        return 4, "FSN exact", row.get("fsn") or ""

    for variant in variants:
        if variant in synonyms:
            return 5, "Synonym lemma", row.get("term") or ""

    if normalized_query and display.startswith(normalized_query):
        return 6, "Preferred term prefix", row.get("displayTerm") or row.get("term") or ""
    if normalized_query and term.startswith(normalized_query):
        return 6, "Term prefix", row.get("term") or ""
    if normalized_query and any(synonym.startswith(normalized_query) for synonym in synonyms):
        return 7, "Synonym prefix", row.get("term") or ""

    if normalized_query and (normalized_query in term or normalized_query in display):
        return 8, "Term contains query", row.get("term") or ""
    if normalized_query and normalized_query in fsn:
        return 9, "FSN contains query", row.get("fsn") or ""

    for variant in variants:
        if variant and (variant in term or variant in display or any(variant in synonym for synonym in synonyms)):
            return 10, "Lexical variant", row.get("term") or ""

    return 11, "Fuzzy lexical", row.get("term") or ""


def rerank_navigator_results(
    *,
    query: str,
    results: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    """Apply the tiered ordering and annotate each row with its match explanation."""
    normalized_query = normalize_text(query)
    variants = build_search_query_variants(query)
    annotated: list[dict[str, Any]] = []
    for index, row in enumerate(results or []):
        tier, reason, matched_text = _match_tier(row, normalized_query, variants)
        term = str(row.get("term") or "")
        length_distance = abs(len(normalize_text(term)) - len(normalized_query))
        annotated.append(
            {
                **row,
                "matchTier": tier,
                "matchReason": reason,
                "matchedText": matched_text or None,
                "_retrievalIndex": index,
                "_lengthDistance": length_distance,
            }
        )

    annotated.sort(
        key=lambda row: (
            int(row["matchTier"]),
            0 if row.get("preferred") or row.get("isPreferred") else 1,
            row["_lengthDistance"],
            -float(row.get("termRank") or 0),
            -float(row.get("score") or 0),
            str(row.get("term") or ""),
            str(row.get("conceptId") or ""),
        )
    )
    deduped = dedupe_by_concept(annotated)
    for row in deduped:
        row.pop("_lengthDistance", None)
        row.pop("_retrievalIndex", None)
    return deduped[: max(int(limit or 1), 1)]


def reciprocal_rank_fusion(
    *,
    lexical: list[dict[str, Any]],
    vector: list[dict[str, Any]],
    k: int = 60,
) -> list[dict[str, Any]]:
    """Fuse two ranked lists by rank position (RRF), one entry per concept."""
    table: dict[str, dict[str, Any]] = {}

    def add(rows: list[dict[str, Any]], engine: str) -> None:
        for index, row in enumerate(rows or []):
            concept_id = str(row.get("conceptId") or "")
            if not concept_id:
                continue
            entry = table.setdefault(
                concept_id,
                {"conceptId": concept_id, "rrf": 0.0, "engines": [], "lexical": None, "vector": None, "lexicalRank": None, "vectorRank": None},
            )
            entry["rrf"] += 1.0 / (k + index + 1)
            if engine not in entry["engines"]:
                entry["engines"].append(engine)
            entry[engine] = row
            entry[f"{engine}Rank"] = index + 1

    add(lexical, "lexical")
    add(vector, "vector")
    fused = sorted(table.values(), key=lambda entry: (-entry["rrf"], entry["conceptId"]))
    return [merge_fused_row(entry) for entry in fused]


def merge_fused_row(entry: dict[str, Any]) -> dict[str, Any]:
    """Merge the per-engine views of one concept into a single result row."""
    lexical = entry.get("lexical") or {}
    vector = entry.get("vector") or {}
    base = lexical or vector or {}
    engines = list(entry.get("engines") or [])
    vector_score = vector.get("score")
    return {
        "conceptId": entry["conceptId"],
        "term": base.get("term") or base.get("displayTerm") or vector.get("term") or entry["conceptId"],
        "displayTerm": base.get("displayTerm"),
        "preferredTerm": base.get("preferredTerm") or vector.get("preferredTerm"),
        "fsn": base.get("fsn") or vector.get("fsn"),
        "semanticTag": base.get("semanticTag") or vector.get("semanticTag"),
        "languageCode": base.get("languageCode") or vector.get("languageCode"),
        "ancestorIds": base.get("ancestorIds") or vector.get("ancestorIds") or [],
        "matchedText": lexical.get("matchedText"),
        "matchReason": lexical.get("matchReason") or ("Semantic" if engines == ["vector"] else None),
        "matchTier": lexical.get("matchTier"),
        "foundBy": engines,
        "lexicalRank": entry.get("lexicalRank"),
        "vectorRank": entry.get("vectorRank"),
        "vectorScore": vector_score if isinstance(vector_score, (int, float)) else None,
        "rrfScore": round(float(entry.get("rrf") or 0), 6),
        "rerankScore": None,
    }


def build_vector_search_pipeline(
    *,
    query: str,
    limit: int,
    index_name: str,
    path: str,
    num_candidates: int = 200,
    filter_expression: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Atlas ``$vectorSearch`` over the sidecar's embedded term text."""
    stage: dict[str, Any] = {
        "index": index_name,
        "path": path,
        "queryVector": query,
        "numCandidates": max(int(num_candidates), 1),
        "limit": max(int(limit), 1),
    }
    if filter_expression:
        stage["filter"] = filter_expression
    return [
        {"$vectorSearch": stage},
        {
            "$project": {
                "_id": 0,
                "releaseId": 1,
                "conceptId": 1,
                "term": 1,
                "displayTerm": 1,
                "preferredTerm": 1,
                "fsn": 1,
                "semanticTag": 1,
                "languageCode": 1,
                "ancestorIds": 1,
                "preferred": 1,
                "isPreferred": 1,
                "termRank": 1,
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ]


def vector_filter_expression(
    *,
    release_id: str,
    language_code: str,
    concept_ids: list[str] | None = None,
    max_scope_ids: int = 2_000,
) -> dict[str, Any]:
    """Build the ``$vectorSearch`` pre-filter for release, language, and scope."""
    clauses: list[dict[str, Any]] = [
        {"releaseId": {"$eq": release_id}},
        {"languageCode": {"$eq": language_code}},
        {"conceptActive": {"$eq": True}},
    ]
    if concept_ids:
        clauses.append({"conceptId": {"$in": list(concept_ids)[:max_scope_ids]}})
    return {"$and": clauses} if len(clauses) > 1 else clauses[0]


def _rerank_text(row: dict[str, Any]) -> str:
    parts = [str(row.get("term") or "")]
    if row.get("semanticTag"):
        parts.append(f"({row['semanticTag']})")
    if row.get("fsn") and row.get("fsn") != row.get("term"):
        parts.append(f"— {row['fsn']}")
    return " ".join(part for part in parts if part)


async def apply_optional_rerank(
    *,
    rerank: Any,
    query: str,
    rows: list[dict[str, Any]],
    rerank_pool: int,
    limit: int,
) -> dict[str, Any]:
    """Rerank the fused pool when a reranker adapter is configured.

    Returns the (possibly reordered) rows plus whether reranking actually ran, so
    callers can report the degraded path honestly rather than implying a rerank
    that never happened.
    """
    pool = rows[: max(int(rerank_pool or 1), 1)]
    if rerank is None or not pool:
        return {"results": rows[: max(int(limit or 1), 1)], "reranked": False, "rerankPoolSize": len(pool)}

    try:
        ranking = await rerank.rerank(query, [_rerank_text(row) for row in pool], top_k=max(int(limit or 1), 1))
    except Exception:  # noqa: BLE001 - rerank is best-effort
        return {"results": rows[: max(int(limit or 1), 1)], "reranked": False, "rerankPoolSize": len(pool)}

    if not ranking:
        return {"results": rows[: max(int(limit or 1), 1)], "reranked": False, "rerankPoolSize": len(pool)}

    ordered: list[dict[str, Any]] = []
    for item in ranking:
        index = int(item.get("index", -1))
        if 0 <= index < len(pool):
            ordered.append({**pool[index], "rerankScore": item.get("relevanceScore")})

    if len(ordered) < limit:
        used = {row.get("conceptId") for row in ordered}
        for row in rows:
            if len(ordered) >= limit:
                break
            if row.get("conceptId") not in used:
                ordered.append({**row, "rerankScore": None})
    return {"results": ordered[: max(int(limit or 1), 1)], "reranked": True, "rerankPoolSize": len(pool)}


def concept_id_query(query: str) -> bool:
    """True when the whole query is a SNOMED CT identifier."""
    return bool(re.fullmatch(r"\d{4,18}", str(query or "").strip()))
