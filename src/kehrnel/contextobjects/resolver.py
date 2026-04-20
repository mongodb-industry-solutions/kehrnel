from __future__ import annotations

from typing import Any, Dict, List

from .models import normalize_context_definition, safe_list, token_set


def _request_signals(draft: Dict[str, Any] | None) -> Dict[str, Any]:
    draft = dict(draft or {})
    request_ir = draft.get("request_ir") or draft.get("requestIr") or {}
    requested_points = []
    for item in (
        safe_list(request_ir.get("requested_points"))
        + safe_list(request_ir.get("requestedPoints"))
        + safe_list(request_ir.get("focus"))
        + safe_list(draft.get("focus"))
        + safe_list(draft.get("evidence"))
        + safe_list(draft.get("groupBy"))
    ):
        text = f"{item or ''}".strip()
        if text and text not in requested_points:
            requested_points.append(text)
    utterance = draft.get("utterance") or draft.get("query") or draft.get("question") or ""
    scope = request_ir.get("scope") or draft.get("scope") or draft.get("subject")
    assertion_type = request_ir.get("assertion_type") or request_ir.get("assertionType") or draft.get("assertion_type")
    request_tokens = token_set(
        utterance,
        request_ir.get("task"),
        request_ir.get("aggregate"),
        request_ir.get("temporal"),
        request_ir.get("compare"),
        requested_points,
        scope,
        assertion_type,
    )
    return {
        "utterance": utterance,
        "scope": f"{scope or ''}".strip().lower() or None,
        "assertion_type": f"{assertion_type or ''}".strip().lower() or None,
        "requested_points": requested_points,
        "tokens": request_tokens,
    }


def _match_requested_points(requested_points: List[str], definition: Dict[str, Any]) -> Dict[str, Any]:
    matched: List[str] = []
    block_vocab = {}
    for block in definition.get("blocks", []):
        vocab = set(token_set(block.get("id"), block.get("title"), block.get("aliases")))
        block_vocab[block.get("id")] = vocab
    terminology_vocab = set()
    for binding in definition.get("terminology", []):
        terminology_vocab.update(binding.get("tokens", []))

    for point in requested_points:
        point_tokens = set(token_set(point))
        if not point_tokens:
            continue
        found = False
        for vocab in block_vocab.values():
            if point_tokens.issubset(vocab) or point_tokens.intersection(vocab):
                found = True
                break
        if not found and point_tokens.intersection(terminology_vocab):
            found = True
        if found:
            matched.append(point)

    missing = [point for point in requested_points if point not in matched]
    return {"matched": matched, "missing": missing}


def _score_definition(signals: Dict[str, Any], definition: Dict[str, Any]) -> Dict[str, Any]:
    point_match = _match_requested_points(signals["requested_points"], definition)
    signal_tokens = set(signals["tokens"])
    definition_tokens = set(definition.get("tokens", []))
    for block in definition.get("blocks", []):
        definition_tokens.update(block.get("tokens", []))
    for binding in definition.get("terminology", []):
        definition_tokens.update(binding.get("tokens", []))

    lexical_overlap = len(signal_tokens.intersection(definition_tokens))
    scope_match = bool(signals["scope"] and signals["scope"] in set(definition.get("subject_kinds", [])))
    assertion_match = bool(
        signals["assertion_type"] and signals["assertion_type"] in set(definition.get("assertion_types", []))
    )

    score = 0.0
    score += len(point_match["matched"]) * 1.7
    score += lexical_overlap * 0.18
    if scope_match:
        score += 0.9
    if assertion_match:
        score += 0.9
    if definition.get("relations"):
        score += 0.2

    max_score = max(len(signals["requested_points"]) * 1.7 + 2.5, 1.0)
    confidence = min(score / max_score, 1.0)
    return {
        "definition": definition,
        "score": round(score, 4),
        "confidence": round(confidence, 4),
        "matched_requested_points": point_match["matched"],
        "missing_requested_points": point_match["missing"],
        "scope_match": scope_match,
        "assertion_match": assertion_match,
        "lexical_overlap": lexical_overlap,
    }


def resolve_context_contract(
    draft: Dict[str, Any] | None,
    definitions: List[Dict[str, Any]] | None,
    options: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    signals = _request_signals(draft)
    catalog = [normalize_context_definition(item) for item in safe_list(definitions)]
    if not catalog:
        return {
            "ready": False,
            "needsClarification": True,
            "reason": "No ContextObject definitions are available for contract resolution.",
            "requestedPoints": signals["requested_points"],
            "candidates": [],
        }

    candidates = [_score_definition(signals, definition) for definition in catalog]
    candidates.sort(key=lambda item: (item["score"], item["confidence"]), reverse=True)
    best = candidates[0]
    second = candidates[1] if len(candidates) > 1 else None

    threshold = float(
        best["definition"].get("resolution", {}).get("clarification_threshold")
        or (options or {}).get("clarification_threshold")
        or 0.65
    )
    ambiguous = bool(second and abs(best["score"] - second["score"]) < 0.35)
    needs_clarification = bool(best["confidence"] < threshold or ambiguous)
    chosen = best["definition"]

    return {
        "ready": not needs_clarification,
        "needsClarification": needs_clarification,
        "confidence": best["confidence"],
        "threshold": threshold,
        "contextContract": chosen["id"],
        "definition": {
            "id": chosen["id"],
            "title": chosen["title"],
            "summary": chosen["summary"],
            "subjectKinds": chosen["subject_kinds"],
            "assertionTypes": chosen["assertion_types"],
            "outputFamilies": chosen["output_families"],
        },
        "requestedPoints": signals["requested_points"],
        "matchedRequestedPoints": best["matched_requested_points"],
        "missingRequestedPoints": best["missing_requested_points"],
        "scope": signals["scope"],
        "assertionType": signals["assertion_type"],
        "clarificationPrompts": (
            [
                f"Confirm whether you want '{best['definition']['title']}' or '{second['definition']['title']}'."
                for second in ([second] if ambiguous and second else [])
            ]
            + (
                [f"Specify which missing data points matter most: {', '.join(best['missing_requested_points'])}."]
                if best["missing_requested_points"] and needs_clarification
                else []
            )
        ),
        "candidates": [
            {
                "id": item["definition"]["id"],
                "title": item["definition"]["title"],
                "score": item["score"],
                "confidence": item["confidence"],
                "matchedRequestedPoints": item["matched_requested_points"],
                "missingRequestedPoints": item["missing_requested_points"],
                "scopeMatch": item["scope_match"],
                "assertionMatch": item["assertion_match"],
            }
            for item in candidates[:5]
        ],
    }
