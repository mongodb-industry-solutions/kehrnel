from __future__ import annotations

import copy
import re
from typing import Any, Dict, Optional


DEFAULT_PAGE_LIMIT = 100
MAX_PAGE_LIMIT = 100


def _positive_int(value: Any, *, label: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return parsed


def _non_negative_int(value: Any, *, label: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return parsed


def _extract_token_options(options: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    options = options or {}
    nested = options.get("pagination") if isinstance(options.get("pagination"), dict) else {}
    search_after = (
        nested.get("searchAfter")
        or nested.get("pageToken")
        or options.get("searchAfter")
        or options.get("pageToken")
    )
    search_before = nested.get("searchBefore") or options.get("searchBefore")
    if search_after and search_before:
        raise ValueError("Use either searchAfter/pageToken or searchBefore, not both")
    return {
        "searchAfter": search_after,
        "searchBefore": search_before,
    }


def normalize_ast_pagination(
    ast: Dict[str, Any],
    *,
    default_limit: int = DEFAULT_PAGE_LIMIT,
    max_limit: int = MAX_PAGE_LIMIT,
    pagination_options: Optional[Dict[str, Any]] = None,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Return an effective AST where the page limit is part of the query contract."""
    effective_ast = copy.deepcopy(ast)
    warnings: list[Dict[str, Any]] = []

    limit_source = "explicit"
    raw_limit = effective_ast.get("limit")
    if raw_limit is None:
        effective_limit = _positive_int(default_limit, label="default LIMIT")
        effective_ast["limit"] = effective_limit
        limit_source = "default"
        warnings.append(
            {
                "code": "default_limit_applied",
                "message": f"AQL query did not specify LIMIT; using LIMIT {effective_limit}.",
                "details": {"limit": effective_limit},
            }
        )
    else:
        requested_limit = _positive_int(raw_limit, label="LIMIT")
        if requested_limit > max_limit:
            effective_limit = _positive_int(max_limit, label="maximum LIMIT")
            effective_ast["limit"] = effective_limit
            limit_source = "capped"
            warnings.append(
                {
                    "code": "limit_capped",
                    "message": f"AQL LIMIT {requested_limit} exceeds the maximum page size; using LIMIT {effective_limit}.",
                    "details": {
                        "requestedLimit": requested_limit,
                        "effectiveLimit": effective_limit,
                        "maxLimit": max_limit,
                    },
                }
            )
        else:
            effective_limit = requested_limit
            effective_ast["limit"] = effective_limit

    if effective_ast.get("offset") is not None:
        effective_ast["offset"] = _non_negative_int(effective_ast["offset"], label="OFFSET")

    token_options = _extract_token_options(pagination_options)
    metadata = {
        "pageSize": effective_limit,
        "maxPageSize": max_limit,
        "limitSource": limit_source,
        "warnings": warnings,
        "tokens": {key: value for key, value in token_options.items() if value},
    }
    effective_ast["__pagination"] = {
        "pageSize": effective_limit,
        "maxPageSize": max_limit,
        "limitSource": limit_source,
        **metadata["tokens"],
    }
    return effective_ast, metadata


def render_effective_aql(raw_aql: str, pagination: Dict[str, Any]) -> str:
    """Best-effort display string for the effective AQL, not an internal parser input."""
    if not isinstance(raw_aql, str):
        return ""
    page_size = pagination.get("pageSize")
    if not page_size:
        return raw_aql

    stripped = raw_aql.strip().rstrip(";")
    source = pagination.get("limitSource")
    if source == "default":
        return f"{stripped}\nLIMIT {page_size}"
    if source == "capped":
        return re.sub(r"(?is)\bLIMIT\s+\d+\b", f"LIMIT {page_size}", stripped, count=1)
    return stripped
