"""
Unified FHIR REST search request parser.

Accepts type-level and compartment search text in the forms callers pass to APIs:

- ``Patient?gender=female``
- ``Patient/{id}/Observation?status=final``
- ``http://host/fhir/Patient/{id}/Observation?category=vital-signs``
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from fhir_search_to_mql.core.exceptions import ParsingError
from fhir_search_to_mql.parser.compartment_parser import CompartmentParser


def _param_token(
    name: str,
    value: str,
    *,
    modifier: str | None = None,
    prefix: str | None = None,
) -> str:
    key = name
    if modifier:
        key = f"{name}:{modifier}"
    if prefix:
        return f"{key}={prefix}{value}"
    return f"{key}={value}"


def criteria_dict_to_query_string(criteria: dict[str, Any]) -> str:
    """Turn structured criteria into a FHIR search query string."""
    parts: list[str] = []
    for name, raw in criteria.items():
        if raw is None:
            continue
        if isinstance(raw, list):
            for item in raw:
                parts.append(_param_token(str(name), str(item)))
        else:
            parts.append(_param_token(str(name), str(raw)))
    return "&".join(parts)


def criteria_tuples_to_query_string(criteria: list[Any]) -> str:
    """Turn parser-style (name, value, ...) tuples into a query string."""
    parts: list[str] = []
    for item in criteria:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        name = str(item[0])
        value = str(item[1])
        modifier = str(item[2]) if len(item) > 2 and item[2] else None
        prefix = str(item[3]) if len(item) > 3 and item[3] else None
        parts.append(_param_token(name, value, modifier=modifier, prefix=prefix))
    return "&".join(parts)


def _path_segments(path: str) -> list[str]:
    return [segment for segment in (path or "").strip().strip("/").split("/") if segment]


def _split_path_and_query(text: str) -> tuple[str, str]:
    """Split REST search text into path and query string."""
    if "?" in text and not text.lower().startswith("http"):
        path_part, query_string = text.split("?", 1)
        return path_part, query_string

    parsed = urlparse(text)
    if parsed.scheme and parsed.path:
        return parsed.path, parsed.query or ""

    if "?" in text:
        path_part, query_string = text.split("?", 1)
        return path_part, query_string
    return text, ""


def _normalize_for_compartment_parser(text: str, path_part: str, query_string: str) -> str:
    """Build a path+query string CompartmentParser accepts."""
    path = path_part if path_part.startswith("/") else f"/{path_part}"
    if query_string:
        return f"{path}?{query_string}"
    return path


def parse_fhir_search(value: str) -> dict[str, Any]:
    """
    Parse FHIR search text into compile-ready fragments.

    Returns:
        ``resource_type``, ``query_string``, and optional ``compartment``
        (``{"type": ..., "id": ...}``).
    """
    text = (value or "").strip()
    if not text:
        raise ParsingError("fhir_search must be a non-empty string")

    path_part, query_string = _split_path_and_query(text)
    compartment_parser = CompartmentParser()
    compartment_url = _normalize_for_compartment_parser(text, path_part, query_string)

    if compartment_parser.is_compartment_url(compartment_url):
        parsed = compartment_parser.parse(compartment_url)
        result: dict[str, Any] = {
            "resource_type": parsed["resource_type"],
            "query_string": parsed.get("query_string") or "",
            "compartment": {
                "type": parsed["compartment_type"],
                "id": parsed["compartment_id"],
            },
        }
        return result

    segments = _path_segments(path_part)
    if not segments:
        raise ParsingError(
            f"Could not determine resource type from fhir_search: {value}"
        )

    resource_type = segments[-1]
    if not resource_type or not resource_type[0].isupper():
        raise ParsingError(
            f"Could not determine resource type from fhir_search: {value}"
        )

    return {
        "resource_type": resource_type,
        "query_string": query_string,
    }


def parse_fhir_search_parts(value: str) -> tuple[str, str]:
    """Parse ``Patient?family=Smith`` into ``(resource_type, query_string)``."""
    parsed = parse_fhir_search(value)
    return parsed["resource_type"], parsed.get("query_string") or ""
