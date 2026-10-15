"""KDL-007: expand SNOMED implicit ValueSets in FHIR search criteria.

A FHIR search such as::

    Condition?code:in=http://snomed.info/sct?fhir_vs=ecl/<<84114007

must not compile the ValueSet URL as a literal code. Before compilation the
compiler routes the parameter through the tenant terminology gateway's bounded
ValueSet expansion and hands the expanded code set to the existing, tested
``terminology_code_filter`` seam. There is no fallback to literal-code matching:
an unexpandable implicit ValueSet fails closed with a typed error.
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl, urlencode

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext

SNOMED_SYSTEM_URI = "http://snomed.info/sct"
# Mirrors MAX_TERMINOLOGY_CODE_FILTER_CODES: the expansion seam is bounded.
MAX_EXPANDED_CODES = 256
SUPPORTED_MODIFIERS = ("in", "below")

_IMPLICIT_VALUESET = re.compile(
    r"^https?://snomed\.info/sct\?fhir_vs=(?P<selector>.+)$", re.IGNORECASE
)
_ECL_SELECTOR = re.compile(r"^ecl/(?P<expression>.+)$", re.IGNORECASE)
_REFSET_SELECTOR = re.compile(r"^refset/(?P<refset>.+)$", re.IGNORECASE)


def _split_modifier(name: str) -> tuple[str, str] | None:
    base, separator, modifier = str(name or "").partition(":")
    if not separator or not base or not modifier:
        return None
    return base, modifier.lower()


def _expansion_request(parameter: str, modifier: str, value: Any) -> dict[str, Any] | None:
    """Describe the expansion for one ``:in`` / ``:below`` parameter value."""
    token = str(value or "").strip()
    if not token:
        return None
    if modifier == "below":
        system, separator, code = token.partition("|")
        if not separator:
            # A bare code cannot be resolved without its system; fail closed.
            raise KehrnelError(
                code="FHIR_VALUESET_EXPANSION_UNSUPPORTED",
                status=422,
                message=(
                    "code:below requires a system-qualified value (system|code) so the "
                    "hierarchy can be expanded."
                ),
                details={"parameter": parameter, "value": token},
            )
        if system.strip().rstrip("/").lower() != SNOMED_SYSTEM_URI:
            return None
        code = code.strip()
        if not code:
            raise KehrnelError(
                code="FHIR_VALUESET_EXPANSION_UNSUPPORTED",
                status=422,
                message="code:below requires a concept id to expand.",
                details={"parameter": parameter, "value": token},
            )
        return {
            "parameter": parameter,
            "system": SNOMED_SYSTEM_URI,
            "expression": f"<<{code}",
            "source": "code:below",
        }

    match = _IMPLICIT_VALUESET.match(token)
    if not match:
        return None
    selector = match.group("selector").strip()
    ecl = _ECL_SELECTOR.match(selector)
    if ecl:
        return {
            "parameter": parameter,
            "system": SNOMED_SYSTEM_URI,
            "expression": ecl.group("expression").strip(),
            "source": "fhir_vs=ecl",
        }
    refset = _REFSET_SELECTOR.match(selector)
    if refset:
        return {
            "parameter": parameter,
            "system": SNOMED_SYSTEM_URI,
            "expression": f"^ {refset.group('refset').strip()}",
            "source": "fhir_vs=refset",
        }
    raise KehrnelError(
        code="FHIR_VALUESET_EXPANSION_UNSUPPORTED",
        status=422,
        message=(
            "Only implicit SNOMED CT ValueSet URLs with an 'ecl/' or 'refset/' selector "
            "can be expanded by the search compiler."
        ),
        details={"parameter": parameter, "value": token},
    )


def _extract_codes(result: Any) -> list[str]:
    if not isinstance(result, dict):
        return []
    for key in ("concepts", "rows", "matches", "members", "items"):
        entries = result.get(key)
        if isinstance(entries, list):
            return _codes_from_entries(entries)
    expansion = result.get("expansion")
    if isinstance(expansion, dict) and isinstance(expansion.get("contains"), list):
        return _codes_from_entries(expansion["contains"])
    return []


def _codes_from_entries(entries: list[Any]) -> list[str]:
    codes: list[str] = []
    for entry in entries:
        if isinstance(entry, dict):
            code = entry.get("conceptId") or entry.get("code") or entry.get("concept_id")
        else:
            code = entry
        text = str(code or "").strip()
        if text and text not in codes:
            codes.append(text)
    return codes


async def expand_implicit_valuesets(
    ctx: StrategyContext,
    query_string: str | None,
    terminology_code_filter: dict[str, Any] | None,
) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
    """Rewrite SNOMED implicit ValueSet parameters into an expanded code filter.

    Returns ``(query_string, terminology_code_filter, expansion_evidence)``. When
    nothing matches, the inputs are returned unchanged and evidence is ``None``.
    """
    if terminology_code_filter is not None or not query_string:
        return query_string, terminology_code_filter, None

    pairs = parse_qsl(query_string, keep_blank_values=True)
    matches: list[tuple[int, dict[str, Any]]] = []
    for index, (name, value) in enumerate(pairs):
        split = _split_modifier(name)
        if split is None:
            continue
        parameter, modifier = split
        if modifier not in SUPPORTED_MODIFIERS:
            continue
        request = _expansion_request(parameter, modifier, value)
        if request is not None:
            matches.append((index, request))

    if not matches:
        return query_string, terminology_code_filter, None
    if len(matches) > 1:
        raise KehrnelError(
            code="FHIR_VALUESET_EXPANSION_AMBIGUOUS",
            status=422,
            message=(
                "Only one implicit SNOMED CT ValueSet parameter can be expanded per "
                "search; combine the criteria into a single expression."
            ),
            details={"parameters": [request["parameter"] for _index, request in matches]},
        )

    index, request = matches[0]
    adapter = (ctx.adapters or {}).get("terminology")
    if adapter is None:
        raise KehrnelError(
            code="FHIR_TERMINOLOGY_UNAVAILABLE",
            status=503,
            message=(
                "Expanding an implicit SNOMED CT ValueSet requires the environment's "
                "terminology gateway."
            ),
            details={"parameter": request["parameter"]},
        )

    payload: dict[str, Any] = {
        "system": request["system"],
        "offset": 0,
        # One extra member detects silent truncation at the accepted bound.
        "count": MAX_EXPANDED_CODES + 1,
    }
    if request.get("expression"):
        payload["expression"] = request["expression"]
    if request.get("url"):
        payload["url"] = request["url"]

    try:
        result = await adapter.execute("expand", payload)
    except KehrnelError as exc:
        raise KehrnelError(
            code=exc.code,
            status=exc.status,
            message=f"ValueSet expansion failed: {exc.message}",
            details={**(exc.details or {}), "parameter": request["parameter"]},
        ) from exc

    codes = _extract_codes(result)
    if len(codes) > MAX_EXPANDED_CODES:
        raise KehrnelError(
            code="FHIR_VALUESET_EXPANSION_TOO_LARGE",
            status=422,
            message=(
                f"The expanded ValueSet exceeds the {MAX_EXPANDED_CODES}-code bound for a "
                "compiled search filter."
            ),
            details={"parameter": request["parameter"], "maximumCodes": MAX_EXPANDED_CODES},
        )
    if not codes:
        raise KehrnelError(
            code="FHIR_VALUESET_EXPANSION_EMPTY",
            status=422,
            message="The implicit SNOMED CT ValueSet expanded to no codes.",
            details={"parameter": request["parameter"], "expression": request.get("expression")},
        )

    evidence = {
        "source": request["source"],
        "parameter": request["parameter"],
        "system": request["system"],
        "expression": request.get("expression"),
        "url": request.get("url"),
        "releaseId": result.get("releaseId") or result.get("release_id"),
        "version": result.get("version"),
        "code_count": len(codes),
        "complete": bool(result.get("complete", True)),
    }

    remaining = [pair for position, pair in enumerate(pairs) if position != index]
    rewritten = urlencode(remaining, doseq=False)
    return (
        rewritten,
        {
            "parameter": request["parameter"],
            "system": request["system"],
            # Deterministic order, matching the normalized filter seam.
            "codes": sorted(codes),
        },
        evidence,
    )
