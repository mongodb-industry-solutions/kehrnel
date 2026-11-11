"""Terminology-gateway adapter for production cohort intent grounding."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Protocol, Sequence

from kehrnel.contracts.cohort_intent import (
    MAX_TERMINOLOGY_EXPANSION_CODES,
    ConceptMention,
    ConceptRelationship,
    TerminologyCandidate,
    TerminologyCandidateAuthority,
    TerminologyExpansion,
    TerminologyExpressionLanguage,
    create_terminology_expansion,
)


class TerminologyGateway(Protocol):
    async def execute(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]: ...


def _normalized(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in text if not unicodedata.combining(character))
    return " ".join(re.findall(r"[a-z0-9]+", text.lower()))


def _contains_rows(value_set: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def visit(items: Any) -> None:
        for item in items or []:
            if not isinstance(item, dict):
                continue
            if item.get("code") and item.get("system"):
                rows.append(item)
            visit(item.get("contains"))

    visit((value_set.get("expansion") or {}).get("contains"))
    return rows


def _search_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(response.get("matches"), list):
        return [item for item in response["matches"] if isinstance(item, dict)]
    value_set = response.get("valueSet")
    if isinstance(value_set, dict):
        return _contains_rows(value_set)
    return []


def _expansion_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    native_sets = [
        [item for item in response[name] if isinstance(item, dict)]
        for name in ("concepts", "matches")
        if isinstance(response.get(name), list)
    ]
    if len(native_sets) == 2:
        first = {_row_code(item) for item in native_sets[0]}
        second = {_row_code(item) for item in native_sets[1]}
        if first != second:
            raise ValueError("terminology provider concepts and matches disagree")
    if native_sets:
        return native_sets[0]
    value_set = response.get("valueSet")
    if isinstance(value_set, dict):
        return _contains_rows(value_set)
    raise ValueError("terminology expansion has no recognized member collection")


def _release_id(version: str) -> str | None:
    normalized = version.rstrip("/")
    if "/version/" in normalized:
        return normalized.rsplit("/version/", 1)[-1] or None
    return normalized if normalized and "://" not in normalized else None


def _require_complete_expansion(response: dict[str, Any], code_count: int) -> None:
    if response.get("truncated") is True:
        raise ValueError("terminology provider marked the expansion as truncated")
    page = response.get("page")
    if page is not None:
        if not isinstance(page, dict):
            raise ValueError("terminology expansion page metadata must be an object")
        if page.get("hasMore") is True or page.get("nextOffset") is not None:
            raise ValueError("terminology provider returned an incomplete expansion page")
        returned = page.get("returned")
        if returned is not None and returned != code_count:
            raise ValueError("terminology expansion page count does not match its members")
        total = page.get("total")
        if total is not None and total != code_count:
            raise ValueError("terminology expansion total does not match its members")
    value_set = response.get("valueSet")
    if isinstance(value_set, dict) and isinstance(value_set.get("expansion"), dict):
        expansion = value_set["expansion"]
        if expansion.get("offset", 0) != 0:
            raise ValueError("FHIR terminology expansion does not start at offset zero")
        total = expansion.get("total")
        if total is not None and total != code_count:
            raise ValueError("FHIR terminology provider returned an incomplete expansion")


def _row_code(row: dict[str, Any]) -> str:
    return str(row.get("conceptId") or row.get("code") or "").strip()


def _row_display(row: dict[str, Any]) -> str:
    return str(
        row.get("preferredTerm")
        or row.get("display")
        or row.get("term")
        or row.get("matchedTerm")
        or _row_code(row)
    ).strip()


def _score_basis_points(row: dict[str, Any], mention_text: str) -> int:
    raw = row.get("score")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return max(0, min(10_000, int(round(float(raw) * 100))))
    return 10_000 if _normalized(_row_display(row)) == _normalized(mention_text) else 8_500


class GatewayTerminologyCandidateProvider:
    """Generate candidates through Kehrnel's tenant-scoped terminology gateway.

    A free-text system mention uses the provider's search operation. A mention
    constrained by a ValueSet uses the FHIR-standard ``$expand`` filter path.
    Candidate recall alone is never authoritative: every returned coding is
    verified with lookup or ValueSet validation before ``verified=True``.
    """

    def __init__(self, gateway: TerminologyGateway, *, maximum_verifications: int = 10) -> None:
        if not 1 <= maximum_verifications <= 20:
            raise ValueError("maximum_verifications must be between one and twenty")
        self.gateway = gateway
        self.maximum_verifications = maximum_verifications

    @staticmethod
    def _base_payload(
        mention: ConceptMention,
        provider_ids: Sequence[str],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        system = (
            mention.explicit_coding.system
            if mention.explicit_coding
            else mention.terminology_system_hint
        )
        if not system:
            raise ValueError("terminology grounding requires a system hint or explicit coding")
        payload: dict[str, Any] = {
            "system": system,
            "language": context.get("language"),
            "correlation_id": context.get("correlation_id"),
        }
        if len(provider_ids) == 1:
            payload["provider_id"] = provider_ids[0]
        elif len(provider_ids) > 1:
            raise ValueError("a terminology mention must route to one explicit provider")
        version = mention.explicit_coding.version if mention.explicit_coding else context.get("terminology_version")
        if version:
            payload["version"] = version
        return {key: value for key, value in payload.items() if value is not None}

    async def candidates(
        self,
        mention: ConceptMention,
        *,
        provider_ids: Sequence[str],
        context: dict[str, Any],
    ) -> Sequence[TerminologyCandidate]:
        base = self._base_payload(mention, provider_ids, context)
        if mention.explicit_coding:
            return await self._explicit_candidate(mention, base)

        if mention.value_set_hint:
            recalled = await self.gateway.execute(
                "expand",
                {
                    **base,
                    "url": mention.value_set_hint,
                    "value_set_version": context.get("value_set_version"),
                    "filter": mention.text,
                    "count": self.maximum_verifications,
                    "active_only": True,
                },
            )
        else:
            recalled = await self.gateway.execute(
                "search",
                {**base, "q": mention.text, "limit": self.maximum_verifications},
            )

        candidates: list[TerminologyCandidate] = []
        for row in _search_rows(recalled)[: self.maximum_verifications]:
            code = _row_code(row)
            system = str(row.get("system") or base["system"]).strip()
            if not code:
                continue
            verification_payload = {**base, "system": system, "code": code}
            operation = "lookup"
            if mention.value_set_hint:
                operation = "validate-code"
                verification_payload["url"] = mention.value_set_hint
                if context.get("value_set_version"):
                    verification_payload["value_set_version"] = context["value_set_version"]
            verified_response = await self.gateway.execute(operation, verification_payload)
            valid = bool(verified_response.get("valid", True))
            concept = verified_response.get("concept")
            if operation == "lookup" and concept is None and not verified_response.get("parameters"):
                valid = False
            candidates.append(
                TerminologyCandidate(
                    system=system,
                    code=code,
                    display=_row_display(row),
                    provider_id=str((verified_response.get("provider") or {}).get("id") or base.get("provider_id") or "routed-provider"),
                    version=str(verified_response.get("version") or row.get("version") or base.get("version") or "") or None,
                    score_basis_points=_score_basis_points(row, mention.text or ""),
                    authority=TerminologyCandidateAuthority.PROVIDER_VERIFIED,
                    verified=valid,
                    match_methods=["valueset-filter" if mention.value_set_hint else "provider-search", operation],
                    evidence={
                        "recall": recalled.get("evidence") or {},
                        "verification": verified_response.get("evidence") or {},
                    },
                )
            )
        return candidates

    async def expand(
        self,
        mention: ConceptMention,
        selected: TerminologyCandidate,
        *,
        provider_ids: Sequence[str],
        context: dict[str, Any],
    ) -> TerminologyExpansion:
        """Produce the complete code set that downstream compilers may execute."""

        if not selected.verified:
            raise ValueError("only a provider-verified terminology candidate can be expanded")
        base = self._base_payload(mention, provider_ids, context)
        if base.get("provider_id") and base["provider_id"] != selected.provider_id:
            raise ValueError("selected terminology candidate does not match the routed provider")
        if selected.system.rstrip("/") != str(base["system"]).rstrip("/"):
            raise ValueError("selected terminology candidate does not match the routed code system")
        if not selected.version:
            raise ValueError("terminology expansion requires a provider-verified version")

        relationship = mention.requested_relationship
        if relationship is ConceptRelationship.EXACT:
            return create_terminology_expansion(
                provider_id=selected.provider_id,
                system=selected.system,
                version=selected.version,
                expression_language=TerminologyExpressionLanguage.CODE,
                expression=selected.code,
                codes=[selected.code],
            )

        payload: dict[str, Any] = {
            **base,
            "provider_id": selected.provider_id,
            "system": selected.system,
            "version": selected.version,
            "offset": 0,
            # Ask for one additional member; accepting a bounded partial set
            # would change the clinical meaning of the cohort.
            "count": MAX_TERMINOLOGY_EXPANSION_CODES + 1,
        }
        release_id = _release_id(selected.version)
        if release_id:
            payload["release_id"] = release_id

        if mention.value_set_hint:
            value_set_version = str(context.get("value_set_version") or "").strip()
            if not value_set_version:
                raise ValueError("a governed ValueSet expansion requires value_set_version")
            payload.update(
                {
                    "url": mention.value_set_hint,
                    "value_set_version": value_set_version,
                }
            )
            expression_language = TerminologyExpressionLanguage.VALUE_SET
            expression = f"value-set:{mention.value_set_hint}|{value_set_version}"
        elif selected.system.rstrip("/") == "http://snomed.info/sct":
            operator = (
                "<"
                if relationship is ConceptRelationship.DESCENDANT
                else "<<"
            )
            expression_language = TerminologyExpressionLanguage.ECL
            expression = f"{operator} {selected.code}"
            payload["expression"] = expression
            value_set_version = None
        else:
            raise ValueError(
                "hierarchical expansion for this terminology requires a pinned ValueSet"
            )

        response = await self.gateway.execute("expand", payload)
        if not isinstance(response, dict) or response.get("ok") is False:
            raise ValueError("terminology provider returned an invalid expansion")
        provider = response.get("provider") or {}
        if provider and provider.get("id") != selected.provider_id:
            raise ValueError("terminology expansion came from a different provider")
        result_system = str(response.get("system") or selected.system).rstrip("/")
        if result_system != selected.system.rstrip("/"):
            raise ValueError("terminology expansion came from a different code system")

        rows = _expansion_rows(response)
        codes = sorted({_row_code(row) for row in rows if _row_code(row)})
        if not codes:
            raise ValueError("terminology expansion contains no codes")
        if selected.code not in codes and relationship is ConceptRelationship.DESCENDANT_OR_SELF:
            raise ValueError("descendant-or-self expansion omitted the selected concept")
        if len(codes) > MAX_TERMINOLOGY_EXPANSION_CODES:
            raise ValueError("terminology expansion exceeds the governed code bound")
        _require_complete_expansion(response, len(codes))

        observed_versions = {
            str(row.get("version"))
            for row in rows
            if str(row.get("version") or "").strip()
        }
        response_version = str(response.get("version") or "").strip()
        if response_version:
            observed_versions.add(response_version)
        if observed_versions and observed_versions != {selected.version}:
            raise ValueError("terminology expansion version does not match the selected candidate")

        return create_terminology_expansion(
            provider_id=selected.provider_id,
            system=selected.system,
            version=selected.version,
            expression_language=expression_language,
            expression=expression,
            value_set_uri=mention.value_set_hint,
            value_set_version=value_set_version,
            codes=codes,
        )

    async def _explicit_candidate(
        self,
        mention: ConceptMention,
        base: dict[str, Any],
    ) -> Sequence[TerminologyCandidate]:
        coding = mention.explicit_coding
        assert coding is not None
        payload = {**base, "code": coding.code}
        operation = "lookup"
        if mention.value_set_hint:
            operation = "validate-code"
            payload["url"] = mention.value_set_hint
        response = await self.gateway.execute(operation, payload)
        concept = response.get("concept") if isinstance(response.get("concept"), dict) else {}
        valid = bool(response.get("valid", True))
        if operation == "lookup" and not concept and not response.get("parameters"):
            valid = False
        display = str(
            coding.display
            or response.get("display")
            or concept.get("preferredTerm")
            or concept.get("display")
            or coding.code
        )
        return [
            TerminologyCandidate(
                system=coding.system,
                code=coding.code,
                display=display,
                provider_id=str((response.get("provider") or {}).get("id") or base.get("provider_id") or "routed-provider"),
                version=str(response.get("version") or coding.version or "") or None,
                score_basis_points=10_000,
                authority=TerminologyCandidateAuthority.EXPLICIT_CODE,
                verified=valid,
                match_methods=[operation],
                evidence={"verification": response.get("evidence") or {}},
            )
        ]
