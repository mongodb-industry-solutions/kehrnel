from __future__ import annotations

import pytest

from kehrnel.contracts import ConceptMention, TerminologyCandidateAuthority
from kehrnel.engine.semantics import GatewayTerminologyCandidateProvider


class Gateway:
    def __init__(self):
        self.calls = []

    async def execute(self, operation, payload):
        self.calls.append((operation, payload))
        if operation == "search":
            return {
                "matches": [
                    {"conceptId": "84114007", "preferredTerm": "Heart failure", "score": 100, "releaseId": "20260601"},
                    {"conceptId": "195111005", "preferredTerm": "Acute left-sided heart failure", "score": 50, "releaseId": "20260601"},
                ],
                "evidence": {"operation": "search"},
            }
        if operation == "lookup":
            return {
                "concept": {"conceptId": payload["code"], "preferredTerm": "Heart failure"},
                "version": "http://snomed.info/sct/version/20260601",
                "provider": {"id": "snomed-production"},
                "evidence": {"operation": "lookup", "code": payload["code"]},
            }
        if operation == "expand":
            return {
                "ok": True,
                "system": "http://snomed.info/sct",
                "version": "http://snomed.info/sct/version/20260601",
                "releaseId": "20260601",
                "provider": {"id": "snomed-production"},
                "concepts": [
                    {"conceptId": "84114007"},
                    {"conceptId": "195111005"},
                ],
                "matches": [
                    {"conceptId": "84114007"},
                    {"conceptId": "195111005"},
                ],
                "page": {
                    "offset": 0,
                    "limit": 513,
                    "returned": 2,
                    "hasMore": False,
                    "nextOffset": None,
                },
            }
        raise AssertionError(operation)


@pytest.mark.asyncio
async def test_gateway_search_candidates_are_verified_before_becoming_authoritative():
    gateway = Gateway()
    provider = GatewayTerminologyCandidateProvider(gateway)
    candidates = await provider.candidates(
        ConceptMention(
            mention_id="m1",
            text="heart failure",
            semantic_category="clinical_condition",
            requested_relationship="descendant_or_self",
            terminology_system_hint="http://snomed.info/sct",
        ),
        provider_ids=["snomed-production"],
        context={"language": "en"},
    )

    assert [call[0] for call in gateway.calls] == ["search", "lookup", "lookup"]
    assert candidates[0].code == "84114007"
    assert candidates[0].score_basis_points == 10_000
    assert candidates[0].verified is True
    assert candidates[0].authority is TerminologyCandidateAuthority.PROVIDER_VERIFIED


@pytest.mark.asyncio
async def test_explicit_code_is_looked_up_instead_of_trusted_from_the_caller():
    gateway = Gateway()
    provider = GatewayTerminologyCandidateProvider(gateway)
    candidates = await provider.candidates(
        ConceptMention(
            mention_id="m1",
            semantic_category="clinical_condition",
            explicit_coding={
                "system": "http://snomed.info/sct",
                "code": "84114007",
                "display": "Heart failure",
            },
        ),
        provider_ids=["snomed-production"],
        context={},
    )

    assert [call[0] for call in gateway.calls] == ["lookup"]
    assert candidates[0].authority is TerminologyCandidateAuthority.EXPLICIT_CODE
    assert candidates[0].verified is True


@pytest.mark.asyncio
async def test_descendant_relationship_becomes_a_complete_versioned_ecl_expansion():
    gateway = Gateway()
    provider = GatewayTerminologyCandidateProvider(gateway)
    mention = ConceptMention(
        mention_id="m1",
        text="heart failure",
        semantic_category="clinical_condition",
        requested_relationship="descendant_or_self",
        terminology_system_hint="http://snomed.info/sct",
    )
    selected = (
        await provider.candidates(
            mention,
            provider_ids=["snomed-production"],
            context={"language": "en"},
        )
    )[0]
    expansion = await provider.expand(
        mention,
        selected,
        provider_ids=["snomed-production"],
        context={"language": "en"},
    )

    operation, payload = gateway.calls[-1]
    assert operation == "expand"
    assert payload["expression"] == "<< 84114007"
    assert payload["release_id"] == "20260601"
    assert expansion.expression == "<< 84114007"
    assert expansion.codes == ["195111005", "84114007"]
    assert expansion.expansion_digest.startswith("sha256:")
