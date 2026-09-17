"""Integration tests for MQL-shipped resources (batch generation and dependency chains).

Per-resource smoke, enriched fields, references, and coding checks live in
``test_mql_shipped_resources.py`` (default pytest, no integration marker).
Module-specific behavior tests live in ``test_clinical.py``, ``test_workflow.py``,
``test_financial.py``, and ``test_specialized.py``.
"""

from __future__ import annotations

import pytest

from fhir_gen import ResourceGenerator
from fhir_gen.resolvers.dependency import CORE_DEPENDENCIES, MQL_SHIPPED_RESOURCES, resolve_order

pytestmark = pytest.mark.integration

SEED_INTEGRATION = 99


@pytest.fixture
def gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED_INTEGRATION)


class TestMqlBatchGeneration:
    def test_generate_many_all_mql_resources(self, gen: ResourceGenerator) -> None:
        results = gen.generate_many(list(MQL_SHIPPED_RESOURCES), counts={r: 1 for r in MQL_SHIPPED_RESOURCES})
        assert set(results.keys()) == set(MQL_SHIPPED_RESOURCES)
        for rtype, docs in results.items():
            assert len(docs) == 1, rtype
            assert docs[0]["resourceType"] == rtype

    def test_resolve_order_covers_full_mql_set(self) -> None:
        order = resolve_order(list(MQL_SHIPPED_RESOURCES))
        assert set(order) >= set(MQL_SHIPPED_RESOURCES)
        for rtype, deps in CORE_DEPENDENCIES.items():
            if rtype not in MQL_SHIPPED_RESOURCES:
                continue
            for dep in deps:
                assert order.index(dep) < order.index(rtype), f"{dep} must precede {rtype}"


class TestMqlDependencyChains:
    def test_measure_report_links_measure(self, gen: ResourceGenerator) -> None:
        report = gen.generate("MeasureReport", count=1)[0]
        assert report["measure"].startswith("http://example.org/Measure/")
        measures = [
            resource
            for resource in gen.store.all_resources()
            if resource.get("resourceType") == "Measure"
        ]
        assert any(measure.get("url") == report["measure"] for measure in measures)

    def test_explanation_of_benefit_claim_chain(self, gen: ResourceGenerator) -> None:
        eob = gen.generate("ExplanationOfBenefit", count=1)[0]
        assert "patient" in eob
        assert eob["patient"]["reference"].startswith("Patient/")
        assert "type" in eob and "coding" in eob["type"]

    def test_coverage_eligibility_response_links_request(self, gen: ResourceGenerator) -> None:
        resp = gen.generate("CoverageEligibilityResponse", count=1)[0]
        assert resp["request"]["reference"].startswith("CoverageEligibilityRequest/")
        assert gen.store.reference_is_valid(resp["request"]["reference"])

    def test_questionnaire_response_links_questionnaire(self, gen: ResourceGenerator) -> None:
        qr = gen.generate("QuestionnaireResponse", count=1)[0]
        assert qr["questionnaire"].startswith("Questionnaire/")
        assert gen.store.reference_is_valid(qr["questionnaire"])

    def test_payment_reconciliation_links_payment_notice(self, gen: ResourceGenerator) -> None:
        recon = gen.generate("PaymentReconciliation", count=1)[0]
        assert recon["request"]["reference"].startswith("PaymentNotice/")
        assert gen.store.reference_is_valid(recon["request"]["reference"])

    def test_device_request_links_device_concept(self, gen: ResourceGenerator) -> None:
        req = gen.generate("DeviceRequest", count=1)[0]
        assert "code" in req
        concept = req["code"].get("concept", {})
        assert concept.get("coding")
        assert concept["coding"][0].get("system") == "http://snomed.info/sct"

    def test_composition_uses_loinc_type(self, gen: ResourceGenerator) -> None:
        comp = gen.generate("Composition", count=1)[0]
        coding = comp["type"]["coding"][0]
        assert coding["system"] == "http://loinc.org"
        assert coding.get("code")

    def test_organization_affiliation_dual_orgs(self, gen: ResourceGenerator) -> None:
        aff = gen.generate("OrganizationAffiliation", count=1)[0]
        assert aff["organization"]["reference"].startswith("Organization/")
        assert aff["participatingOrganization"]["reference"].startswith("Organization/")

    def test_mql_financial_rcm_bundle(self, gen: ResourceGenerator) -> None:
        types = [
            "CoverageEligibilityRequest",
            "CoverageEligibilityResponse",
            "ExplanationOfBenefit",
            "EnrollmentRequest",
            "EnrollmentResponse",
            "PaymentNotice",
            "PaymentReconciliation",
        ]
        results = gen.generate_many(types, counts={t: 1 for t in types})
        eob = results["ExplanationOfBenefit"][0]
        assert eob["patient"]["reference"].startswith("Patient/")
        recon = results["PaymentReconciliation"][0]
        assert recon["request"]["reference"].startswith("PaymentNotice/")


class TestMqlStoreIntegrity:
    def test_store_counts_after_full_mql_batch(self, gen: ResourceGenerator) -> None:
        gen.generate_many(list(MQL_SHIPPED_RESOURCES), counts={r: 1 for r in MQL_SHIPPED_RESOURCES})
        for rtype in MQL_SHIPPED_RESOURCES:
            assert gen.store.count(rtype) >= 1, f"store missing {rtype}"

    def test_find_by_reference_roundtrip(self, gen: ResourceGenerator) -> None:
        patient = gen.generate("Patient", count=1)[0]
        ref = f"Patient/{patient['id']}"
        assert gen.store.reference_is_valid(ref)
        entry = gen.store._store["Patient"][0]
        assert entry["reference"] == ref
