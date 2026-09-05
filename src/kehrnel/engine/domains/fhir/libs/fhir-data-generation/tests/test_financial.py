"""Prompt 12 — financial resource enricher tests."""

from fhir_gen import ResourceGenerator
from fhir_gen.generators.resources.financial import ENRICHERS

SEED = 42


def make_gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


class TestFinancialEnrichers:
    def test_enrichers_registered(self):
        assert len(ENRICHERS) == 15
        assert "Coverage" in ENRICHERS
        assert "ExplanationOfBenefit" in ENRICHERS
        assert "PaymentReconciliation" in ENRICHERS

    def test_coverage_structure(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        cov = gen.generate("Coverage")[0]
        assert "beneficiary" in cov and "insurer" in cov
        assert "period" in cov
        assert cov["beneficiary"]["reference"].startswith("Patient/")

    def test_claim_with_coverage(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        gen.generate("Coverage")
        gen.generate("Practitioner")
        claim = gen.generate("Claim")[0]
        assert "patient" in claim and "insurer" in claim
        assert "insurance" in claim
        assert claim["total"]["value"] > 0

    def test_claim_response(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        resp = gen.generate("ClaimResponse")[0]
        assert resp["outcome"]
        assert resp["total"]

    def test_account(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        gen.generate("Coverage")
        acct = gen.generate("Account")[0]
        assert acct["name"]
        assert acct["subject"]
        assert "coverage" in acct

    def test_invoice(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        inv = gen.generate("Invoice")[0]
        assert inv["lineItem"]
        assert "totalNet" in inv and "totalGross" in inv

    def test_charge_item(self):
        gen = make_gen()
        gen.generate("Patient")
        item = gen.generate("ChargeItem")[0]
        assert item["code"]
        assert item["unitPriceComponent"]

    def test_coverage_eligibility_request(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        gen.generate("Coverage")
        req = gen.generate("CoverageEligibilityRequest")[0]
        assert req["purpose"]
        assert req["patient"]["reference"].startswith("Patient/")

    def test_explanation_of_benefit(self):
        gen = make_gen()
        eob = gen.generate("ExplanationOfBenefit")[0]
        assert eob["type"]["coding"]
        assert eob["use"] in ["claim", "preauthorization", "predetermination"]

    def test_coverage_eligibility_response(self):
        gen = make_gen()
        resp = gen.generate("CoverageEligibilityResponse")[0]
        assert resp["request"]["reference"].startswith("CoverageEligibilityRequest/")

    def test_enrollment_request_response(self):
        gen = make_gen()
        req = gen.generate("EnrollmentRequest")[0]
        assert req["candidate"]["reference"].startswith("Patient/")
        resp = gen.generate("EnrollmentResponse")[0]
        assert resp["request"]["reference"].startswith("EnrollmentRequest/")

    def test_insurance_plan(self):
        gen = make_gen()
        plan = gen.generate("InsurancePlan")[0]
        assert plan["type"][0]["coding"]
        assert plan["ownedBy"]["reference"].startswith("Organization/")

    def test_charge_item_definition(self):
        gen = make_gen()
        cid = gen.generate("ChargeItemDefinition")[0]
        assert cid["url"]
        assert isinstance(cid["publisher"], str) and cid["publisher"]
        vac = cid.get("versionAlgorithmCoding", {})
        assert vac.get("code") and vac.get("system")
        assert "versionAlgorithmString" not in cid

    def test_payment_notice_and_reconciliation(self):
        gen = make_gen()
        notice = gen.generate("PaymentNotice")[0]
        assert notice["paymentStatus"]["coding"]
        recon = gen.generate("PaymentReconciliation")[0]
        assert isinstance(recon["outcome"], str) and recon["outcome"]
        assert recon["request"]["reference"].startswith("PaymentNotice/")
