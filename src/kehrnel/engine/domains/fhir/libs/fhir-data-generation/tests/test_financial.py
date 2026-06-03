"""Prompt 12 — financial resource enricher tests."""

from fhir_gen import ResourceGenerator
from fhir_gen.generators.resources.financial import ENRICHERS

SEED = 42


def make_gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


class TestFinancialEnrichers:
    def test_enrichers_registered(self):
        assert len(ENRICHERS) == 7
        assert "Coverage" in ENRICHERS
        assert "Claim" in ENRICHERS

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
