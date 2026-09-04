"""Prompt 13 — specialized resource enricher tests."""

from fhir_gen import ResourceGenerator
from fhir_gen.generators.resources import ENRICHERS as ALL_ENRICHERS
from fhir_gen.generators.resources.specialized import ENRICHERS

SEED = 42


def make_gen() -> ResourceGenerator:
    return ResourceGenerator(seed=SEED)


class TestSpecializedEnrichers:
    def test_enrichers_registered(self):
        assert len(ENRICHERS) == 24
        assert "ImagingStudy" in ENRICHERS
        assert "MeasureReport" in ENRICHERS
        assert "Endpoint" in ENRICHERS

    def test_combined_enrichers_count(self):
        assert len(ALL_ENRICHERS) >= 50

    def test_specimen_collection(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Practitioner")
        spec = gen.generate("Specimen")[0]
        assert "type" in spec
        assert "collection" in spec
        assert "collectedDateTime" in spec["collection"]

    def test_imaging_study(self):
        gen = make_gen()
        gen.generate("Patient")
        img = gen.generate("ImagingStudy")[0]
        assert "modality" in img
        assert img["series"]
        assert len(img["series"]) > 0

    def test_device(self):
        dev = make_gen().generate("Device")[0]
        assert dev["name"]
        assert len(dev["name"]) > 0

    def test_research_study(self):
        gen = make_gen()
        gen.generate("Organization")
        rs = gen.generate("ResearchStudy")[0]
        assert rs["title"]
        assert rs["status"]

    def test_research_subject(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        gen.generate("ResearchStudy")
        subj = gen.generate("ResearchSubject")[0]
        assert subj["study"]["reference"].startswith("ResearchStudy/")
        assert subj["subject"]["reference"].startswith("Patient/")

    def test_group_with_members(self):
        gen = make_gen()
        gen.generate("Patient", count=3)
        group = gen.generate("Group")[0]
        assert group["name"]
        if group.get("type") == "person" and group.get("member"):
            assert group["member"][0]["entity"]["reference"].startswith("Patient/")

    def test_related_person(self):
        gen = make_gen()
        gen.generate("Patient")
        rp = gen.generate("RelatedPerson")[0]
        assert rp["patient"]["reference"].startswith("Patient/")
        assert rp["relationship"]

    def test_questionnaire_response(self):
        gen = make_gen()
        qr = gen.generate("QuestionnaireResponse")[0]
        assert qr["item"]
        assert len(qr["item"]) >= 2
        assert qr["questionnaire"].startswith("Questionnaire/")

    def test_device_usage_and_dispense(self):
        gen = make_gen()
        usage = gen.generate("DeviceUsage")[0]
        assert usage["device"]["concept"]["coding"]
        disp = gen.generate("DeviceDispense")[0]
        assert disp["device"]["concept"]["coding"]

    def test_biologically_derived_product(self):
        gen = make_gen()
        bdp = gen.generate("BiologicallyDerivedProduct")[0]
        assert bdp["productCategory"]["code"] in ["organ", "tissue", "fluid", "cells"]
        assert bdp["productStatus"]["code"] in ["available", "unavailable"]

    def test_organization_affiliation(self):
        gen = make_gen()
        aff = gen.generate("OrganizationAffiliation")[0]
        assert aff["organization"]["reference"].startswith("Organization/")

    def test_endpoint(self):
        gen = make_gen()
        ep = gen.generate("Endpoint")[0]
        assert ep["connectionType"][0]["coding"]
        assert ep["address"].startswith("https://")

    def test_genomic_study(self):
        gen = make_gen()
        gs = gen.generate("GenomicStudy")[0]
        assert gs["status"]
        assert gs["subject"]["reference"].startswith("Patient/")

    def test_measure_and_report(self):
        gen = make_gen()
        report = gen.generate("MeasureReport")[0]
        assert report["measure"].startswith("http://example.org/Measure/")
        assert report["type"] in [
            "individual", "subject-list", "summary", "data-collection",
        ]
