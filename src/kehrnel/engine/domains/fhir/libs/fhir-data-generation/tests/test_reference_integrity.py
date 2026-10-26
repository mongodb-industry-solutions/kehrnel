"""Reference integrity and generate-many persistence tests."""

import pytest

mongomock = pytest.importorskip("mongomock")

from fhir_gen import ResourceGenerator
from fhir_gen.persistence.mongo import FHIRMongoStore
from fhir_gen.resolvers.dependency import resolve_order
from .reference_validation import assert_all_references_resolve


class TestReferenceIntegrity:
    def test_organization_before_patient_in_order(self):
        order = resolve_order(["Patient", "Encounter", "Observation"])
        assert order.index("Organization") < order.index("Patient")

    def test_patient_managing_organization_exists(self):
        gen = ResourceGenerator(seed=42)
        gen.generate_many(
            ["Organization", "Patient"],
            counts={"Organization": 2, "Patient": 5},
        )
        org_ids = {e["id"] for e in gen.store._store["Organization"]}
        for patient in gen.store._store["Patient"]:
            p = patient["resource"]
            mo = p.get("managingOrganization", {})
            ref = mo.get("reference", "")
            if ref.startswith("Organization/"):
                assert ref.split("/", 1)[1] in org_ids

    def test_no_orphan_references_in_clinical_bundle(self):
        gen = ResourceGenerator(seed=99)
        gen.generate_many(
            [
                "Organization", "Patient", "Practitioner", "Location",
                "Encounter", "Condition", "Observation", "MedicationRequest",
            ],
            counts={
                "Organization": 2,
                "Patient": 5,
                "Practitioner": 2,
                "Location": 2,
                "Encounter": 8,
                "Condition": 10,
                "Observation": 15,
                "MedicationRequest": 6,
            },
        )
        for resource in gen.store.all_resources():
            assert_all_references_resolve(resource, gen.store)

    def test_generate_many_saves_dependencies_to_mongo(self, monkeypatch):
        monkeypatch.setattr(
            "fhir_gen.persistence.mongo.MongoClient",
            mongomock.MongoClient,
        )
        gen = ResourceGenerator(seed=7)
        gen.generate_many(
            ["Patient", "Encounter", "Condition"],
            counts={"Patient": 3, "Encounter": 4, "Condition": 5},
        )
        store = FHIRMongoStore(uri="mongodb://localhost", db_name="fhir_ref_test")
        try:
            store.save_many(gen.store.all_resources())
            assert store.count("Patient") >= 3
            assert store.count("Encounter") >= 4
            assert store.count("Condition") >= 5
            assert store.count("Organization") >= 1
        finally:
            store.delete_all()
            store.close()

    def test_lazy_indexes_do_not_create_empty_collections(self, monkeypatch):
        monkeypatch.setattr(
            "fhir_gen.persistence.mongo.MongoClient",
            mongomock.MongoClient,
        )
        store = FHIRMongoStore(uri="mongodb://localhost", db_name="fhir_lazy_idx")
        try:
            names = store._db.list_collection_names()
            assert "Condition" not in names
            assert "MedicationRequest" not in names
        finally:
            store.close()
