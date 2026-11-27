"""Prompt 14 — MongoDB persistence tests (mongomock)."""

import pytest

mongomock = pytest.importorskip("mongomock")

from fhir_gen import ResourceGenerator
from fhir_gen.persistence.mongo import FHIRMongoStore


@pytest.fixture
def store(monkeypatch) -> FHIRMongoStore:
    monkeypatch.setattr(
        "fhir_gen.persistence.mongo.MongoClient",
        mongomock.MongoClient,
    )
    s = FHIRMongoStore(uri="mongodb://localhost", db_name="fhir_test")
    yield s
    s.delete_all()
    s.close()


@pytest.fixture
def sample_patient() -> dict:
    return {
        "resourceType": "Patient",
        "id": "pat-001",
        "meta": {"versionId": "1", "lastUpdated": "2024-01-01T00:00:00Z"},
        "name": [{"family": "Smith", "given": ["John"]}],
        "gender": "male",
        "birthDate": "1980-05-15",
        "identifier": [{"system": "http://hospital.example.org/mrn", "value": "MRN123"}],
    }


class TestFHIRMongoStore:
    def test_save_and_get(self, store: FHIRMongoStore, sample_patient: dict):
        store.save(sample_patient)
        loaded = store.get("Patient", "pat-001")
        assert loaded is not None
        assert loaded["resourceType"] == "Patient"
        assert loaded["id"] == "pat-001"
        assert "_stored_at" not in loaded

    def test_save_requires_type_and_id(self, store: FHIRMongoStore):
        with pytest.raises(ValueError):
            store.save({"resourceType": "Patient"})

    def test_find_by_reference(self, store: FHIRMongoStore, sample_patient: dict):
        store.save(sample_patient)
        found = store.find_by_reference("Patient/pat-001")
        assert found["name"][0]["family"] == "Smith"

    def test_save_many(self, store: FHIRMongoStore):
        resources = [
            {"resourceType": "Patient", "id": f"p{i}", "gender": "male"}
            for i in range(5)
        ]
        counts = store.save_many(resources)
        assert counts["Patient"] >= 5
        assert store.count("Patient") == 5

    def test_search_patient_by_family(self, store: FHIRMongoStore, sample_patient: dict):
        store.save(sample_patient)
        results = store.search_patient(family="smith")
        assert len(results) == 1

    def test_search_patient_by_gender(self, store: FHIRMongoStore, sample_patient: dict):
        store.save(sample_patient)
        results = store.search_patient(gender="male")
        assert len(results) == 1

    def test_observation_search_for_patient(self, store: FHIRMongoStore):
        store.save({"resourceType": "Patient", "id": "p1"})
        store.save({
            "resourceType": "Observation",
            "id": "o1",
            "status": "final",
            "subject": {"reference": "Patient/p1"},
            "code": {"coding": [{"code": "8867-4"}]},
        })
        obs = store.search_observations_for_patient("p1", code="8867-4")
        assert len(obs) == 1

    def test_delete_all_by_type(self, store: FHIRMongoStore, sample_patient: dict):
        store.save(sample_patient)
        store.delete_all("Patient")
        assert store.count("Patient") == 0

    def test_stats_and_list_types(self, store: FHIRMongoStore):
        store.save({"resourceType": "Patient", "id": "a"})
        store.save({"resourceType": "Organization", "id": "b", "name": "Acme"})
        types = store.list_resource_types()
        assert "Patient" in types
        assert "Organization" in types
        stats = store.stats()
        assert stats["Patient"] == 1
        assert stats["Organization"] == 1

    def test_collection_name_without_prefix(self, monkeypatch, sample_patient: dict):
        monkeypatch.setattr(
            "fhir_gen.persistence.mongo.MongoClient",
            mongomock.MongoClient,
        )
        s = FHIRMongoStore(
            uri="mongodb://localhost",
            db_name="fhir_test_noprefix",
            collection_prefix="",
        )
        try:
            s.save(sample_patient)
            assert "Patient" in s._db.list_collection_names()
            assert s.collection_name("Patient") == "Patient"
        finally:
            s.delete_all()
            s.close()

    def test_collection_name_with_prefix(self, monkeypatch, sample_patient: dict):
        monkeypatch.setattr(
            "fhir_gen.persistence.mongo.MongoClient",
            mongomock.MongoClient,
        )
        s = FHIRMongoStore(
            uri="mongodb://localhost",
            db_name="fhir_test_prefix",
            collection_prefix="fhir_",
        )
        try:
            s.save(sample_patient)
            assert "fhir_Patient" in s._db.list_collection_names()
            assert "Patient" not in s._db.list_collection_names()
            assert "Patient" in s.list_resource_types()
            assert s.collection_name("Observation") == "fhir_Observation"
        finally:
            s.delete_all()
            s.close()

    def test_integration_with_generator(self, store: FHIRMongoStore, monkeypatch):
        monkeypatch.setattr(
            "fhir_gen.persistence.mongo.MongoClient",
            mongomock.MongoClient,
        )
        gen = ResourceGenerator(seed=42)
        patients = gen.generate("Patient", count=3)
        store.save_many(patients)
        assert store.count("Patient") == 3
        loaded = store.get("Patient", patients[0]["id"])
        assert loaded is not None
