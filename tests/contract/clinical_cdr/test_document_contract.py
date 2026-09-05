from __future__ import annotations

import json

from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.document_contract import (
    ProjectionVersions,
    build_projection_versions,
    stamp_projection_metadata,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.serialization import canonical_resource


class _Loader:
    def __init__(self, configs):
        self.configs = configs

    def list_resources(self):
        return list(self.configs)

    def get_config(self, resource_type):
        return self.configs[resource_type]


def _compartments(tmp_path, marker="one"):
    path = tmp_path / "compartments"
    path.mkdir(exist_ok=True)
    (path / "patient.json").write_text(json.dumps({"marker": marker}), encoding="utf-8")
    return str(path)


def test_projection_versions_are_stable_and_resource_scoped(tmp_path):
    original = _Loader({
        "Patient": {"resource": "Patient", "indexes": [{"fields": {"_search.name": 1}}]},
        "Observation": {"resource": "Observation", "indexes": [{"fields": {"_search.code": 1}}]},
    })
    changed = _Loader({
        "Patient": {"resource": "Patient", "indexes": [{"fields": {"_search.name": 1}}]},
        "Observation": {"resource": "Observation", "indexes": [{"fields": {"_search.status": 1}}]},
    })
    compartment_dir = _compartments(tmp_path)

    before = build_projection_versions(
        original,
        fhir_release="R5",
        compartment_definitions_dir=compartment_dir,
    )
    repeated = build_projection_versions(
        original,
        fhir_release="R5",
        compartment_definitions_dir=compartment_dir,
    )
    after = build_projection_versions(
        changed,
        fhir_release="R5",
        compartment_definitions_dir=compartment_dir,
    )

    assert before == repeated
    assert before.projection_contract_version != after.projection_contract_version
    assert before.for_resource("Patient") == after.for_resource("Patient")
    assert before.for_resource("Observation") != after.for_resource("Observation")


def test_compartment_change_invalidates_every_resource_projection(tmp_path):
    loader = _Loader({"Patient": {"resource": "Patient"}, "Observation": {"resource": "Observation"}})
    path = _compartments(tmp_path, "one")
    before = build_projection_versions(loader, fhir_release="R5", compartment_definitions_dir=path)
    _compartments(tmp_path, "two")
    after = build_projection_versions(loader, fhir_release="R5", compartment_definitions_dir=path)

    assert before.for_resource("Patient") != after.for_resource("Patient")
    assert before.for_resource("Observation") != after.for_resource("Observation")


def test_operational_metadata_is_grouped_and_not_serialized():
    versions = ProjectionVersions(
        fhir_release="R5",
        projection_contract_version="v1:global",
        resource_projection_versions={"Patient": "v1:patient"},
    )
    stored = stamp_projection_metadata(
        {"resourceType": "Patient", "id": "p1", "_search": {"name": "smith"}},
        versions,
    )

    assert stored["_compartments"] == {}
    assert stored["_kehrnel"]["resource_projection_version"] == "v1:patient"
    assert canonical_resource(stored) == {"resourceType": "Patient", "id": "p1"}


def test_operational_provenance_is_stored_but_not_serialized():
    versions = ProjectionVersions(
        fhir_release="R5",
        projection_contract_version="v1:global",
        resource_projection_versions={"Patient": "v1:patient"},
    )
    stored = stamp_projection_metadata(
        {"resourceType": "Patient", "id": "p1"},
        versions,
        provenance={"source": "synthetic", "job_id": "job-1", "recipe": "clinical_dev"},
    )

    assert stored["_kehrnel"]["provenance"] == {
        "source": "synthetic",
        "job_id": "job-1",
        "recipe": "clinical_dev",
    }
    assert "_kehrnel" not in canonical_resource(stored)
