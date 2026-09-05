"""Contract tests for the FHIR migration accelerator vertical slice."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr import import_resources as importer
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.validation import validate_resource
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.storage_adapter import MongoFHIRStorageAdapter
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.document_contract import (
    ProjectionVersions,
    stamp_projection_metadata,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST


def _ctx(*, release: str = "R5") -> StrategyContext:
    config = {
        "database": "fhir_test",
        "schema_version": release,
        "collections": {"mode": "per_resource_type"},
        "search": {"auto_index": True},
    }
    return StrategyContext(
        environment_id="migration-test",
        config=config,
        bindings={"db": {"uri": "mongodb://unused", "database": "fhir_test"}},
        manifest=MANIFEST,
    )


def test_parse_bundle_and_ndjson_with_line_level_findings():
    resources, source, findings = importer.parse_import_payload({
        "bundle": {
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [{"resource": {"resourceType": "Patient", "id": "p1"}}],
        }
    })
    assert source == "bundle:collection"
    assert resources[0]["id"] == "p1"
    assert findings == []

    resources, source, findings = importer.parse_import_payload({
        "ndjson": '{"resourceType":"Patient","id":"p1"}\nnot-json\n'
    })
    assert source == "ndjson"
    assert len(resources) == 1
    assert findings[0]["line"] == 2


@pytest.mark.parametrize("release", ["R5", "R6"])
def test_bundled_base_validation_for_supported_releases(release):
    valid, findings = validate_resource(
        {"resourceType": "Patient", "id": "p1"},
        index=0,
        level="base",
        release=release,
        supported_resource_types={"Patient"},
    )
    assert valid and not [item for item in findings if item["severity"] == "error"]

    _, findings = validate_resource(
        {"resourceType": "Patient", "id": "contains space"},
        index=0,
        level="base",
        release=release,
        supported_resource_types={"Patient"},
    )
    assert any(item["code"] == "FHIR_ID_INVALID" for item in findings)

    _, findings = validate_resource(
        {"resourceType": "MadeUpClinicalThing", "id": "x1"},
        index=0,
        level="structure",
        release=release,
        supported_resource_types={"Patient"},
    )
    assert any(item["code"] == "FHIR_RESOURCE_TYPE_UNSUPPORTED" for item in findings)


class _FakeDenormalizer:
    class _ConfigLoader:
        @staticmethod
        def list_resources():
            return ["Patient"]

        @staticmethod
        def get_config(resource_type):
            return {"resource": resource_type, "denormalization": {}, "indexes": []}

    config_loader = _ConfigLoader()

    def denormalize(self, resource, warnings=None):
        return {**resource, "_search": {"logicalId": resource["id"]}}


@dataclass
class _FakeMqlContext:
    db: object


@pytest.mark.asyncio
async def test_validation_gate_prevents_partial_write(monkeypatch):
    writes = []

    class _Adapter:
        def __init__(self, db, collection_prefix="", **kwargs):
            pass

        def persist_many(self, resources, *, mode="upsert"):
            writes.extend(resources)
            return {"processed": len(resources), "inserted": len(resources), "matched": 0, "updated": 0, "unchanged": 0, "by_resource_type": {}}

    monkeypatch.setattr(importer, "_build_denormalizer", lambda cfg: _FakeDenormalizer())
    monkeypatch.setattr(importer, "MongoFHIRStorageAdapter", _Adapter)
    monkeypatch.setattr(importer.bridge, "build_mql_context", lambda *args, **kwargs: _FakeMqlContext(db=object()))
    monkeypatch.setattr(importer.bridge, "close_mql_context", lambda ctx: None)

    report = await importer.fhir_import_resources(
        _ctx(),
        {
            "resources": [
                {"resourceType": "Patient", "id": "good"},
                {"resourceType": "Patient", "id": "bad id"},
            ],
            "validation_level": "structure",
            "fail_on_error": True,
        },
    )
    assert report["ok"] is False
    assert report["committed"] is False
    assert writes == []


@pytest.mark.asyncio
async def test_required_profile_failure_prevents_any_write(monkeypatch):
    writes = []

    class _Adapter:
        def __init__(self, db, collection_prefix="", **kwargs):
            pass

        def persist_many(self, resources, *, mode="upsert"):
            writes.extend(resources)
            return {"processed": len(resources)}

    async def _profile_failure(ctx, config, resources, *, resource_indexes=None):
        assert resource_indexes == [0]
        return {
            "enforced": True,
            "checked": 1,
            "passed": 0,
            "failed": 1,
            "failed_resource_indexes": [0],
            "findings": [
                {
                    "index": 0,
                    "severity": "error",
                    "code": "FHIR_PROFILE_INVALID",
                    "message": "Customer profile validation failed",
                }
            ],
        }

    monkeypatch.setattr(importer, "_build_denormalizer", lambda cfg: _FakeDenormalizer())
    monkeypatch.setattr(importer, "MongoFHIRStorageAdapter", _Adapter)
    monkeypatch.setattr(
        importer.profile_validation, "validate_profiles", _profile_failure
    )

    report = await importer.fhir_import_resources(
        _ctx(),
        {
            "resource": {"resourceType": "Patient", "id": "profile-invalid"},
            "validation_level": "structure",
            "fail_on_error": True,
        },
    )

    assert report["ok"] is False
    assert report["committed"] is False
    assert report["validation"]["profile_conformance"] is False
    assert report["validation"]["valid"] == 0
    assert writes == []


@pytest.mark.asyncio
async def test_import_projects_then_writes_valid_resources(monkeypatch):
    writes = []

    class _Adapter:
        def __init__(self, db, collection_prefix="", **kwargs):
            pass

        def persist_many(self, resources, *, mode="upsert"):
            writes.extend(resources)
            return {"processed": len(resources), "inserted": len(resources), "matched": 0, "updated": 0, "unchanged": 0, "by_resource_type": {"Patient": {"processed": len(resources)}}}

    monkeypatch.setattr(importer, "_build_denormalizer", lambda cfg: _FakeDenormalizer())
    monkeypatch.setattr(importer, "MongoFHIRStorageAdapter", _Adapter)
    monkeypatch.setattr(importer.bridge, "build_mql_context", lambda *args, **kwargs: _FakeMqlContext(db=object()))
    monkeypatch.setattr(importer.bridge, "close_mql_context", lambda ctx: None)

    async def _ensure_indexes(*args, **kwargs):
        return {"ok": True, "indexes": [], "skipped": [], "warnings": []}

    monkeypatch.setattr(importer.indexes, "fhir_ensure_indexes", _ensure_indexes)

    report = await importer.fhir_import_resources(
        _ctx(),
        {
            "ndjson": '{"resourceType":"Patient","id":"p1"}\n',
            "validation_level": "structure",
        },
    )
    assert report["ok"] is True
    assert report["committed"] is True
    assert report["write"]["inserted"] == 1
    assert writes[0]["_search"] == {"logicalId": "p1"}
    assert writes[0]["_compartments"] == {}
    assert writes[0]["_kehrnel"]["storage_schema_version"] == "3"


@pytest.mark.asyncio
async def test_import_cannot_disable_mandatory_indexes():
    with pytest.raises(KehrnelError) as exc:
        await importer.fhir_import_resources(
            _ctx(),
            {"resource": {"resourceType": "Patient", "id": "p1"}, "ensure_indexes": False},
        )
    assert getattr(exc.value, "code", None) == "FHIR_PERSISTENCE_INVARIANT_REQUIRED"


def test_storage_adapter_bulk_contract_preserves_projection():
    captured = []

    class _Result:
        inserted_count = 0
        upserted_count = 1
        matched_count = 0
        modified_count = 0

    class _Collection:
        def list_indexes(self):
            return [{"name": "_id_", "key": {"_id": 1}}]

        def create_index(self, fields, **options):
            assert fields == [("id", 1)]
            assert options == {"unique": True, "name": "id_unique"}
            return "id_unique"

        def bulk_write(self, operations, ordered=False):
            captured.extend(operations)
            assert ordered is False
            return _Result()

    class _Db:
        def __getitem__(self, name):
            assert name == "tenant_Patient"
            return _Collection()

    versions = ProjectionVersions(
        fhir_release="R5",
        projection_contract_version="v1:global",
        resource_projection_versions={"Patient": "v1:patient"},
    )
    projected = stamp_projection_metadata(
        {
            "resourceType": "Patient",
            "id": "p1",
            "_search": {"family": "smith"},
            "_compartments": {},
        },
        versions,
    )
    result = MongoFHIRStorageAdapter(
        _Db(),
        "tenant_",
        projection_versions=versions,
    ).persist_many([projected])
    assert result["processed"] == 1
    assert result["inserted"] == 1
    assert len(captured) == 1
    update_pipeline = captured[0]._doc
    preserved_fields = update_pipeline[0]["$replaceWith"]["$arrayToObject"]["$concatArrays"][0]["$filter"]["cond"]["$in"][1]
    assert preserved_fields == ["_custom", "_enrichments", "_id"]


def test_storage_adapter_rejects_stale_projection_metadata():
    versions = ProjectionVersions(
        fhir_release="R5",
        projection_contract_version="v1:current",
        resource_projection_versions={"Patient": "v1:current-patient"},
    )
    stale = stamp_projection_metadata(
        {"resourceType": "Patient", "id": "p1"},
        ProjectionVersions(
            fhir_release="R5",
            projection_contract_version="v1:old",
            resource_projection_versions={"Patient": "v1:old-patient"},
        ),
    )

    with pytest.raises(ValueError, match="projection contract version is stale"):
        MongoFHIRStorageAdapter(object(), projection_versions=versions).persist_many([stale])
