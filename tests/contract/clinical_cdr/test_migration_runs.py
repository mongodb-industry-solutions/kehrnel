"""Contract tests for resumable FHIR migration run coordination."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import migration_runs
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import query as fhir_query
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST


def _value(document, dotted):
    value = document
    for part in dotted.split("."):
        if not isinstance(value, dict) or part not in value:
            return None, False
        value = value[part]
    return value, True


def _matches(document, query):
    for key, expected in query.items():
        actual, exists = _value(document, key)
        if isinstance(expected, dict) and "$exists" in expected:
            if exists is not bool(expected["$exists"]):
                return False
        elif isinstance(expected, dict) and "$in" in expected:
            if actual not in expected["$in"]:
                return False
        elif not exists or actual != expected:
            return False
    return True


def _set_dotted(document, dotted, value):
    target = document
    parts = dotted.split(".")
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = deepcopy(value)


def _unset_dotted(document, dotted):
    target = document
    parts = dotted.split(".")
    for part in parts[:-1]:
        target = target.get(part, {})
    target.pop(parts[-1], None)


class _Cursor(list):
    def sort(self, key, direction):
        super().sort(key=lambda item: _value(item, key)[0], reverse=direction < 0)
        return self

    def limit(self, value):
        return _Cursor(self[:value])


@dataclass
class _WriteResult:
    modified_count: int = 0


class _Collection:
    def __init__(self):
        self.documents = []

    def create_index(self, *args, **kwargs):
        return kwargs.get("name") or "index"

    def insert_one(self, document):
        self.documents.append(deepcopy(document))

    def find_one(self, query, projection=None):
        match = next((deepcopy(item) for item in self.documents if _matches(item, query)), None)
        if not match or projection is None:
            return match
        return {key: _value(match, key)[0] for key, include in projection.items() if include and _value(match, key)[1]}

    def find(self, query, projection=None):
        rows = [deepcopy(item) for item in self.documents if _matches(item, query)]
        if projection:
            rows = [
                {key: _value(item, key)[0] for key, include in projection.items() if include and _value(item, key)[1]}
                for item in rows
            ]
        return _Cursor(rows)

    def update_one(self, query, update):
        for document in self.documents:
            if not _matches(document, query):
                continue
            for key, value in (update.get("$set") or {}).items():
                _set_dotted(document, key, value)
            for key in (update.get("$unset") or {}):
                _unset_dotted(document, key)
            return _WriteResult(modified_count=1)
        return _WriteResult()

    def replace_one(self, query, replacement, upsert=False):
        for index, document in enumerate(self.documents):
            if _matches(document, query):
                self.documents[index] = deepcopy(replacement)
                return _WriteResult(modified_count=1)
        if upsert:
            self.documents.append(deepcopy(replacement))
            return _WriteResult(modified_count=1)
        return _WriteResult()


class _Database:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, _Collection())


def _ctx():
    return StrategyContext(
        environment_id="migration-env",
        config={
            "database": "tenant_fhir",
            "schema_version": "R5",
            "collections": {"mode": "per_resource_type"},
            "search": {"enabled": True, "denormalize_on_generate": True, "auto_index": True},
        },
        bindings={"db": {"uri": "mongodb://unused", "database": "tenant_fhir"}},
        manifest=MANIFEST,
    )


@pytest.mark.asyncio
async def test_chunked_run_advances_checkpoint_and_exact_retry_is_replayed(monkeypatch):
    database = _Database()
    monkeypatch.setattr(migration_runs, "_database", lambda ctx: (database, "tenant_fhir", ""))

    async def fake_import(ctx, payload, **kwargs):
        resources = payload["resources"]
        return {
            "ok": True,
            "committed": True,
            "dry_run": False,
            "validation": {"received": len(resources), "valid": len(resources), "invalid": 0, "findings": []},
            "resource_counts": {"Patient": len(resources)},
            "write": {"processed": len(resources)},
        }

    monkeypatch.setattr(migration_runs, "fhir_import_resources", fake_import)
    started = await migration_runs.fhir_migration_start(
        _ctx(),
        {"source_name": "patients.ndjson", "total_resources": 2, "total_chunks": 1, "chunk_size": 2},
    )
    run_id = started["run"]["run_id"]
    payload = {
        "run_id": run_id,
        "chunk_index": 0,
        "final": True,
        "resources": [
            {"resourceType": "Patient", "id": "p1"},
            {"resourceType": "Patient", "id": "p2"},
        ],
    }
    completed = await migration_runs.fhir_migration_import_chunk(_ctx(), payload)
    assert completed["run"]["status"] == "completed"
    assert completed["run"]["checkpoint"]["next_chunk"] == 1
    assert completed["run"]["totals"]["written"] == 2

    replay = await migration_runs.fhir_migration_import_chunk(_ctx(), payload)
    assert replay["replayed"] is True

    with pytest.raises(KehrnelError) as conflict:
        await migration_runs.fhir_migration_import_chunk(
            _ctx(),
            {**payload, "resources": [{"resourceType": "Patient", "id": "different"}]},
        )
    assert conflict.value.code == "FHIR_MIGRATION_CHUNK_CONFLICT"


@pytest.mark.asyncio
async def test_cancelled_run_rejects_the_next_chunk(monkeypatch):
    database = _Database()
    monkeypatch.setattr(migration_runs, "_database", lambda ctx: (database, "tenant_fhir", ""))
    started = await migration_runs.fhir_migration_start(_ctx(), {"source_name": "cancel.ndjson"})
    run_id = started["run"]["run_id"]
    canceled = await migration_runs.fhir_migration_cancel(_ctx(), {"run_id": run_id})
    assert canceled["run"]["status"] == "canceled"

    with pytest.raises(KehrnelError) as exc:
        await migration_runs.fhir_migration_import_chunk(
            _ctx(),
            {"run_id": run_id, "chunk_index": 0, "resources": [{"resourceType": "Patient", "id": "p1"}]},
        )
    assert exc.value.code == "JOB_CANCELED"


@pytest.mark.asyncio
async def test_reference_integrity_resolves_existing_and_reports_missing(monkeypatch):
    database = _Database()
    monkeypatch.setattr(migration_runs, "_database", lambda ctx: (database, "tenant_fhir", ""))
    started = await migration_runs.fhir_migration_start(
        _ctx(), {"source_name": "references.ndjson", "total_resources": 2}
    )
    run_id = started["run"]["run_id"]
    provenance = {"_kehrnel": {"provenance": {"migration_run_id": run_id}}}
    database["Patient"].documents.append(
        {"resourceType": "Patient", "id": "p1", **provenance}
    )
    database["Observation"].documents.extend(
        [
            {
                "resourceType": "Observation",
                "id": "o1",
                "subject": {"reference": "Patient/p1"},
                **provenance,
            },
            {
                "resourceType": "Observation",
                "id": "o2",
                "subject": {"reference": "Patient/missing"},
                **provenance,
            },
        ]
    )
    database[migration_runs.RUNS_COLLECTION].update_one(
        {"run_id": run_id},
        {"$set": {"totals.resource_counts": {"Patient": 1, "Observation": 2}}},
    )

    report = await migration_runs.fhir_reference_integrity(_ctx(), {"run_id": run_id})
    assert report["references"] == {
        "found": 2,
        "resolved": 1,
        "missing": 1,
        "ignored": {},
    }
    assert report["findings"][0]["reference"] == "Patient/missing"
    assert database[migration_runs.RUNS_COLLECTION].find_one({"run_id": run_id})["reference_integrity"]["mode"] == "informational"


def test_support_matrix_is_derived_from_runtime_capabilities(monkeypatch):
    monkeypatch.setattr(
        fhir_query,
        "fhir_capabilities",
        lambda ctx, payload: {
            "degraded": False,
            "contract_version": "1.0",
            "fhir_version": "R4",
            "release_support": {"support_tier": "minimal"},
            "schema_supported_resource_types": ["Observation", "Patient"],
            "searchable_resource_types": ["Patient"],
            "storable_resource_types": ["Patient"],
            "generatable_resource_types": [],
            "synthetic_writable_resource_types": [],
            "recipe_resource_types": [],
            "profile_conformance": False,
        },
    )
    matrix = fhir_query.fhir_support_matrix(_ctx())
    assert matrix["generated_from"] == "fhir_capabilities"
    assert matrix["rows"] == [
        {
            "resource_type": "Observation",
            "schema": True,
            "search": False,
            "write": False,
            "generate": False,
            "generate_preview": False,
            "generate_and_store": False,
            "example_recipe": False,
        },
        {
            "resource_type": "Patient",
            "schema": True,
            "search": True,
            "write": True,
            "generate": False,
            "generate_preview": False,
            "generate_and_store": False,
            "example_recipe": False,
        },
    ]
    assert "| Patient | yes | yes | yes | — | — | — |" in matrix["markdown"]
