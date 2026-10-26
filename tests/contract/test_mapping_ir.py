from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.contracts.artifacts import CapabilityMaturity, ExecutionState
from kehrnel.contracts.mapping_ir import MappingRule, MappingSimulationRequest
from kehrnel.engine.core.mapping_executor import simulate_mapping
from kehrnel.engine.core.mappings import load_mapping_catalog
from kehrnel.engine.core.source_profiles import load_source_profile_catalog
from kehrnel.engine.domains.openehr.templates import TemplateParser, kehrnelValidator


pytestmark = pytest.mark.filterwarnings(
    "ignore:Testing an element's truth value.*:DeprecationWarning"
)


def _mapping():
    return load_mapping_catalog().mappings[0]


def _openmrs_lab_record() -> dict[str, Any]:
    return {
        "patient": {"patient_id": 1001},
        "encounter": {
            "encounter_id": 3001,
            "uuid": "6ddaad12-9594-4b5c-a1e5-e48b49f634b7",
            "encounter_datetime": "2026-09-25T09:15:00Z",
            "creator": 42,
        },
        "obs": {
            "obs_id": 7001,
            "uuid": "9acf5e83-cef0-49de-8a79-3cfd26b42fc1",
            "person_id": 1001,
            "encounter_id": 3001,
            "concept_id": 5089,
            "value_numeric": 72.4,
            "obs_datetime": "2026-09-25T09:16:00Z",
        },
        "concept_name": {
            "concept_id": 5089,
            "name": "Weight",
            "locale": "en",
            "concept_name_type": "FULLY_SPECIFIED",
        },
        "concept_numeric": {"concept_id": 5089, "units": "kg"},
    }


def _request(mode: str, *, record: dict[str, Any] | None = None):
    mapping = _mapping()
    payload: dict[str, Any] = {
        "contract": "mapping-simulation-request/v1",
        "mappingRef": mapping.artifact_ref.to_wire(),
        "mode": mode,
    }
    if mode == "batch":
        payload["records"] = [record or _openmrs_lab_record()]
    else:
        payload["events"] = [
            {
                "eventId": "mysql-bin.0042:7001",
                "operation": "update",
                "after": record or _openmrs_lab_record(),
                "sequence": "0042:7001",
            }
        ]
    return MappingSimulationRequest.model_validate(payload)


def _leaf_pointers(value: Any, prefix: str = "") -> set[str]:
    if isinstance(value, dict):
        paths: set[str] = set()
        for key, child in value.items():
            escaped = key.replace("~", "~0").replace("/", "~1")
            paths.update(_leaf_pointers(child, f"{prefix}/{escaped}"))
        return paths
    if isinstance(value, list):
        paths = set()
        for index, child in enumerate(value):
            paths.update(_leaf_pointers(child, f"{prefix}/{index}"))
        return paths
    return {prefix}


def test_mapping_catalog_is_exactly_scoped_and_does_not_overclaim_production():
    mapping = _mapping()
    profile = next(
        profile
        for profile in load_source_profile_catalog().profiles
        if profile.artifact_ref.id == "app.openmrs.mysql"
    )

    assert mapping.capability_maturity is CapabilityMaturity.SIMULATED
    assert mapping.source.interaction_ids == ["laboratory-result-observed"]
    assert mapping.source.root_shape == "normalized_joined_record"
    assert set(mapping.target.strategy_ids) == {
        "openehr.rps_dual",
        "openehr.rps_dual_ibm",
    }
    route = next(
        route for route in profile.mapping_routes if route.target_model == "openehr"
    )
    assert route.readiness.value == "proposed"


def test_batch_and_cdc_after_image_produce_the_same_canonical_document():
    batch = simulate_mapping(_request("batch"))
    cdc = simulate_mapping(_request("cdc"))

    assert batch.state is ExecutionState.EXECUTED
    assert batch.errors == []
    assert batch.documents == cdc.documents
    assert batch.output_digest == cdc.output_digest
    assert batch.document_idempotency_keys == cdc.document_idempotency_keys
    assert batch.document_routes == cdc.document_routes
    assert batch.document_routes[0].document_id == (
        "9acf5e83-cef0-49de-8a79-3cfd26b42fc1"
    )
    assert batch.document_routes[0].subject_key == "1001"
    composition = batch.documents[0]
    assert composition["_type"] == "COMPOSITION"
    assert composition["archetype_details"]["template_id"]["value"] == (
        "sample_laboratory_v0.4"
    )
    quantity = composition["content"][0]["items"][0]["data"]["events"][0]["data"][
        "items"
    ][1]["items"][1]["value"]
    assert quantity == {"_type": "DV_QUANTITY", "magnitude": 72.4, "units": "kg"}

    opt = TemplateParser(
        "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/"
        "templates/sample_laboratory_v0_4.opt"
    )
    assert kehrnelValidator(opt).validate(composition) == []


def test_every_emitted_leaf_has_field_level_lineage():
    result = simulate_mapping(_request("batch"))

    emitted = _leaf_pointers(result.documents[0])
    lineaged = {item.target_path for item in result.lineage}
    assert lineaged == emitted
    patient_lineage = next(
        item
        for item in result.lineage
        if item.target_path == "/feeder_audit/originating_system_item_ids/1/id"
    )
    assert patient_lineage.source_path == "patient.patient_id"
    assert patient_lineage.semantic_concept_id == "healthcare.patient.local-id"


def test_required_field_and_delete_fail_closed_with_dead_letter_contract():
    missing = _openmrs_lab_record()
    del missing["obs"]["value_numeric"]
    failed = simulate_mapping(_request("batch", record=missing))

    assert failed.state is ExecutionState.FAILED
    assert failed.documents == []
    assert failed.errors[0].code == "REQUIRED_SOURCE_FIELD_MISSING"
    assert failed.errors[0].rule_id == "observation.result.value.magnitude"
    assert failed.errors[0].dead_letter_contract == "mapping-dead-letter/v1"

    mapping = _mapping()
    delete = MappingSimulationRequest.model_validate(
        {
            "contract": "mapping-simulation-request/v1",
            "mappingRef": mapping.artifact_ref.to_wire(),
            "mode": "cdc",
            "events": [
                {
                    "eventId": "mysql-bin.0042:7002",
                    "operation": "delete",
                    "documentKey": {"obs_id": 7001},
                }
            ],
        }
    )
    rejected = simulate_mapping(delete)
    assert rejected.state is ExecutionState.FAILED
    assert rejected.errors[0].code == "DELETE_NOT_SUPPORTED"

    naive_time = _openmrs_lab_record()
    naive_time["encounter"]["encounter_datetime"] = "2026-09-25T09:15:00"
    rejected_time = simulate_mapping(_request("batch", record=naive_time))
    assert rejected_time.errors[0].code == "TRANSFORM_FAILED"
    assert rejected_time.errors[0].rule_id == "composition.context.start"

    with pytest.raises(ValueError, match="constants must be scalar"):
        MappingRule.model_validate(
            {
                "ruleId": "invalid.object-constant",
                "constant": {"unsafe": "subtree"},
                "target": {"dialect": "json_pointer", "path": "/value"},
            }
        )


def test_mapping_catalog_and_simulation_are_versioned_api_contracts(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    catalog_response = client.get("/v1/mappings")

    assert catalog_response.status_code == 200
    mapping = catalog_response.json()["mappings"][0]
    assert mapping["contract"] == "mapping-definition/v1"
    assert mapping["capabilityMaturity"] == "simulated"

    request = _request("batch").to_wire()
    response = client.post(
        f"/v1/mappings/{mapping['artifactRef']['id']}/simulate",
        json=request,
    )
    assert response.status_code == 200
    assert response.json()["contract"] == "mapping-simulation-result/v1"
    assert response.json()["state"] == "executed"
