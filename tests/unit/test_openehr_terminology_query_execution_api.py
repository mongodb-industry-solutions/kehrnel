from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.api.domains.openehr.semantics.execution import (
    MAX_SEMANTIC_EXECUTION_EXPANSION_CODES,
    MAX_SEMANTIC_EXECUTION_ROWS,
    OpenEhrTerminologyExecutionService,
)
from kehrnel.api.domains.openehr.semantics.routes import (
    get_open_ehr_terminology_execution_service,
)
from kehrnel.api.domains.openehr.semantics.service import (
    OpenEhrSemanticService,
    StoredAdl14Opt,
    get_stored_adl14_opt_loader,
)
from kehrnel.api.domains.openehr.semantics.terminology_planning import (
    OpenEhrTerminologyPlanningService,
    OpenEhrTerminologyQueryPlanRequest,
)
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import QueryPlan, StrategyContext
from kehrnel.engine.domains.openehr.templates import (
    compile_opt_semantic_catalog_xml,
)
from kehrnel.engine.strategies.openehr.rps_dual.query.executor import (
    execute as execute_rps_query,
)
from kehrnel.engine.strategies.openehr.rps_dual.projection_readiness import (
    build_semantic_projection_manifest,
    content_digest,
)


SAMPLE_LAB_OPT = Path(
    "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/"
    "templates/sample_laboratory_v0_4.opt"
)
DATA_TARGET_DIGEST = "sha256:" + "d" * 64


def _stored_opt() -> StoredAdl14Opt:
    return StoredAdl14Opt(
        template_id="sample_laboratory_v0.4",
        content=SAMPLE_LAB_OPT.read_text(encoding="utf-8"),
        data_target_digest=DATA_TARGET_DIGEST,
    )


def _request(stored: StoredAdl14Opt) -> dict[str, Any]:
    catalog = compile_opt_semantic_catalog_xml(stored.content)
    diagnosis = next(field for field in catalog.fields if field.label == "Diagnosis")
    onset = next(
        field
        for field in catalog.fields
        if field.label == "Date/time clinically recognised"
    )
    expansion_id = "snomed-heart-failure-20260131"
    return {
        "contract": "openehr-terminology-query-plan-request/v1",
        "query": {
            "contract": "openehr-semantic-query/v1",
            "scope": "cross_patient",
            "templateRef": {
                "templateId": catalog.template.template_id,
                "sourceDigest": catalog.template.source_digest,
                "catalogDigest": catalog.catalog_digest,
            },
            "select": ["ehr_id", "composition_uid"],
            "predicates": [
                {
                    "semanticUnitId": diagnosis.field_id,
                    "facet": "code",
                    "operator": "matches",
                    "terminologyExpansionRef": expansion_id,
                    "correlationGroup": "problem-instance",
                },
                {
                    "semanticUnitId": onset.field_id,
                    "facet": "value",
                    "operator": "gte",
                    "value": "2025-01-01",
                    "correlationGroup": "problem-instance",
                },
            ],
            "terminologyExpansions": [],
            "limit": 50,
        },
        "expansionRequests": [
            {
                "expansionId": expansion_id,
                "providerId": "snomed-local",
                "systemUri": "http://snomed.info/sct",
                "terminologyId": "SNOMED-CT",
                "version": "http://snomed.info/sct/version/20260131",
                "expression": "<< 84114007",
                "maxCodes": 32,
            }
        ],
    }


class _Gateway:
    def __init__(self):
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, operation: str, payload: dict[str, Any]):
        self.calls.append((operation, payload))
        version = "http://snomed.info/sct/version/20260131"
        return {
            "ok": True,
            "system": "http://snomed.info/sct",
            "version": version,
            "releaseId": "20260131",
            "concepts": [
                {"conceptId": "84114007"},
                {"conceptId": "195111005"},
            ],
            "matches": [
                {"conceptId": "84114007"},
                {"conceptId": "195111005"},
            ],
            "page": {
                "offset": 0,
                "limit": 33,
                "returned": 2,
                "hasMore": False,
                "nextOffset": None,
            },
            "provider": {
                "id": "snomed-local",
                "type": "kehrnel-strategy",
            },
            "evidence": {
                "correlationId": "tx-123",
                "providerId": "snomed-local",
                "providerType": "kehrnel-strategy",
                "system": "http://snomed.info/sct",
                "version": version,
                "releaseId": "20260131",
                "requestDigest": "request-digest",
                "resultDigest": "result-digest",
            },
        }


class _Registry:
    def __init__(self, strategy_id: str, *, domain: str = "openehr"):
        self.activation = SimpleNamespace(
            strategy_id=strategy_id,
            domain=domain,
            activation_id="activation-1",
            version="1.0.0",
            manifest_digest="sha256:" + "a" * 64,
            config_hash="sha256:" + "b" * 64,
            bindings_hash="sha256:" + "c" * 64,
        )

    def get_activation(self, environment_id: str, domain: str):
        assert environment_id == "tenant-a"
        assert domain == "openehr"
        return self.activation


class _Runtime:
    def __init__(
        self,
        *,
        strategy_id: str = "openehr.rps_dual",
        result: Any = None,
        error: Exception | None = None,
        preflight_result: Any = None,
    ):
        self.registry = _Registry(strategy_id)
        self.result = result or {
            "engine_used": "text_search_dual",
            "rows": [
                {"ehr_id": "ehr-1", "composition_uid": "composition-1"}
            ],
            "explain": {
                "scope": "cross_patient",
                "activation_id": "activation-1",
                "strategy_id": strategy_id,
                "config_hash": "sha256:" + "b" * 64,
                "manifest_digest": "sha256:" + "a" * 64,
                "collection": "compositions_search",
                "stage0": "$search",
                "pagination": {"pageSize": 51},
                "builder": {
                    "chosen": "search_pipeline_builder",
                    "compiler_engine": "search_pipeline_builder",
                    "execution_engine": "text_search_dual",
                    "scope": "cross_patient",
                    "exact_path_correlation_required": True,
                },
                "pipeline": [
                    {
                        "$search": {
                            "index": "search_nodes_index",
                            "compound": {"filter": []},
                            "sort": {"comp_id": 1},
                        }
                    },
                    {"$limit": 51},
                ],
            },
        }
        self.error = error
        self.preflight_result = preflight_result
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def dispatch(
        self,
        environment_id: str,
        operation: str,
        payload: dict[str, Any],
    ):
        self.calls.append((environment_id, operation, deepcopy(payload)))
        if operation == "op":
            if self.preflight_result is not None:
                return deepcopy(self.preflight_result)
            request = payload["payload"]
            manifest = build_semantic_projection_manifest(
                environment_id=environment_id,
                template_id=request["templateId"],
                template_source_digest=request["templateSourceDigest"],
                catalog_digest=request["catalogDigest"],
                plan_digest=request["planDigest"],
                mapping_content={"templates": []},
                projection_revision="projection-v1",
                required_paths=request["requiredPaths"],
                available_paths=request["requiredPaths"],
                atlas_index_name="search_nodes_index",
                atlas_index_definition={"mappings": {"dynamic": False}},
            )
            return {
                "contract": "openehr-semantic-projection-readiness/v1",
                "ready": True,
                "manifest": manifest,
                "rebuildState": "ready",
                "rebuildComplete": True,
                "rebuildInProgress": False,
                "rebuildWatermark": {"processedCount": 3},
                "reconciliation": {
                    "complete": True,
                    "safe": True,
                    "sourceToTargetCoverage": "verified",
                    "targetToSourceCoverage": "verified",
                    "mappedEmptyCount": 0,
                },
                "consistency": {
                    "model": "governed-writer-mutation-fence/v1",
                    "mutationSequence": 7,
                    "activeMutations": 0,
                    "governedWritersOnly": True,
                    "publication": "compare-and-set",
                },
                "atlasIndexState": "verified_ready",
                "atlasIndexLiveVerified": True,
                "reasons": [],
            }
        if self.error:
            raise self.error
        result = deepcopy(self.result)
        explain = result.get("explain")
        if isinstance(explain, dict):
            query = payload["query"]
            explain.setdefault("rawAql", query["raw_aql"])
            explain.setdefault("parameters", deepcopy(query["params"]))
            explain.setdefault("mode", "raw_aql_strategy")
            explain.setdefault(
                "data_target_digest",
                payload["__activation_precondition"]["dataTargetDigest"],
            )
        return result


def _execution_service(
    runtime: _Runtime,
    gateway: _Gateway | None = None,
) -> OpenEhrTerminologyExecutionService:
    planning = OpenEhrTerminologyPlanningService(
        semantic_service=OpenEhrSemanticService(),
        terminology_gateway=gateway or _Gateway(),
        environment_id="tenant-a",
    )
    return OpenEhrTerminologyExecutionService(
        planning_service=planning,
        runtime=runtime,
        environment_id="tenant-a",
    )


def _refresh_manifest_digest(receipt: dict[str, Any]) -> None:
    manifest = receipt["manifest"]
    manifest["manifestDigest"] = content_digest(
        {key: value for key, value in manifest.items() if key != "manifestDigest"}
    )


@pytest.mark.asyncio
async def test_execution_plans_and_dispatches_only_to_supported_active_strategy(
):
    stored = _stored_opt()
    runtime = _Runtime(strategy_id="openehr.rps_dual")
    gateway = _Gateway()

    response = await _execution_service(runtime, gateway).execute_query(
        stored,
        OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
    )

    assert response.contract == "openehr-terminology-query-execution-result/v1"
    assert response.strategy_id == "openehr.rps_dual"
    assert response.result.result_kind == "composition_match_rows"
    assert response.result.rows == [
        {"ehr_id": "ehr-1", "composition_uid": "composition-1"}
    ]
    assert response.execution_evidence.row_count == 1
    assert response.execution_evidence.requested_limit == 50
    assert response.execution_evidence.offset == 0
    assert response.execution_evidence.has_more is False
    assert response.execution_evidence.next_offset is None
    assert response.execution_evidence.projection_mutation_sequence == 7
    assert response.execution_evidence.plan_digest == response.plan.plan_digest
    assert response.execution_evidence.execution_input_digest.startswith("sha256:")
    assert response.execution_evidence.compiled_pipeline_digest.startswith("sha256:")
    assert response.execution_evidence.require_exact_path_correlation is True
    assert len(response.execution_evidence.terminology_expansion_digests) == 1
    assert gateway.calls
    assert len(runtime.calls) == 3
    preflight_environment_id, preflight_operation, preflight_payload = runtime.calls[0]
    assert preflight_environment_id == "tenant-a"
    assert preflight_operation == "op"
    assert preflight_payload["op"] == "semantic_projection_preflight"
    assert preflight_payload["payload"]["requiredPaths"]
    environment_id, operation, payload = runtime.calls[1]
    assert environment_id == "tenant-a"
    assert operation == "query"
    assert payload["domain"] == "openEHR"
    assert set(payload) == {"domain", "query", "__activation_precondition"}
    assert payload["__activation_precondition"] == {
        "activationId": "activation-1",
        "strategyId": "openehr.rps_dual",
        "configHash": "sha256:" + "b" * 64,
        "manifestDigest": "sha256:" + "a" * 64,
        "bindingsHash": "sha256:" + "c" * 64,
        "dataTargetDigest": DATA_TARGET_DIGEST,
    }
    assert payload["query"] == response.plan.execution_request.to_strategy_payload()
    assert payload["query"]["require_exact_path_correlation"] is True
    assert "195111005" in payload["query"]["raw_aql"]
    assert "ORDER BY c/uid/value ASC LIMIT 51" in payload["query"]["raw_aql"]
    assert response.execution_evidence.projection_readiness.ready is True
    assert (
        response.execution_evidence.projection_readiness.atlas_index_live_verified
        is True
    )
    assert runtime.calls[2][1] == "op"


@pytest.mark.asyncio
async def test_execution_uses_active_collection_and_comp_id_configuration():
    stored = _stored_opt()
    runtime = _Runtime()
    runtime.registry.activation.config = {
        "collections": {"search": {"name": "tenant_semantic_search"}},
        "fields": {"document": {"comp_id": "composition_key"}},
    }
    runtime.result["explain"]["collection"] = "tenant_semantic_search"
    runtime.result["explain"]["pipeline"][0]["$search"]["sort"] = {
        "composition_key": 1
    }

    response = await _execution_service(runtime).execute_query(
        stored,
        OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
    )

    assert response.result.rows
    assert response.execution_evidence.engine_used == "text_search_dual"


@pytest.mark.asyncio
async def test_execution_discards_rows_if_projection_changes_during_query():
    class MutatingRuntime(_Runtime):
        def __init__(self):
            super().__init__()
            self.preflight_count = 0

        async def dispatch(self, environment_id, operation, payload):
            result = await super().dispatch(environment_id, operation, payload)
            if operation == "op":
                self.preflight_count += 1
                if self.preflight_count == 2:
                    result["consistency"]["mutationSequence"] += 1
            return result

    stored = _stored_opt()
    runtime = MutatingRuntime()

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == (
        "OPENEHR_SEMANTIC_PROJECTION_CHANGED_DURING_QUERY"
    )
    assert [call[1] for call in runtime.calls] == ["op", "query", "op"]


@pytest.mark.asyncio
async def test_execution_fails_closed_before_query_when_projection_path_is_missing():
    class MissingPathRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            result = await super().dispatch(environment_id, operation, payload)
            if operation == "op":
                missing = result["manifest"]["coveredPaths"].pop()
                result["manifest"]["missingPaths"] = [missing]
                result["ready"] = False
                result["reasons"] = ["required_projection_paths_missing"]
                _refresh_manifest_digest(result)
            return result

    stored = _stored_opt()
    runtime = MissingPathRuntime()

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_SEMANTIC_PROJECTION_NOT_READY"
    assert caught.value.details["missingPaths"]
    assert [call[1] for call in runtime.calls] == ["op"]


@pytest.mark.asyncio
async def test_execution_rejects_stale_template_and_catalog_projection_receipt():
    class StaleCatalogRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            result = await super().dispatch(environment_id, operation, payload)
            if operation == "op":
                result["manifest"]["templateSourceDigest"] = "sha256:" + "8" * 64
                result["manifest"]["catalogDigest"] = "sha256:" + "9" * 64
                _refresh_manifest_digest(result)
            return result

    stored = _stored_opt()
    runtime = StaleCatalogRuntime()

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_SEMANTIC_PROJECTION_NOT_READY"
    assert "templateSourceDigest" in caught.value.details["mismatches"]
    assert "catalogDigest" in caught.value.details["mismatches"]
    assert [call[1] for call in runtime.calls] == ["op"]


@pytest.mark.asyncio
async def test_unready_projection_cannot_be_reported_as_a_successful_empty_cohort():
    class UnreadyEmptyRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            result = await super().dispatch(environment_id, operation, payload)
            if operation == "op":
                result["ready"] = False
                result["rebuildState"] = "building"
                result["rebuildComplete"] = False
                result["atlasIndexState"] = "configured_unverified"
                result["atlasIndexLiveVerified"] = False
                result["reasons"] = ["projection_rebuild_incomplete"]
            return result

    stored = _stored_opt()
    runtime = UnreadyEmptyRuntime(
        result={
            "engine_used": "text_search_dual",
            "rows": [],
            "explain": {},
        }
    )

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_SEMANTIC_PROJECTION_NOT_READY"
    assert [call[1] for call in runtime.calls] == ["op"]


@pytest.mark.asyncio
async def test_execution_rejects_unsupported_activation_before_terminology_call():
    stored = _stored_opt()
    runtime = _Runtime(strategy_id="openehr.other")
    gateway = _Gateway()

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime, gateway).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_SEMANTIC_EXECUTION_STRATEGY_UNSUPPORTED"
    assert caught.value.status == 409
    assert gateway.calls == []
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_rejects_result_from_switched_activation():
    stored = _stored_opt()
    runtime = _Runtime()
    runtime.result["explain"]["activation_id"] = "activation-2"

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_EXECUTION_ACTIVATION_MISMATCH"
    assert caught.value.status == 409
    assert caught.value.details["mismatches"]["activationId"] == {
        "expected": "activation-1",
        "actual": "activation-2",
    }


@pytest.mark.asyncio
async def test_execution_requires_a_verified_template_data_target():
    stored = _stored_opt()
    stored = StoredAdl14Opt(
        template_id=stored.template_id,
        content=stored.content,
    )
    runtime = _Runtime()

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_DATA_TARGET_UNVERIFIED"
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_rejects_result_from_a_different_data_target():
    stored = _stored_opt()
    runtime = _Runtime()
    runtime.result["explain"]["data_target_digest"] = "sha256:" + "e" * 64

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_EXECUTION_ACTIVATION_MISMATCH"
    assert caught.value.details["mismatches"]["dataTargetDigest"] == {
        "expected": DATA_TARGET_DIGEST,
        "actual": "sha256:" + "e" * 64,
    }


@pytest.mark.asyncio
async def test_execution_requires_correlated_pipeline_evidence():
    stored = _stored_opt()
    runtime = _Runtime()
    runtime.result["explain"].pop("builder")

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_EXACT_PATH_CORRELATION_UNVERIFIED"
    assert caught.value.status == 502


@pytest.mark.asyncio
async def test_execution_rejects_pipeline_evidence_for_different_aql():
    stored = _stored_opt()
    runtime = _Runtime()
    runtime.result["explain"]["rawAql"] = "SELECT c FROM COMPOSITION c LIMIT 1"

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_EXECUTION_INPUT_MISMATCH"
    assert "rawAql" in caught.value.details["mismatches"]


@pytest.mark.asyncio
async def test_execution_rejects_pipeline_evidence_for_different_parameters():
    stored = _stored_opt()
    runtime = _Runtime()
    runtime.result["explain"]["parameters"] = {"templateId": "other"}

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_EXECUTION_INPUT_MISMATCH"
    assert "parameters" in caught.value.details["mismatches"]


@pytest.mark.parametrize(
    ("tamper", "mismatch_key"),
    [
        ("engine", "engine"),
        ("collection", "collection"),
        ("limit", "pipeline.$limit"),
        ("skip", "pipeline.$skip"),
        ("sort", "pipeline.$search.sort"),
        ("stage0", "stage0"),
        ("builder", "builder.execution_engine"),
        ("page_size", "pagination.pageSize"),
        ("search_after", "pipeline.$search.paginationTokens"),
    ],
)
@pytest.mark.asyncio
async def test_execution_rejects_compiled_shape_that_changes_authorized_plan(
    tamper: str,
    mismatch_key: str,
):
    stored = _stored_opt()
    runtime = _Runtime()

    if tamper == "engine":
        runtime.result["engine_used"] = "mongo_pipeline"
    elif tamper == "collection":
        runtime.result["explain"]["collection"] = "compositions_rps"
    elif tamper == "limit":
        runtime.result["explain"]["pipeline"][-1] = {"$limit": 50}
    elif tamper == "skip":
        runtime.result["explain"]["pipeline"].insert(-1, {"$skip": 1})
    elif tamper == "sort":
        runtime.result["explain"]["pipeline"][0]["$search"]["sort"] = {
            "comp_id": -1
        }
    elif tamper == "stage0":
        runtime.result["explain"]["stage0"] = "$match"
    elif tamper == "builder":
        runtime.result["explain"]["builder"]["execution_engine"] = (
            "mongo_pipeline"
        )
    elif tamper == "page_size":
        runtime.result["explain"]["pagination"]["pageSize"] = 50
    elif tamper == "search_after":
        runtime.result["explain"]["pipeline"][0]["$search"][
            "searchAfter"
        ] = "unreviewed-token"

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_COMPILED_EXECUTION_MISMATCH"
    assert mismatch_key in caught.value.details["mismatches"]


@pytest.mark.asyncio
async def test_execution_returns_a_bounded_window_with_lookahead_evidence():
    stored = _stored_opt()
    body = _request(stored)
    body["query"]["limit"] = 2
    body["query"]["offset"] = 4
    runtime = _Runtime(
        result={
            "engine_used": "text_search_dual",
            "rows": [
                {"ehr_id": "ehr-1", "composition_uid": "composition-1"},
                {"ehr_id": "ehr-2", "composition_uid": "composition-2"},
                {"ehr_id": "ehr-3", "composition_uid": "composition-3"},
            ],
            "explain": {
                "scope": "cross_patient",
                "activation_id": "activation-1",
                "strategy_id": "openehr.rps_dual",
                "config_hash": "sha256:" + "b" * 64,
                "manifest_digest": "sha256:" + "a" * 64,
                "collection": "compositions_search",
                "stage0": "$search",
                "pagination": {"pageSize": 3},
                "builder": {
                    "chosen": "search_pipeline_builder",
                    "compiler_engine": "search_pipeline_builder",
                    "execution_engine": "text_search_dual",
                    "scope": "cross_patient",
                    "exact_path_correlation_required": True,
                },
                "pipeline": [
                    {
                        "$search": {
                            "index": "search_nodes_index",
                            "compound": {"filter": []},
                            "sort": {"comp_id": 1},
                        }
                    },
                    {"$skip": 4},
                    {"$limit": 3},
                ],
            },
        }
    )

    response = await _execution_service(runtime).execute_query(
        stored,
        OpenEhrTerminologyQueryPlanRequest.model_validate(body),
    )

    assert len(response.result.rows) == 2
    assert response.result.requested_limit == 2
    assert response.result.offset == 4
    assert response.result.has_more is True
    assert response.result.next_offset == 6
    assert response.execution_evidence.row_count == 2
    assert response.execution_evidence.has_more is True
    assert response.execution_evidence.next_offset == 6


@pytest.mark.asyncio
async def test_execution_rejects_rows_missing_requested_aliases():
    stored = _stored_opt()
    runtime = _Runtime()
    runtime.result["rows"] = [{"ehr_id": "ehr-1"}]

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_QUERY_RESULT_INVALID"
    assert caught.value.details["missingAliases"] == ["composition_uid"]


@pytest.mark.asyncio
async def test_execution_does_not_advertise_ibm_scalar_lineage_as_exact():
    stored = _stored_opt()
    runtime = _Runtime(strategy_id="openehr.rps_dual_ibm")
    gateway = _Gateway()

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime, gateway).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_SEMANTIC_EXECUTION_STRATEGY_UNSUPPORTED"
    assert caught.value.status == 409
    assert caught.value.details["activeStrategyId"] == "openehr.rps_dual_ibm"
    assert caught.value.details["supportedStrategyIds"] == ["openehr.rps_dual"]
    assert gateway.calls == []
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_enforces_interactive_row_limit_before_external_calls():
    stored = _stored_opt()
    runtime = _Runtime()
    gateway = _Gateway()
    body = _request(stored)
    body["query"]["limit"] = MAX_SEMANTIC_EXECUTION_ROWS + 1

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime, gateway).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(body),
        )

    assert caught.value.code == "OPENEHR_SEMANTIC_EXECUTION_LIMIT_EXCEEDED"
    assert gateway.calls == []
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_bounds_total_terminology_expansion_before_external_calls():
    stored = _stored_opt()
    runtime = _Runtime()
    gateway = _Gateway()
    body = _request(stored)
    original_predicate = deepcopy(body["query"]["predicates"][0])
    original_request = deepcopy(body["expansionRequests"][0])
    original_request["maxCodes"] = 512
    body["expansionRequests"][0] = original_request
    for suffix in ("b", "c"):
        expansion_id = f"snomed-heart-failure-20260131-{suffix}"
        predicate = deepcopy(original_predicate)
        predicate["terminologyExpansionRef"] = expansion_id
        body["query"]["predicates"].append(predicate)
        expansion = deepcopy(original_request)
        expansion["expansionId"] = expansion_id
        body["expansionRequests"].append(expansion)

    assert sum(
        item["maxCodes"] for item in body["expansionRequests"]
    ) > MAX_SEMANTIC_EXECUTION_EXPANSION_CODES
    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime, gateway).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(body),
        )

    assert caught.value.code == (
        "OPENEHR_SEMANTIC_EXECUTION_EXPANSION_LIMIT_EXCEEDED"
    )
    assert gateway.calls == []
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_rejects_a_tampered_plan_before_dispatch():
    stored = _stored_opt()
    gateway = _Gateway()
    planning = OpenEhrTerminologyPlanningService(
        semantic_service=OpenEhrSemanticService(),
        terminology_gateway=gateway,
        environment_id="tenant-a",
    )

    class TamperingPlanner:
        async def plan_query(self, stored, request):
            result = await planning.plan_query(stored, request)
            result.plan.aql += " "
            return result

    runtime = _Runtime()
    service = OpenEhrTerminologyExecutionService(
        planning_service=TamperingPlanner(),
        runtime=runtime,
        environment_id="tenant-a",
    )

    with pytest.raises(KehrnelError) as caught:
        await service.execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_SEMANTIC_PLAN_DIGEST_INVALID"
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_execution_propagates_runtime_failure_and_never_returns_empty_success():
    stored = _stored_opt()
    runtime = _Runtime(
        error=KehrnelError(
            code="OPENEHR_QUERY_EXECUTION_FAILED",
            status=502,
            message="The openEHR query could not be executed.",
            details={"engine": "text_search_dual"},
        )
    )

    with pytest.raises(KehrnelError) as caught:
        await _execution_service(runtime).execute_query(
            stored,
            OpenEhrTerminologyQueryPlanRequest.model_validate(_request(stored)),
        )

    assert caught.value.code == "OPENEHR_QUERY_EXECUTION_FAILED"
    assert caught.value.status == 502


def test_execution_http_contract_returns_plan_rows_and_evidence(tmp_path):
    stored = _stored_opt()
    runtime = _Runtime()
    execution_service = _execution_service(runtime)
    app = create_app(str(tmp_path / "registry.json"))

    class Loader:
        async def load(self, template_id: str) -> StoredAdl14Opt:
            assert template_id == stored.template_id
            return stored

    app.dependency_overrides[get_stored_adl14_opt_loader] = Loader
    app.dependency_overrides[get_open_ehr_terminology_execution_service] = (
        lambda: execution_service
    )

    response = TestClient(app).post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-executions",
        json=_request(stored),
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["contract"] == "openehr-terminology-query-execution-result/v1"
    assert body["strategyId"] == "openehr.rps_dual"
    assert body["result"]["resultKind"] == "composition_match_rows"
    assert body["result"]["engineUsed"] == "text_search_dual"
    assert body["result"]["rows"][0]["ehr_id"] == "ehr-1"
    assert body["executionEvidence"]["rowCount"] == 1
    assert body["executionEvidence"]["planDigest"] == body["plan"]["planDigest"]
    assert body["executionEvidence"]["compiledPipelineDigest"].startswith(
        "sha256:"
    )
    assert body["executionEvidence"]["requireExactPathCorrelation"] is True

    versioned = TestClient(app).post(
        f"/v1/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-executions",
        json=_request(stored),
    )
    assert versioned.status_code == 200


def test_execution_http_contract_serializes_default_object_ids(tmp_path):
    stored = _stored_opt()
    composition_id = ObjectId("66f8c790f37b6c0a97f98532")

    class ObjectIdStorage:
        async def aggregate(self, collection, pipeline):
            return [{"ehr_id": "ehr-1", "composition_uid": composition_id}]

    class ExecutingRuntime(_Runtime):
        async def dispatch(self, environment_id, operation, payload):
            if operation == "op":
                return await super().dispatch(environment_id, operation, payload)
            self.calls.append((environment_id, operation, deepcopy(payload)))
            result = await execute_rps_query(
                StrategyContext(
                    environment_id=environment_id,
                    config={},
                    adapters={"storage": ObjectIdStorage()},
                ),
                QueryPlan(
                    engine="text_search_dual",
                    plan={
                        "collection": "compositions_search",
                        "pipeline": [
                            {
                                "$search": {
                                    "index": "search_nodes_index",
                                    "compound": {"filter": []},
                                    "sort": {"comp_id": 1},
                                }
                            },
                            {"$limit": 51},
                        ],
                        "explain": {
                            "scope": "cross_patient",
                            "activation_id": "activation-1",
                            "strategy_id": "openehr.rps_dual",
                            "config_hash": "sha256:" + "b" * 64,
                            "manifest_digest": "sha256:" + "a" * 64,
                            "stage0": "$search",
                            "pagination": {"pageSize": 51},
                            "builder": {
                                "chosen": "search_pipeline_builder",
                                "compiler_engine": "search_pipeline_builder",
                                "execution_engine": "text_search_dual",
                                "scope": "cross_patient",
                                "exact_path_correlation_required": True,
                            },
                            "rawAql": payload["query"]["raw_aql"],
                            "parameters": payload["query"]["params"],
                            "mode": "raw_aql_strategy",
                            "data_target_digest": DATA_TARGET_DIGEST,
                        },
                    },
                ),
            )
            return {
                "engine_used": result.engine_used,
                "rows": result.rows,
                "explain": result.explain,
            }

    runtime = ExecutingRuntime()
    execution_service = _execution_service(runtime)
    app = create_app(str(tmp_path / "registry.json"))

    class Loader:
        async def load(self, template_id: str) -> StoredAdl14Opt:
            return stored

    app.dependency_overrides[get_stored_adl14_opt_loader] = Loader
    app.dependency_overrides[get_open_ehr_terminology_execution_service] = (
        lambda: execution_service
    )

    response = TestClient(app).post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-executions",
        json=_request(stored),
    )

    assert response.status_code == 200, response.text
    assert response.json()["result"]["rows"][0]["composition_uid"] == (
        "66f8c790f37b6c0a97f98532"
    )


def test_execution_http_contract_surfaces_runtime_failure(tmp_path):
    stored = _stored_opt()
    runtime = _Runtime(
        error=KehrnelError(
            code="OPENEHR_QUERY_EXECUTION_FAILED",
            status=502,
            message="The openEHR query could not be executed.",
            details={"engine": "text_search_dual"},
        )
    )
    app = create_app(str(tmp_path / "registry.json"))

    class Loader:
        async def load(self, template_id: str) -> StoredAdl14Opt:
            return stored

    app.dependency_overrides[get_stored_adl14_opt_loader] = Loader
    app.dependency_overrides[get_open_ehr_terminology_execution_service] = (
        lambda: _execution_service(runtime)
    )

    response = TestClient(app).post(
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-executions",
        json=_request(stored),
    )

    assert response.status_code == 502
    assert response.json() == {
        "error": {
            "code": "OPENEHR_QUERY_EXECUTION_FAILED",
            "message": "The openEHR query could not be executed.",
            "details": {"engine": "text_search_dual"},
        }
    }


def test_execution_endpoint_is_protected_by_api_authentication(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "true")
    monkeypatch.setenv("KEHRNEL_API_KEYS", "execution-test-key")
    stored = _stored_opt()
    runtime = _Runtime()
    app = create_app(str(tmp_path / "registry.json"))

    class Loader:
        async def load(self, template_id: str) -> StoredAdl14Opt:
            return stored

    app.dependency_overrides[get_stored_adl14_opt_loader] = Loader
    app.dependency_overrides[get_open_ehr_terminology_execution_service] = (
        lambda: _execution_service(runtime)
    )
    client = TestClient(app)
    url = (
        f"/api/domains/openehr/semantics/templates/{stored.template_id}/"
        "terminology-query-executions"
    )

    rejected = client.post(url, json=_request(stored))
    assert rejected.status_code == 401
    assert rejected.json()["error"]["code"] == "UNAUTHORIZED"
    assert runtime.calls == []

    accepted = client.post(
        url,
        json=_request(stored),
        headers={"X-API-Key": "execution-test-key"},
    )
    assert accepted.status_code == 200, accepted.text
