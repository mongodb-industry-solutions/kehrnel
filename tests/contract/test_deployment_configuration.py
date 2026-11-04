from copy import deepcopy
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.contracts.deployment_configuration import DeploymentCompilationRequest
from kehrnel.engine.core.deployment_identity import content_digest
from kehrnel.engine.core.deployment_configuration import compile_deployment_configuration
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy


ROOT = Path(__file__).resolve().parents[2] / "src/kehrnel/engine/strategies/openehr"
TEMPLATE = {"templateId": "CatSalut.Example", "fields": [
    {"name": "amount", "path": "/context/value", "rmType": "DV_QUANTITY", "extract": "value.magnitude"},
    {"name": "time", "path": "/context/start_time/value", "rmType": "DV_DATE_TIME"},
]}


def manifest(strategy="rps_dual"):
    return load_strategy("openehr." + strategy, ROOT / strategy)


def request(**kwargs):
    return DeploymentCompilationRequest(config={"database": "catsalut_local"}, templates=[TEMPLATE], **kwargs)


@pytest.mark.asyncio
async def test_deterministic_portable_compilation_and_extraction_digest():
    first = await compile_deployment_configuration(manifest(), request())
    second = await compile_deployment_configuration(manifest(), request())
    assert first == second
    payload = {key: value for key, value in first.items() if key != "digest"}
    assert content_digest(payload) == first["digest"]
    assert first["readiness"] == {"indexes": True, "projection": True, "runtime": "not_observed"}
    assert first["templates"][0]["fields"][0]["extract"] == "value.magnitude"
    assert "file://" not in str(first["config"])
    fields = first["indexes"]["atlasSearch"][0]["definition"]["mappings"]["fields"]
    assert fields["sn"]["fields"]["data"]["fields"]["v"]["fields"]["magnitude"]["type"] == "number"
    changed = deepcopy(TEMPLATE)
    changed["fields"][0]["extract"] = "value.units"
    other = await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "catsalut_local"}, templates=[changed]))
    assert other["digest"] != first["digest"]


@pytest.mark.asyncio
async def test_ibm_aliases_optional_indexes_and_disabled_search():
    compiled = await compile_deployment_configuration(manifest("rps_dual_ibm"), request())
    assert compiled["config"]["fields"]["document"]["comp_id"] == "version"
    assert compiled["config"]["fields"]["node"]["pi"] == "li"
    assert compiled["indexes"]["atlasSearch"][0]["definition"]["mappings"]["fields"]["ehr_id"]["type"] == "uuid"
    assert compiled["projection"]["shortcutValuePrefix"] == "$>"
    assert compiled["projection"]["archetypeCodeValueType"] == "string"
    pack = manifest()
    pack.pack_spec["storageModel"]["stores"][0]["indexes"].append({"id": "optional_example", "type": "btree", "fields": ["tid"], "autoCreate": False, "requirement": "optional"})
    with_optional = await compile_deployment_configuration(pack, request())
    optional = next(item for item in with_optional["indexes"]["btree"] if item.get("id") == "optional_example")
    assert optional["autoCreate"] is False
    disabled = await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "catsalut_local", "collections": {"search": {"enabled": False}}}))
    assert not disabled["indexes"]["atlasSearch"]
    assert all(item["collection"] != "compositions_search" for item in disabled["indexes"]["btree"])


@pytest.mark.asyncio
async def test_strict_export_rejects_empty_search_duplicates_and_secrets():
    with pytest.raises(KehrnelError, match="analytics"):
        await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "local"}))
    with pytest.raises(KehrnelError) as exc:
        await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "local"}, templates=[TEMPLATE, TEMPLATE]))
    assert exc.value.code == "DEPLOYMENT_TEMPLATE_AMBIGUOUS"
    with pytest.raises(KehrnelError) as exc:
        await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "local", "password": "do-not-export"}, templates=[TEMPLATE]))
    assert exc.value.code == "DEPLOYMENT_SECRET_FORBIDDEN"


@pytest.mark.asyncio
async def test_custom_shortcuts_and_incomplete_dictionary_are_explicit():
    pack = manifest()
    pack.pack_spec["storageModel"]["stores"][0]["indexes"].append({"id": "custom_archetype", "type": "btree", "fields": ["cn.data.ani"]})
    compiled = await compile_deployment_configuration(pack, request(dictionaries={"shortcuts": {"keys": {"value": "VAL", "archetype_node_id": "ARC"}}}))
    index = next(item for item in compiled["indexes"]["btree"] if item.get("id") == "custom_archetype")
    assert "cn.data.ARC" in [item[0] for item in index["keys"]]
    template = {"templateId": "New", "fields": [{"path": "/content[openEHR-EHR-OBSERVATION.not_in_dictionary.v1]/value/magnitude", "rmType": "DV_QUANTITY"}]}
    result = await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "local"}, templates=[template], dictionaries={"codes": {}}))
    assert result["readiness"]["indexes"] is True
    assert result["readiness"]["projection"] is False
    assert result["diagnostics"][0]["code"] == "PROJECTION_DICTIONARY_COVERAGE_INCOMPLETE"


def test_api_compiles_without_activation_or_target_bindings(tmp_path, monkeypatch):
    from kehrnel.engine.core.runtime import StrategyRuntime
    async def forbidden(*args, **kwargs):
        raise AssertionError("Compilation must not activate a runtime")
    monkeypatch.setattr(StrategyRuntime, "activate", forbidden)
    with TestClient(create_app(str(tmp_path / "registry.json"))) as client:
        response = client.post("/strategies/openehr.rps_dual/deployment-config/compile", json=request().model_dump())
        assert response.status_code == 200, response.text
        assert response.json()["executionMode"] == "external"
        registry = client.app.state.strategy_runtime.registry
        assert not registry.activations
        response = client.post("/strategies/openehr.rps_dual/deployment-config/compile", json={**request().model_dump(), "bindings": {"db": {}}})
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_boolean_typing_and_conflicting_analytics_are_explicit():
    boolean = {"templateId": "Boolean", "fields": [{"path": "/context/value", "extract": "value.value", "rmType": "DV_BOOLEAN"}]}
    result = await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "local"}, templates=[boolean]))
    fields = result["indexes"]["atlasSearch"][0]["definition"]["mappings"]["fields"]["sn"]["fields"]["data"]["fields"]
    assert fields["v"]["fields"]["v"]["type"] == "boolean"
    conflict = deepcopy(boolean)
    conflict["fields"].append({"path": "/context/value/units", "extract": "value", "rmType": "DV_TEXT"})
    with pytest.raises(KehrnelError) as exc:
        await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "local"}, templates=[conflict]))
    assert exc.value.code == "DEPLOYMENT_ANALYTICS_INVALID"
    with pytest.raises(KehrnelError) as exc:
        await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "local", "unsupported_number": 1.5}, templates=[boolean]))
    assert exc.value.code == "DEPLOYMENT_JSON_INVALID"


@pytest.mark.asyncio
@pytest.mark.parametrize("fixture", ["deployment-projection-example.json", "deployment-projection-ibm-example.json"])
async def test_exported_projection_matches_ingest_and_rebuild_golden_example(fixture):
    import json
    from bson import json_util
    from kehrnel.engine.strategies.openehr.rps_dual.config import normalize_config, build_flattener_config, build_coding_opts
    from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
    example = json.loads((ROOT.parents[4] / "docs/examples" / fixture).read_text())
    result = await compile_deployment_configuration(manifest(example["strategyId"].split(".")[-1]), DeploymentCompilationRequest.model_validate(example["input"]))
    cfg = normalize_config(result["config"])
    config = build_flattener_config(cfg)
    config["role"] = "secondary"
    projector_class = CompositionFlattener
    if example["strategyId"].endswith("_ibm"):
        from kehrnel.engine.strategies.openehr.rps_dual_ibm.ingest.flattener import IBMCompositionFlattener
        projector_class = IBMCompositionFlattener
    projector = projector_class(db=None, config=config, mappings_path="", mappings_content=result["projection"]["mappings"], coding_opts=build_coding_opts(cfg))
    projector.code_book = result["projection"]["codes"]
    projector.shortcut_keys = result["projection"]["shortcuts"]["keys"]
    projector.shortcut_vals = result["projection"]["shortcuts"]["values"]
    projector._refresh_codec()
    base, search = projector.transform_composition(example["source"])
    portable = lambda value: json.loads(json_util.dumps(value))
    assert search is not None
    assert portable(base) == example["expectedComposition"]
    assert portable(search) == example["expectedSearch"]
    assert portable(projector.project_search_from_flattened(base)) == example["expectedSearch"]


@pytest.mark.asyncio
async def test_pinned_package_and_missing_search_definition_fail_closed():
    with pytest.raises(KehrnelError) as error:
        await compile_deployment_configuration(manifest(), request(packageDigest="sha256:" + "a" * 64))
    assert error.value.code == "DEPLOYMENT_PACKAGE_MISMATCH"
    with pytest.raises(KehrnelError) as error:
        await compile_deployment_configuration(manifest(), DeploymentCompilationRequest(config={"database": "local", "collections": {"search": {"atlasIndex": None}}}, templates=[TEMPLATE]))
    assert error.value.code == "DEPLOYMENT_SEARCH_INDEX_REQUIRED"


@pytest.mark.asyncio
@pytest.mark.parametrize("section", ["items", "keys", "values"])
async def test_malformed_shortcut_sections_are_validation_errors(section):
    with pytest.raises(KehrnelError) as error:
        await compile_deployment_configuration(manifest(), request(dictionaries={"shortcuts": {section: ["bad"]}}))
    assert error.value.status == 422
    assert error.value.code == "DEPLOYMENT_DICTIONARY_INVALID"
