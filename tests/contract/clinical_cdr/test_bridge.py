"""Contract tests for fhir.clinical_cdr bridge (config merge + Mongo bindings)."""

from __future__ import annotations

import os
import json

import pytest
import jsonschema

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import FHIRClinicalCDRStrategy, MANIFEST


def _ctx(
    *,
    config: dict | None = None,
    bindings: dict | None = None,
) -> StrategyContext:
    return StrategyContext(
        environment_id="test-env",
        config=config or {},
        bindings=bindings,
        manifest=MANIFEST,
    )


def test_resolve_strategy_config_merges_defaults_and_override():
    cfg = bridge.resolve_strategy_config(
        _ctx(config={"database": "fhir_custom", "generation": {"seed": 99}}),
    )
    assert cfg["database"] == "fhir_custom"
    assert cfg["schema_version"] == "R5"
    assert cfg["collections"]["mode"] == "per_resource_type"
    assert cfg["generation"]["seed"] == 99
    assert cfg["search"]["enabled"] is True


def test_resolve_strategy_config_rejects_invalid_mode():
    with pytest.raises(KehrnelError) as exc:
        bridge.resolve_strategy_config(
            _ctx(config={"database": "x", "collections": {"mode": "single_collection"}}),
        )
    assert exc.value.code == "INVALID_INPUT"


def test_resolve_mongo_from_bindings():
    uri, database, prefix = bridge.resolve_mongo(
        _ctx(
            config={"database": "ignored_when_binding_set"},
            bindings={
                "db": {
                    "provider": "mongodb",
                    "uri": "mongodb://localhost:27017",
                    "database": "fhir_bindings_db",
                }
            },
        )
    )
    assert uri == "mongodb://localhost:27017"
    assert database == "fhir_bindings_db"
    assert prefix == ""


def test_resolve_mongo_env_fallback(monkeypatch):
    monkeypatch.delenv("MONGODB_URI", raising=False)
    monkeypatch.delenv("MONGODB_DB", raising=False)
    monkeypatch.setenv("MONGODB_URI", "mongodb://127.0.0.1:27017")
    monkeypatch.setenv("MONGODB_DB", "fhir_from_env")

    uri, database, prefix = bridge.resolve_mongo(_ctx(config={}, bindings={}))
    assert uri == "mongodb://127.0.0.1:27017"
    assert database == "fhir_from_env"
    assert prefix == ""


def test_resolve_mongo_missing_raises(monkeypatch):
    monkeypatch.delenv("MONGODB_URI", raising=False)
    monkeypatch.delenv("MONGODB_DB", raising=False)
    with pytest.raises(KehrnelError) as exc:
        bridge.resolve_mongo(_ctx(config={}, bindings={}))
    assert exc.value.code == "BINDINGS_NOT_RESOLVED"


def test_parse_resources_payload_dict_and_legacy_list():
    counts = bridge.parse_resources_payload(
        {"resources": {"Patient": 2, "Observation": 5}, "seed": 1},
    )
    assert counts == {"Patient": 2, "Observation": 5}

    legacy = bridge.parse_resources_payload({"resource_list": ["Patient=3", "Organization=1"]})
    assert legacy["Patient"] == 3
    assert legacy["Organization"] == 1

    alias = bridge.parse_resources_payload({"resource_counts": {"Encounter": 4}})
    assert alias["Encounter"] == 4


def test_resolve_generation_payload_clinical_dev_recipe():
    cfg = bridge.resolve_strategy_config(_ctx(config={"database": "fhir_test"}))
    effective = bridge.resolve_generation_payload(cfg, {"recipe": "clinical_dev", "plan_only": True})
    resources = effective["resources"]
    assert resources["Patient"] == 50
    assert resources["Observation"] == 200
    assert len(resources) == 10


def test_resolve_generation_payload_recipe_override():
    cfg = bridge.resolve_strategy_config(_ctx(config={"database": "fhir_test"}))
    effective = bridge.resolve_generation_payload(
        cfg,
        {"recipe": "clinical_dev", "resources": {"Patient": 3}},
    )
    assert effective["resources"]["Patient"] == 3
    assert effective["resources"]["Observation"] == 200


def test_resolve_generation_payload_unknown_recipe():
    cfg = bridge.resolve_strategy_config(_ctx(config={"database": "fhir_test"}))
    with pytest.raises(KehrnelError) as exc:
        bridge.resolve_generation_payload(cfg, {"recipe": "not_a_recipe"})
    assert exc.value.code == "INVALID_INPUT"
    assert "known_recipes" in exc.value.details


def test_parse_resources_unknown_type():
    with pytest.raises(KehrnelError) as exc:
        bridge.parse_resources_payload({"resources": {"NotARealFhirResource": 1}})
    assert exc.value.code == "INVALID_INPUT"
    assert "unknown" in exc.value.details


def test_collection_name_matches_prefix_rule():
    assert bridge.collection_name("", "Patient") == "Patient"
    assert bridge.collection_name("dev_", "Patient") == "dev_Patient"


@pytest.mark.asyncio
async def test_strategy_validate_config_uses_bridge():
    strat = FHIRClinicalCDRStrategy(MANIFEST)
    ok = await strat.validate_config(_ctx(config={"database": "fhir_test"}))
    assert ok is True


@pytest.mark.parametrize("field", ["enabled", "denormalize_on_generate", "auto_index"])
def test_legacy_disabled_persistence_flags_are_coerced_true(field):
    cfg = bridge.resolve_strategy_config(
        _ctx(config={"database": "fhir_test", "search": {field: False}})
    )
    assert cfg["search"][field] is True


@pytest.mark.parametrize("field", ["enabled", "denormalize_on_generate", "auto_index"])
def test_activation_schema_rejects_disabled_persistence_invariant(field):
    schema = json.loads(bridge.DEFAULTS_PATH.with_name("schema.json").read_text(encoding="utf-8"))
    config = bridge.load_pack_defaults()
    config["search"][field] = False
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(config, schema)


@pytest.mark.asyncio
async def test_strategy_run_op_unknown_raises_value_error():
    strat = FHIRClinicalCDRStrategy(MANIFEST)
    with pytest.raises(ValueError, match="not supported"):
        await strat.run_op(_ctx(), "not_a_real_op", {})


@pytest.mark.asyncio
async def test_strategy_run_op_fhir_search_explain_only():
    pytest.importorskip("fhir_search_to_mql")
    strat = FHIRClinicalCDRStrategy(MANIFEST)
    result = await strat.run_op(
        _ctx(config={"database": "fhir_test"}),
        "fhir_search",
        {"resource_type": "Patient", "criteria": {"gender": "female"}, "explain_only": True},
    )
    assert result["ok"] is True
    assert result["explain_only"] is True


@pytest.mark.skipif(
    not os.getenv("FHIR_BRIDGE_INTEGRATION"),
    reason="Set FHIR_BRIDGE_INTEGRATION=1 to run live fhir-mql client smoke test",
)
def test_build_mql_context_bundled_configs():
    ctx = bridge.build_mql_context(
        "mongodb://localhost:27017",
        "fhir_bridge_test",
        config_dir=None,
        compartment_dir=None,
    )
    types = bridge.supported_search_resource_types(ctx.config_loader)
    assert "Patient" in types
    ctx.client.close()
