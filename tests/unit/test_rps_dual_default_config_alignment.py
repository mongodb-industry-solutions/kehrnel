from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import jsonschema
import pytest

from kehrnel.core.pack_loader import load_strategy
from kehrnel.core.registry import FileActivationRegistry
from kehrnel.core.runtime import StrategyRuntime
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.config import (
    build_flattener_config,
    build_schema_config,
    normalize_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.encoding import PathCodec
from kehrnel.engine.strategies.openehr.rps_dual.strategy import (
    DEFAULTS_PATH,
    MANIFEST,
    MANIFEST_PATH,
    SCHEMA_PATH,
    RPSDualStrategy,
    load_json,
)
from kehrnel.strategy_sdk import StrategyBindings


def test_defaults_json_matches_schema_and_python_config_builders():
    defaults = load_json(DEFAULTS_PATH)
    schema = load_json(SCHEMA_PATH)

    jsonschema.validate(defaults, schema)

    cfg = normalize_config(defaults)
    flattener_cfg = build_flattener_config(cfg)
    schema_cfg = build_schema_config(cfg)

    assert flattener_cfg["collections"]["compositions"]["name"] == defaults["collections"]["compositions"]["name"]
    assert flattener_cfg["collections"]["search"]["name"] == defaults["collections"]["search"]["name"]
    assert flattener_cfg["paths"]["separator"] == defaults["paths"]["separator"]
    assert flattener_cfg["apply_shortcuts"] == defaults["transform"]["apply_shortcuts"]
    assert flattener_cfg["target"]["codes_collection"] == defaults["collections"]["codes"]["name"]
    assert flattener_cfg["target"]["shortcuts_collection"] == defaults["collections"]["shortcuts"]["name"]
    assert flattener_cfg["envelope_fields"] == defaults["transform"]["envelope"]

    assert schema_cfg["composition"]["collection"] == defaults["collections"]["compositions"]["name"]
    assert schema_cfg["search"]["collection"] == defaults["collections"]["search"]["name"]
    assert schema_cfg["search"]["index_name"] == defaults["collections"]["search"]["atlasIndex"]["name"]
    assert schema_cfg["composition"]["ehr_id"] == defaults["fields"]["document"]["ehr_id"]
    assert schema_cfg["composition"]["composition_id_encoding"] == defaults["ids"]["composition_id"]
    assert schema_cfg["search"]["ehr_id_encoding"] == defaults["ids"]["ehr_id"]
    assert schema_cfg["search"]["sort_time"] == defaults["fields"]["document"]["sort_time"]
    assert flattener_cfg["search_fields"]["comp_id"] == defaults["fields"]["document"]["comp_id"]


def test_manifest_schema_and_spec_advertise_current_supported_surface():
    manifest = load_json(MANIFEST_PATH)
    schema = load_json(SCHEMA_PATH)
    spec_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "kehrnel"
        / "engine"
        / "strategies"
        / "openehr"
        / "rps_dual"
        / "spec.json"
    )
    spec = load_json(spec_path)

    assert "composition.fullpath" not in manifest["variants"]
    assert schema["definitions"]["pathSeparator"]["enum"] == [".", "/", ":", "|", "~"]
    assert schema["definitions"]["encodingProfile"]["enum"] == [
        "profile.codedpath",
        "profile.search_shortcuts",
    ]

    search_type = next(
        item
        for item in spec["logicalModel"]["destinations"]["types"]
        if item["id"] == "openehr.rps_dual.search_nodes.v1"
    )
    assert "comp_id" in search_type["fields"]

    search_materialize = next(
        step
        for step in spec["transformModel"]["pipeline"]
        if step["id"] == "step.materialize.sn"
    )
    search_extra_fields = [item["field"] for item in search_materialize["extraFields"]]
    assert "comp_id" in search_extra_fields

    search_catalog = next(
        item
        for item in spec["visualization"]["collectionModel"]["entities"]
        if item["id"] == "coll.search"
    )
    assert "comp_id" in search_catalog["fields"]


def test_path_codec_rejects_unsupported_profiles_instead_of_tolerating_them():
    codec = PathCodec()

    with pytest.raises(ValueError, match="Unsupported path encoding profile"):
        codec.encode_path_from_chain(["openEHR-EHR-OBSERVATION.example.v1", "at0001"], "profile.fullpath")

    with pytest.raises(ValueError, match="Unsupported path encoding profile"):
        codec.decode_path("-4.-1", "profile.fullpath")


@pytest.mark.asyncio
async def test_default_plan_materializes_expected_default_artifacts(tmp_path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    pack_dir = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "kehrnel"
        / "engine"
        / "strategies"
        / "openehr"
        / "rps_dual"
    )
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    rt.register_manifest(manifest)
    await rt.activate(
        "env-defaults",
        manifest.id,
        manifest.version,
        manifest.default_config or {},
        StrategyBindings(extras={"db": {"provider": "none"}}),
        allow_plaintext_bindings=True,
    )

    plan = await rt.dispatch("env-defaults", "plan", {})
    artifacts = (plan or {}).get("artifacts", {})

    assert set(artifacts["collections"]) == {
        "compositions_rps",
        "compositions_search",
        "_codes",
        "_shortcuts",
    }

    index_keys = {(idx["collection"], tuple(idx["keys"])) for idx in artifacts["indexes"]}
    assert ("compositions_rps", (("ehr_id", 1), ("cn.p", 1), ("time_c", 1))) in index_keys
    assert ("compositions_search", (("ehr_id", 1), ("sort_time", 1))) in index_keys

    assert len(artifacts["search_indexes"]) == 1
    search_index = artifacts["search_indexes"][0]
    assert search_index["collection"] == "compositions_search"
    assert search_index["name"] == "search_nodes_index"
    assert isinstance(search_index["definition"], dict)
    assert search_index["definition"].get("mappings")


@pytest.mark.asyncio
async def test_active_mappings_prefer_generated_search_index_definition_over_seed_definition():
    strategy = RPSDualStrategy(MANIFEST.model_copy(deep=True))
    cfg = deepcopy(strategy.defaults)
    cfg["transform"]["mappings"] = {
        "analyticsTemplate": {
            "templateId": "SampleTemplate",
            "fields": [
                {
                    "path": "/content[openEHR-EHR-OBSERVATION.test.v1]/name/value",
                    "rmType": "DV_TEXT",
                }
            ],
        }
    }
    cfg["collections"]["search"]["atlasIndex"]["definition"] = {
        "mappings": {
            "dynamic": False,
            "fields": {
                "dummy": {"type": "token"},
            },
        }
    }

    ctx = StrategyContext(
        environment_id="env-mappings",
        config=cfg,
        adapters={},
        manifest=strategy.manifest.model_copy(deep=True),
        meta={},
    )

    definition = await strategy._resolve_search_index_definition(ctx, normalize_config(cfg))
    fields = definition["mappings"]["fields"]
    sn_field = cfg["fields"]["document"]["sn"]
    data_field = cfg["fields"]["node"]["data"]

    assert "dummy" not in fields
    assert fields[sn_field]["fields"][data_field]["fields"]


@pytest.mark.asyncio
async def test_validate_config_accepts_supported_separator_override():
    strategy = RPSDualStrategy(MANIFEST.model_copy(deep=True))
    cfg = deepcopy(strategy.defaults)
    cfg["paths"]["separator"] = ":"

    ctx = StrategyContext(
        environment_id="env-valid-separator",
        config=cfg,
        adapters={},
        manifest=strategy.manifest.model_copy(deep=True),
        meta={},
    )

    await strategy.validate_config(ctx)
    assert strategy.normalized_config is not None
    assert strategy.normalized_config.paths.separator == ":"


@pytest.mark.asyncio
async def test_validate_config_rejects_query_unsafe_encoding_profile():
    strategy = RPSDualStrategy(MANIFEST.model_copy(deep=True))
    cfg = deepcopy(strategy.defaults)
    cfg["collections"]["compositions"]["encodingProfile"] = "profile.fullpath"

    ctx = StrategyContext(
        environment_id="env-invalid-profile",
        config=cfg,
        adapters={},
        manifest=strategy.manifest.model_copy(deep=True),
        meta={},
    )

    with pytest.raises(KehrnelError) as exc_info:
        await strategy.validate_config(ctx)

    assert "encodingProfile" in exc_info.value.details["errors"][0]


@pytest.mark.asyncio
async def test_validate_config_rejects_search_profile_even_when_search_sidecar_disabled():
    strategy = RPSDualStrategy(MANIFEST.model_copy(deep=True))
    cfg = deepcopy(strategy.defaults)
    cfg["collections"]["search"]["enabled"] = False
    cfg["collections"]["search"]["encodingProfile"] = "profile.fullpath"

    ctx = StrategyContext(
        environment_id="env-search-disabled",
        config=cfg,
        adapters={},
        manifest=strategy.manifest.model_copy(deep=True),
        meta={},
    )

    with pytest.raises(KehrnelError) as exc_info:
        await strategy.validate_config(ctx)

    assert "collections.search.encodingProfile" in exc_info.value.details["errors"][0]
