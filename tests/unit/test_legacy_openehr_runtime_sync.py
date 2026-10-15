import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from kehrnel.api.bridge.app.core import database as database_module
from kehrnel.api.domains.openehr.composition import dependencies as composition_dependencies


def _restore_settings(snapshot):
    database_module.settings.COMPOSITIONS_COLL_NAME = snapshot["compositions"]
    database_module.settings.FLAT_COMPOSITIONS_COLL_NAME = snapshot["flatten"]
    database_module.settings.SEARCH_COMPOSITIONS_COLL_NAME = snapshot["search"]
    database_module.settings.EHR_COLL_NAME = snapshot["ehr"]
    database_module.settings.EHR_CONTRIBUTIONS_COLL = snapshot["contributions"]
    database_module.settings.MONGODB_DB = snapshot["database"]
    database_module.settings.search_config.flatten_collection = snapshot["search_flatten"]
    database_module.settings.search_config.search_collection = snapshot["search_collection"]
    database_module.settings.search_config.codes_collection = snapshot["codes"]
    database_module.settings.search_config.shortcuts_collection = snapshot["shortcuts"]
    database_module.settings.search_config.search_index_name = snapshot["search_index"]
    database_module.settings.search_config.search_compositions_merge = snapshot["merge"]


def test_sync_legacy_openehr_settings_is_pure_request_configuration():
    snapshot = {
        "compositions": database_module.settings.COMPOSITIONS_COLL_NAME,
        "flatten": database_module.settings.FLAT_COMPOSITIONS_COLL_NAME,
        "search": database_module.settings.SEARCH_COMPOSITIONS_COLL_NAME,
        "ehr": database_module.settings.EHR_COLL_NAME,
        "contributions": database_module.settings.EHR_CONTRIBUTIONS_COLL,
        "database": database_module.settings.MONGODB_DB,
        "search_flatten": database_module.settings.search_config.flatten_collection,
        "search_collection": database_module.settings.search_config.search_collection,
        "codes": database_module.settings.search_config.codes_collection,
        "shortcuts": database_module.settings.search_config.shortcuts_collection,
        "search_index": database_module.settings.search_config.search_index_name,
        "merge": database_module.settings.search_config.search_compositions_merge,
    }
    activation = SimpleNamespace(
        config={
            "collections": {
                "compositions": {"name": "compositions_rps"},
                "search": {
                    "name": "compositions_search",
                    "enabled": True,
                    "atlasIndex": {"name": "search_nodes_index"},
                },
                "codes": {"name": "_codes"},
                "shortcuts": {"name": "_shortcuts"},
            }
        }
    )

    try:
        synced = database_module._sync_legacy_openehr_settings(activation, "tenant_db")
        assert synced["canonical_collection"] == snapshot["compositions"]
        assert database_module.settings.COMPOSITIONS_COLL_NAME == snapshot["compositions"]
        assert synced["flat_collection"] == "compositions_rps"
        assert synced["search_collection"] == "compositions_search"
        assert synced["codes_collection"] == "_codes"
        assert synced["shortcuts_collection"] == "_shortcuts"
        assert synced["search_index_name"] == "search_nodes_index"
        assert synced["database_name"] == "tenant_db"
        assert database_module.settings.FLAT_COMPOSITIONS_COLL_NAME == snapshot["flatten"]
        assert database_module.settings.SEARCH_COMPOSITIONS_COLL_NAME == snapshot["search"]
        assert database_module.settings.search_config.flatten_collection == snapshot["search_flatten"]
        assert database_module.settings.search_config.search_collection == snapshot["search_collection"]
        assert database_module.settings.search_config.codes_collection == snapshot["codes"]
        assert database_module.settings.search_config.shortcuts_collection == snapshot["shortcuts"]
        assert database_module.settings.search_config.search_index_name == snapshot["search_index"]
        assert database_module.settings.MONGODB_DB == snapshot["database"]
    finally:
        _restore_settings(snapshot)


def test_get_composition_config_prefers_request_scoped_activation(monkeypatch):
    activation = SimpleNamespace(
        config={
            "fields": {
                "document": {
                    "cn": "custom_cn",
                    "ehr_id": "ehrId",
                    "comp_id": "compId",
                    "tid": "templateId",
                    "v": "version",
                    "sn": "custom_sn",
                },
                "node": {
                    "p": "pathValue",
                    "pi": "pathInstance",
                    "data": "payload",
                },
            },
            "collections": {
                "codes": {"name": "custom_codes"},
            },
        }
    )
    monkeypatch.setattr(
        composition_dependencies,
        "resolve_active_openehr_context",
        AsyncMock(
            return_value={
                "activation": activation,
                "database_name": "tenant_db",
                "synced": {
                    "canonical_collection": "canonical_custom",
                    "flat_collection": "flat_custom",
                    "search_collection": "search_custom",
                    "contributions_collection": "contributions_custom",
                    "ehr_collection": "ehr_custom",
                    "codes_collection": "custom_codes",
                },
            }
        ),
    )

    result = asyncio.run(composition_dependencies.get_composition_config(request=object()))

    assert result.database == "tenant_db"
    assert result.dictionaries == "custom_codes"
    assert result.composition_fields.nodes == "custom_cn"
    assert result.composition_fields.ehr_id == "ehrId"
    assert result.composition_fields.comp_id == "compId"
    assert result.composition_fields.template_id == "templateId"
    assert result.composition_fields.version == "version"
    assert result.search_fields.nodes == "custom_sn"
    assert result.search_fields.path == "pathValue"
    assert result.search_fields.data == "payload"


def test_get_composition_config_fails_closed_without_request_collection_contract(monkeypatch):
    monkeypatch.setattr(
        composition_dependencies,
        "resolve_active_openehr_context",
        AsyncMock(
            return_value={
                "activation": SimpleNamespace(config={}),
                "database_name": "tenant_db",
                "synced": {},
            }
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(composition_dependencies.get_composition_config(request=object()))

    assert getattr(exc_info.value, "status_code", None) == 500
    assert "refusing process-global collection fallback" in str(exc_info.value.detail)


def test_openehr_activation_resolution_does_not_fall_back_to_another_domain():
    other_activation = SimpleNamespace(domain="fhir", strategy_id="fhir.resource_store")

    class Registry:
        def get_activation(self, _env_id, domain=None):
            return None if domain else other_activation

    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(strategy_runtime=SimpleNamespace(registry=Registry())))
    )

    with pytest.raises(HTTPException) as exc_info:
        database_module._get_activation(request, "tenant-a", "openehr")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_concurrent_tenants_keep_database_collections_and_mappings_request_scoped(monkeypatch):
    arrivals = 0
    all_arrived = asyncio.Event()
    observed = []

    async def fake_resolve_uri(mapping_ref, target_db, _base_dir):
        return {"mapping": mapping_ref["mapping"], "database": target_db.name}

    async def fake_create(*, db, config, mappings_path, mappings_content, field_map, coding_opts):
        nonlocal arrivals
        observed.append(
            {
                "database": db.name,
                "flat": config["collections"]["compositions"]["name"],
                "search": config["collections"]["search"]["name"],
                "mapping": mappings_content["mapping"],
            }
        )
        arrivals += 1
        if arrivals == 2:
            all_arrived.set()
        await asyncio.wait_for(all_arrived.wait(), timeout=1)
        return SimpleNamespace(config=config, tenant=db.name)

    monkeypatch.setattr(database_module, "resolve_uri_async", fake_resolve_uri)
    monkeypatch.setattr(database_module.CompositionFlattener, "create", staticmethod(fake_create))
    monkeypatch.setattr(
        database_module.CompositionUnflattener,
        "from_flattener",
        staticmethod(lambda flattener: SimpleNamespace(tenant=flattener.tenant)),
    )

    app_state = SimpleNamespace(
        flattener="startup-flattener",
        unflattener="startup-unflattener",
        config={"startup": True},
        target_db="startup-db",
    )

    def tenant(name: str):
        activation = SimpleNamespace(
            env_id=name,
            activation_id=f"act-{name}",
            config_hash=f"hash-{name}",
            strategy_id="openehr.rps_dual",
            domain="openehr",
            config={
                "transform": {"mappings": {"mapping": f"mapping-{name}"}},
                "collections": {
                    "compositions": {"name": f"flat-{name}"},
                    "search": {"name": f"search-{name}"},
                    "codes": {"name": f"codes-{name}"},
                    "shortcuts": {"name": f"shortcuts-{name}"},
                },
            },
        )
        target_db = SimpleNamespace(name=f"db-{name}")
        request = SimpleNamespace(app=SimpleNamespace(state=app_state), state=SimpleNamespace())
        synced = database_module._sync_legacy_openehr_settings(activation, target_db.name)
        context = {
            "activation": activation,
            "db": target_db,
            "database_name": target_db.name,
            "synced": synced,
            "ingestion_ready": False,
        }
        request.state._openehr_context = context
        return request, activation, target_db, synced, context

    first = tenant("a")
    second = tenant("b")

    await asyncio.gather(
        database_module._ensure_legacy_openehr_ingestion_runtime(
            first[0], activation=first[1], target_db=first[2],
            database_name=first[2].name, synced=first[3], context=first[4],
        ),
        database_module._ensure_legacy_openehr_ingestion_runtime(
            second[0], activation=second[1], target_db=second[2],
            database_name=second[2].name, synced=second[3], context=second[4],
        ),
    )

    assert sorted(observed, key=lambda item: item["database"]) == [
        {"database": "db-a", "flat": "flat-a", "search": "search-a", "mapping": "mapping-a"},
        {"database": "db-b", "flat": "flat-b", "search": "search-b", "mapping": "mapping-b"},
    ]
    assert first[4]["flattener"].tenant == "db-a"
    assert first[4]["unflattener"].tenant == "db-a"
    assert second[4]["flattener"].tenant == "db-b"
    assert second[4]["unflattener"].tenant == "db-b"
    assert first[4]["ingest_config"]["target"]["compositions_collection"] == "flat-a"
    assert second[4]["ingest_config"]["target"]["compositions_collection"] == "flat-b"
    assert first[4]["mappings_content"]["mapping"] == "mapping-a"
    assert second[4]["mappings_content"]["mapping"] == "mapping-b"

    # Startup fallbacks are not request-owned and must remain untouched.
    assert app_state.flattener == "startup-flattener"
    assert app_state.unflattener == "startup-unflattener"
    assert app_state.config == {"startup": True}
    assert app_state.target_db == "startup-db"
