import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

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


def test_sync_legacy_openehr_settings_preserves_canonical_collection():
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
        assert database_module.settings.FLAT_COMPOSITIONS_COLL_NAME == "compositions_rps"
        assert database_module.settings.SEARCH_COMPOSITIONS_COLL_NAME == "compositions_search"
        assert database_module.settings.search_config.flatten_collection == "compositions_rps"
        assert database_module.settings.search_config.search_collection == "compositions_search"
        assert database_module.settings.search_config.codes_collection == "_codes"
        assert database_module.settings.search_config.shortcuts_collection == "_shortcuts"
        assert database_module.settings.search_config.search_index_name == "search_nodes_index"
        assert database_module.settings.MONGODB_DB == "tenant_db"
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
        AsyncMock(return_value={"activation": activation, "database_name": "tenant_db"}),
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
