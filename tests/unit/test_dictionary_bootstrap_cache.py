from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from kehrnel.api.bridge.app.core import database as database_module


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or {}
        self.find_one_calls = []

    async def find_one(self, filter_doc, projection=None):
        self.find_one_calls.append(
            {
                "filter": dict(filter_doc),
                "projection": dict(projection or {}),
            }
        )
        return self.docs.get(filter_doc.get("_id"))


class _FakeDb:
    def __init__(self, collections):
        self.collections = collections
        self.list_collection_names_calls = 0

    async def list_collection_names(self):
        self.list_collection_names_calls += 1
        return list(self.collections.keys())

    def __getitem__(self, name):
        return self.collections[name]


def _request():
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace()),
        state=SimpleNamespace(),
    )


def _activation(*, activation_id="act-1", config_hash="cfg-1"):
    return SimpleNamespace(
        env_id="env-1",
        domain="openehr",
        strategy_id="openehr.rps_dual",
        activation_id=activation_id,
        config_hash=config_hash,
        config={
            "bootstrap": {
                "dictionariesOnActivate": {
                    "codes": "seed",
                    "shortcuts": "seed",
                }
            }
        },
    )


def _context(db, activation, runtime):
    return {
        "activation": activation,
        "db": db,
        "runtime": runtime,
        "env_id": activation.env_id,
        "domain": activation.domain,
        "database_name": "tenant_db",
        "synced": {},
        "ingestion_ready": False,
    }


def _ready_db():
    return _FakeDb(
        {
            "_codes": _FakeCollection({"ar_code": {"_id": "ar_code"}}),
            "_shortcuts": _FakeCollection({"shortcuts": {"_id": "shortcuts"}}),
        }
    )


@pytest.mark.asyncio
async def test_dictionary_bootstrap_ready_cache_skips_repeated_db_checks(monkeypatch):
    monkeypatch.setenv("KEHRNEL_DICTIONARY_READY_CACHE_TTL_SECONDS", "60")
    request = _request()
    db = _ready_db()
    runtime = SimpleNamespace(dispatch=AsyncMock())
    context = _context(db, _activation(), runtime)

    assert await database_module.ensure_active_openehr_dictionaries(request, context=context) is False
    assert await database_module.ensure_active_openehr_dictionaries(request, context=context) is False

    assert db.list_collection_names_calls == 1
    assert len(db["_codes"].find_one_calls) == 1
    assert len(db["_shortcuts"].find_one_calls) == 1
    runtime.dispatch.assert_not_called()


@pytest.mark.asyncio
async def test_dictionary_bootstrap_ready_cache_rechecks_after_ttl(monkeypatch):
    clock = {"value": 100.0}

    monkeypatch.setenv("KEHRNEL_DICTIONARY_READY_CACHE_TTL_SECONDS", "1")
    monkeypatch.setattr(database_module.time, "monotonic", lambda: clock["value"])

    request = _request()
    db = _ready_db()
    runtime = SimpleNamespace(dispatch=AsyncMock())
    context = _context(db, _activation(), runtime)

    assert await database_module.ensure_active_openehr_dictionaries(request, context=context) is False

    clock["value"] = 100.5
    assert await database_module.ensure_active_openehr_dictionaries(request, context=context) is False

    clock["value"] = 101.5
    assert await database_module.ensure_active_openehr_dictionaries(request, context=context) is False

    assert db.list_collection_names_calls == 2
    assert len(db["_codes"].find_one_calls) == 2
    assert len(db["_shortcuts"].find_one_calls) == 2
    runtime.dispatch.assert_not_called()
