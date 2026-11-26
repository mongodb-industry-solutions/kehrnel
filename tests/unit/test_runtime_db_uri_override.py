from kehrnel.engine.core.runtime import StrategyRuntime

BINDINGS = {"db": {"provider": "mongodb", "uri": "mongodb+srv://remote", "database": "fhir_store"}, "extras": {}}


def test_override_replaces_only_the_db_uri(monkeypatch):
    monkeypatch.setenv("KEHRNEL_DB_URI_OVERRIDE", "mongodb://127.0.0.1:27017")

    resolved = StrategyRuntime._apply_db_uri_override(BINDINGS)

    assert resolved["db"] == {"provider": "mongodb", "uri": "mongodb://127.0.0.1:27017", "database": "fhir_store"}
    assert resolved["extras"] == {}
    assert BINDINGS["db"]["uri"] == "mongodb+srv://remote"


def test_bindings_unchanged_without_override(monkeypatch):
    monkeypatch.delenv("KEHRNEL_DB_URI_OVERRIDE", raising=False)

    assert StrategyRuntime._apply_db_uri_override(BINDINGS) is BINDINGS
