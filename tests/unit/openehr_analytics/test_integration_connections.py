from unittest.mock import MagicMock
import pytest
from kehrnel.engine.domains.openehr.analytics.connections import connect


def environment():
    return {
        "ANALYTICS_SOURCE_URI": "mongodb://localhost:27017",
        "ANALYTICS_SOURCE_DATABASE": "source",
        "ANALYTICS_SOURCE_COLLECTION": "compositions",
        "ANALYTICS_TARGET_URI": "mongodb://localhost:27017",
        "ANALYTICS_TARGET_DATABASE": "analytics_target",
        "ANALYTICS_SOURCE_NAMESPACE": "test-cdr",
    }


@pytest.mark.parametrize("settings, overrides, message", [
    ({"database": "wrong_target"}, {}, "target databases differ"),
    ({"collection_prefix": "analytics_"}, {"ANALYTICS_COLLECTION_PREFIX": "other_"}, "collection prefixes differ"),
])
def test_conflicting_delivery_settings_fail_before_any_client(monkeypatch, settings, overrides, message):
    client = MagicMock()
    monkeypatch.setattr("pymongo.MongoClient", client)
    with pytest.raises(ValueError, match=message):
        connect({**environment(), **overrides}, settings=settings)
    client.assert_not_called()


def test_destination_matches_reviewed_settings(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr("pymongo.MongoClient", client)
    source_client, target_client, _, store = connect(environment(), settings={
        "database": "analytics_target", "source_namespace": "test-cdr", "collection_prefix": "analytics_",
    })
    assert client.call_count == 2
    assert store.namespace == "test-cdr"
    source_client.close()
    target_client.close()


def test_canonical_source_defaults_to_upload_without_source_credentials(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr('pymongo.MongoClient', client)
    env = {k: v for k, v in environment().items() if not k.startswith('ANALYTICS_SOURCE_')}
    settings = {'database': 'analytics_target', 'source_namespace': 'test-cdr', 'source': {'format': 'canonical'}}
    source_client, target_client, source, store = connect(env, settings=settings)
    assert source_client is None and source is None
    assert client.call_count == 1
    assert store.namespace == 'test-cdr'
    target_client.close()


def test_target_connection_failure_closes_open_source_client(monkeypatch):
    source = MagicMock()
    client = MagicMock(side_effect=[source, RuntimeError('target unavailable')])
    monkeypatch.setattr('pymongo.MongoClient', client)
    with pytest.raises(RuntimeError, match='target unavailable'):
        connect(environment())
    source.close.assert_called_once()


def test_store_initialization_failure_closes_both_clients(monkeypatch):
    source, target = MagicMock(), MagicMock()
    monkeypatch.setattr('pymongo.MongoClient', MagicMock(side_effect=[source, target]))
    monkeypatch.setattr('kehrnel.engine.domains.openehr.analytics.connections.MongoProjectionStore', MagicMock(side_effect=RuntimeError('invalid store')))
    with pytest.raises(RuntimeError, match='invalid store'):
        connect(environment())
    source.close.assert_called_once()
    target.close.assert_called_once()
