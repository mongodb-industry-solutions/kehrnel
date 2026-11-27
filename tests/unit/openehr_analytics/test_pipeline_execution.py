"""The GUI operations and standalone converter share configuration and results."""
import asyncio
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import jsonschema
import pytest
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.analytics.strategy import OpenEHRAnalyticsStrategy
from kehrnel.engine.domains.openehr.analytics import execution
from kehrnel.engine.domains.openehr.analytics.setup import validate_setup, worker_environment
from kehrnel.engine.domains.openehr.analytics.connections import connect

ROOT = Path(__file__).resolve().parents[3]
AT = '2026-11-01T12:00:00Z'

@pytest.fixture
def config():
    config = json.loads((ROOT / 'examples/openehr_analytics/config.canonical.json').read_text())
    config['source']['mode'] = 'upload'
    return config

@pytest.fixture
def document():
    return json.loads((ROOT / 'examples/openehr_analytics/composition.canonical.json').read_text())

def run(config, operation, payload, **context):
    return asyncio.run(OpenEHRAnalyticsStrategy().run_op(StrategyContext('test', config, **context), operation, payload))

def test_unpinned_ibm_setup_can_be_validated_and_planned_without_source_access(config, monkeypatch):
    config['source'] = dict(mode='mongodb', format='ibm', profile='ctti_ibm_legacy_v1',
                            connection_ref='IBM_PRE', database='PRE', collection='Compositions')
    client = MagicMock()
    monkeypatch.setattr(execution, 'MongoClient', client)
    assert run(config, 'analytics_validate_setup', {})['configuration'] == config
    plan = asyncio.run(OpenEHRAnalyticsStrategy().plan(StrategyContext('test', config)))
    assert plan.artifacts['source_writes'] is False
    assert len(plan.artifacts['collections']) == 11
    client.assert_not_called()

def test_sdk_runtime_activation_validates_the_same_configuration(config, tmp_path):
    from kehrnel.engine.core.runtime import StrategyRuntime
    from kehrnel.engine.core.registry import FileActivationRegistry
    from kehrnel.strategy_sdk import StrategyBindings
    registry = FileActivationRegistry(tmp_path / 'registry.json')
    runtime = StrategyRuntime(registry)
    runtime.register_manifest(OpenEHRAnalyticsStrategy().manifest)
    activation = asyncio.run(runtime.activate('test', 'openehr.analytics', '0.1.0', config,
        StrategyBindings(), domain='openehr', bindings_ref='env://SYNTHETIC_TEST_BINDINGS'))
    assert activation.config == config
    assert activation.bindings is None
    other = deepcopy(config)
    other['database'] = 'second_analytics_database'
    other['source_namespace'] = 'second-source'
    asyncio.run(runtime.activate('second', 'openehr.analytics', '0.1.0', other,
        StrategyBindings(), domain='openehr', bindings_ref='env://SECOND_BINDINGS'))
    assert registry.get_activation('test', 'openehr').config == config
    assert registry.get_activation('second', 'openehr').config == other

@pytest.mark.parametrize('change', [
    {'source': {'format': 'ibm', 'mode': 'mongodb'}},
    {'source_namespace': 'mongodb://user:password@host/db'},
    {'source': {'mode': 'upload', 'format': 'ibm', 'profile': 'ctti_ibm_legacy_v1'}},
])
def test_invalid_setup_is_rejected_before_source_io(config, change, monkeypatch):
    config.update(change)
    client = MagicMock()
    monkeypatch.setattr(execution, 'MongoClient', client)
    with pytest.raises((ValueError, jsonschema.ValidationError)):
        run(config, 'analytics_validate_setup', {})
    client.assert_not_called()

def test_source_and_output_are_distinct_and_bindings_must_match(config, monkeypatch):
    config['source'] = dict(mode='mongodb', format='canonical', connection_ref='IBM_PRE',
                            database=config['database'], collection='Compositions')
    with pytest.raises(ValueError, match='different databases'):
        run(config, 'analytics_validate_setup', {})
    config['source']['database'] = 'PRE'
    client = MagicMock()
    monkeypatch.setattr(execution, 'MongoClient', client)
    with pytest.raises(ValueError, match='not available'):
        with execution.source_connection(config, {'extras': {'analytics_source': {'connection_ref': 'other', 'uri': 'mongodb://localhost'}}}):
            pytest.fail('mismatched binding opened')
    client.assert_not_called()

def test_upload_preview_equals_direct_converter_and_does_not_open_databases(config, document, monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(execution, 'MongoClient', client)
    expected = execution.converter_for(config).convert(document, accepted_at=AT)
    result = run(config, 'analytics_process_documents', {'documents': [document], 'accepted_at': AT})
    assert result == dict(read=1, facts=len(expected.facts), written=0, unchanged=0, held=0, database_writes=False)
    client.assert_not_called()

@pytest.mark.parametrize('payload', [
    {'documents': [], 'accepted_at': AT},
    {'documents': [{}] * 26, 'accepted_at': AT},
    {'documents': [{}], 'accepted_at': AT, 'extra': True},
])
def test_request_bounds_are_enforced(config, payload):
    with pytest.raises(jsonschema.ValidationError):
        run(config, 'analytics_process_documents', payload)

def test_processing_requires_rules(config, document):
    config['rules'] = []
    with pytest.raises(ValueError, match='at least one'):
        run(config, 'analytics_process_documents', {'documents': [document], 'accepted_at': AT})

def test_uploaded_config_resolves_worker_settings_without_source_credentials(config, monkeypatch):
    settings = worker_environment({'ANALYTICS_TARGET_URI': 'mongodb://localhost'}, config)
    assert settings['ANALYTICS_TARGET_DATABASE'] == config['database']
    assert settings['ANALYTICS_SOURCE_NAMESPACE'] == config['source_namespace']
    client = MagicMock()
    monkeypatch.setattr('pymongo.MongoClient', client)
    assert connect({}, target=False, settings=config) == (None, None, None, None)
    client.assert_not_called()

def test_source_connection_is_closed_after_failure(config, monkeypatch):
    config['source'] = dict(mode='mongodb', format='canonical', connection_ref='IBM_PRE', database='PRE', collection='Compositions')
    client = MagicMock()
    monkeypatch.setattr(execution, 'MongoClient', client)
    with pytest.raises(RuntimeError):
        with execution.source_connection(config, {'extras': {'analytics_source': {'connection_ref': 'IBM_PRE', 'uri': 'mongodb://localhost'}}}):
            raise RuntimeError('test')
    client.return_value.close.assert_called_once()


def test_runtime_decoder_cache_does_not_cross_environments(config, monkeypatch):
    from contextlib import contextmanager
    config['source'] = dict(mode='mongodb', format='ibm', profile='ctti_ibm_legacy_v1',
                            connection_ref='source', database='PRE', collection='Compositions')
    loaded = []
    @contextmanager
    def source_connection(config, bindings):
        loaded.append(bindings['identity'])
        yield bindings['identity']
    monkeypatch.setattr(execution, 'source_connection', source_connection)
    monkeypatch.setattr(execution, 'converter_for', lambda config, source: source)
    strategy = OpenEHRAnalyticsStrategy()
    a = StrategyContext('tenant-a', config, bindings={'identity': 'decoder-a'})
    b = StrategyContext('tenant-b', config, bindings={'identity': 'decoder-b'})
    assert strategy.converter(a) == 'decoder-a'
    assert strategy.converter(a) == 'decoder-a'
    assert strategy.converter(b) == 'decoder-b'
    assert strategy.converter(a) == 'decoder-a'
    assert loaded == ['decoder-a', 'decoder-b', 'decoder-a']


@pytest.mark.parametrize('secret', [
    {'apiKey': 'synthetic'},
    {'nested': [{'access_token': 'synthetic'}]},
    {'label': 'https://user:synthetic@example.test'},
])
def test_nested_credentials_are_rejected_by_sdk_setup(config, secret):
    config['source']['codes'] = secret
    with pytest.raises(ValueError, match='bindings'):
        run(config, 'analytics_validate_setup', {})
