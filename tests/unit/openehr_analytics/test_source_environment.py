"""Environment source selection is scoped, pinned, read-only and portable."""
from copy import deepcopy
from unittest.mock import MagicMock
import pytest
from kehrnel.engine.domains.openehr.analytics.source_environment import source_from_strategy, configuration_for_link
from kehrnel.engine.core.integrations.hdl.bindings_resolver import _analytics_source_uri


def profile(**changes):
    args = dict(environment_id='ibm-pre', database='PRE', strategy_id='openehr.rps_dual_ibm', config={})
    args.update(changes)
    return source_from_strategy(**args)


def test_ibm_source_inherits_operational_settings_without_io():
    p = profile(config={'collections': {'compositions': {'name': 'Compositions'}, 'codes': {'name': 'ArchetypeCodes'}, 'shortcuts': {'name': 'CodedValues'}}, 'sourceReader': {'dictionary_format': 'ctti_rows', 'fields': {'committed_at': 'updated'}}})
    assert (p['database'], p['collection'], p['connection_ref']) == ('PRE', 'Compositions', 'ibm-pre')
    assert p['nodes']['separator'] == '/'
    assert p['nodes']['at_prefixes']['D'] == 'at000'
    assert p['nodes']['instance_mode'] == 'local_preorder'
    assert p['fields']['committed_at'] == 'updated'
    assert p['codes_collection'] == 'ArchetypeCodes'
    assert profile()['dictionary_format'] == 'kehrnel_documents'
    assert profile(database='other')['source_config_digest'] != profile()['source_config_digest']


def test_source_strategy_and_analytics_activation_are_separate():
    normal = profile(strategy_id='openehr.rps_dual')
    assert normal['nodes']['separator'] == ':'
    assert normal['profile'] == 'rps_dual_full_chain_v1'
    assert normal['nodes']['instance_mode'] == 'full_chain'
    with pytest.raises(ValueError, match='operational'):
        profile(strategy_id='openehr.analytics')


def test_authored_operational_overrides_win():
    link = {'configurationSource': 'authored', 'kehrnel': {'config': {'database': 'stale'}}, 'mergedConfig': {'collections': {'codes': {'name': 'base'}}}, 'configOverrides': {'collections': {'codes': {'name': 'current'}}}}
    cfg = configuration_for_link(link)
    assert 'database' not in cfg
    assert cfg['collections']['codes']['name'] == 'current'


def store(source=None, *, paused=False):
    source = source or profile()
    db = {name: MagicMock() for name in ('teams', 'users', 'environment_controls', 'environment_secrets')}
    holder = {'_id': 'owner', 'environments': [{'id': 'analytics'}, {'id': 'ibm-pre', 'database': 'PRE', 'strategyLinks': [{'strategyId': 'openehr.rps_dual_ibm', 'mergedConfig': {}}]}]}
    db['teams'].find_one.return_value = holder
    db['users'].find_one.return_value = None
    db['environment_controls'].find_one.return_value = {'mode': 'paused' if paused else 'read_only'}
    db['environment_secrets'].find_one.return_value = {'sealedUri': {'synthetic': True}}
    return db, holder


def test_connection_resolved_only_from_same_owner_without_writes(monkeypatch):
    db, holder = store()
    monkeypatch.setattr('kehrnel.engine.core.integrations.hdl.bindings_resolver._decrypt_sealed_uri', lambda value: 'synthetic://connection')
    assert _analytics_source_uri(db, 'analytics', profile()) == 'synthetic://connection'
    db['environment_secrets'].find_one.assert_called_once_with({'envId': 'ibm-pre', 'ownerType': 'team', 'ownerId': 'owner'})
    assert all(call[0].endswith('find_one') or call[0] == '__getitem__' for collection in db.values() for call in collection.mock_calls)


@pytest.mark.parametrize('case', ['other_owner', 'paused', 'drift', 'forged', 'same_environment'])
def test_source_binding_fails_closed(case):
    db, holder = store(paused=case == 'paused')
    source = profile()
    if case == 'other_owner': holder['environments'] = holder['environments'][:1]
    if case == 'drift': holder['environments'][1]['database'] = 'changed'
    if case == 'forged': source['collection'] = 'another'
    if case == 'same_environment': source['environment_id'] = 'analytics'
    with pytest.raises(ValueError): _analytics_source_uri(db, 'analytics', source)
    db['environment_secrets'].find_one.assert_not_called()
