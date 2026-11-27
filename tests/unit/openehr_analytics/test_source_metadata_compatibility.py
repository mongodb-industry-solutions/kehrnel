from copy import deepcopy
from pathlib import Path
import json
import pytest
from test_conversion import ibm_document
from kehrnel.engine.domains.openehr.analytics.authoring import compile_draft
from kehrnel.engine.domains.openehr.analytics.converter import Converter
from kehrnel.engine.domains.openehr.analytics.metadata_compatibility import POLICY, upgrade_rule
from kehrnel.engine.domains.openehr.analytics.rules import RuleRevision, RuleSet
from kehrnel.engine.domains.openehr.analytics.source import SourceError, resolve, validate_archetype_identities
from kehrnel.engine.domains.openehr.analytics.store import decide

ROOT = Path(__file__).resolve().parents[3]
REF = ROOT / 'src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference'
AT = '2026-11-19T00:00:00Z'


@pytest.fixture
def sample():
    xml = (REF / 'templates/sample_laboratory_v0_4.opt').read_text()
    payload = {'opt_xml': xml, 'effective_at': AT}
    rule = compile_draft(payload)['rule']
    document = json.loads((REF / 'envelopes/sample_laboratory_v0_4.ndjson').read_text().splitlines()[0])
    policy = RuleRevision.compile(rule).source_metadata_policy
    path = next(path for path, _ in policy.observations if resolve(document['canonicalJSON'], path))
    return payload, rule, document, path


def copied(document, path):
    document = deepcopy(document)
    root = document['canonicalJSON']
    node = resolve(root, path)[0][0]
    node['archetype_details'] = deepcopy(root['archetype_details'])
    return document, node


def convert(rule, document, ibm=False):
    raw, decoder = ibm_document(document) if ibm else (document, None)
    result = Converter(RuleSet.compile([rule]), source_namespace='fixture', decoder=decoder).convert(raw, accepted_at=AT)
    return result


@pytest.mark.parametrize('ibm', [False, True])
def test_exact_copied_metadata_projects_without_modifying_source_or_facts(sample, ibm):
    _, rule, original, path = sample
    document, _ = copied(original, path)
    before = deepcopy(document)
    if ibm:
        # Construct the observed IBM wire condition: ani remains the observation
        # identity, while ad is a copy of the composition metadata. The local
        # RPS writer itself derives ani from ad, so encoding the inconsistent
        # canonical object directly would synthesize a different source defect.
        raw, decoder = ibm_document(original)
        observation_id = resolve(document['canonicalJSON'], path)[0][0]['archetype_node_id']
        target = next(n for n in raw['cn'] if decoder.expand(n['data']).get('archetype_node_id') == observation_id)
        key = next(k for k in target['data'] if decoder.keys.get(k, k) == 'archetype_details')
        root_node = next(n for n in raw['cn'] if decoder.expand(n['data']).get('_type') == 'COMPOSITION')
        root_key = next(k for k in root_node['data'] if decoder.keys.get(k, k) == 'archetype_details')
        target['data'][key] = deepcopy(root_node['data'][root_key])
        raw_before = deepcopy(raw)
        result = Converter(RuleSet.compile([rule]), source_namespace='fixture', decoder=decoder).convert(raw, accepted_at=AT)
        assert raw == raw_before
    else:
        result = convert(rule, document)
    clean = convert(rule, original, ibm)
    assert document == before
    assert result.status == 'projected' and result.facts
    assert [f['clinical'] for f in result.facts] == [f['clinical'] for f in clean.facts]
    assert [f['_id'] for f in result.facts] == [f['_id'] for f in clean.facts]
    evidence = result.source_ref['source_metadata_compatibility']
    assert evidence['policy'] == POLICY and evidence['source_modified'] is False
    assert evidence['opt_digest'].startswith('sha256:')
    assert evidence['exceptions'][0]['opt_path'] == path
    assert evidence['exceptions'][0]['archetype_node_id'].startswith('openEHR-EHR-OBSERVATION.')
    assert result.diagnostics == [{'code': 'SOURCE_METADATA_COMPATIBILITY', 'count': 1, 'source_ref': result.composition_id}]
    assert all('source_metadata_compatibility' not in f for f in result.facts)
    assert 'source_metadata_compatibility' not in clean.source_ref
    assert clean.diagnostics == []


def test_old_rules_and_explicit_strict_mode_still_reject(sample):
    payload, _, original, path = sample
    strict = compile_draft({**payload, 'edits': {'source_metadata_policy': 'strict'}})['rule']
    assert RuleRevision.compile(strict).source_metadata_policy is None
    document, _ = copied(original, path)
    with pytest.raises(SourceError, match='Conflicting source'):
        convert(strict, document)
    with pytest.raises(ValueError, match='supported source metadata policy'):
        compile_draft({**payload, 'edits': {'source_metadata_policy': 'ignore_all'}})


@pytest.mark.parametrize('case', ['unrelated_id', 'wrong_type', 'unknown_path', 'partial_copy', 'wrong_template', 'internal_cluster', 'unknown_node'])
def test_exception_cannot_hide_other_metadata_conflicts(sample, case):
    _, rule, original, path = sample
    document, node = copied(original, path)
    root = document['canonicalJSON']
    if case == 'unrelated_id':
        node['archetype_details']['archetype_id']['value'] = 'openEHR-EHR-COMPOSITION.other.v1'
    elif case == 'wrong_type':
        node['_type'] = 'EVALUATION'
    elif case == 'unknown_path':
        root['unconfigured_location'] = deepcopy(node)
    elif case == 'partial_copy':
        node['archetype_details']['template_id']['value'] = 'other'
    elif case == 'wrong_template':
        root['archetype_details']['template_id']['value'] = 'other'
        node['archetype_details'] = deepcopy(root['archetype_details'])
    elif case == 'internal_cluster':
        node['protocol'] = {'_type': 'CLUSTER', 'archetype_node_id': 'at0001', 'archetype_details': deepcopy(root['archetype_details'])}
    elif case == 'unknown_node':
        node['archetype_node_id'] = 'openEHR-EHR-OBSERVATION.unknown.v1'
    before = deepcopy(document)
    with pytest.raises(SourceError, match='Conflicting source'):
        convert(rule, document)
    assert document == before


def test_repeated_observations_keep_separate_evidence(sample):
    _, rule, original, path = sample
    document, node = copied(original, path)
    root = document['canonicalJSON']
    def duplicate(value):
        if isinstance(value, list):
            if any(item is node for item in value):
                value.append(deepcopy(node))
                return True
            return any(duplicate(item) for item in value)
        if isinstance(value, dict):
            return any(duplicate(item) for item in value.values())
        return False
    assert duplicate(root)
    policy = RuleRevision.compile(rule).source_metadata_policy
    exceptions = validate_archetype_identities(root, policy)
    assert len(exceptions) == 2
    assert len({x['location'] for x in exceptions}) == 2


def test_policy_configuration_is_validated_and_digest_bound(sample):
    _, rule, _, _ = sample
    original = RuleRevision.compile(rule)
    for change in ('bad_digest', 'duplicate', 'not_observation', 'unsupported'):
        value = deepcopy(rule)
        value.pop('revision', None)
        policy = value['catalog']['templates'][0]['source_metadata_policy']
        if change == 'bad_digest': policy['opt_digest'] = 'guess'
        if change == 'duplicate': policy['observations'].append(deepcopy(policy['observations'][0]))
        if change == 'not_observation': policy['observations'][0]['archetype_id'] = policy['composition_archetype_id']
        if change == 'unsupported': policy['policy'] = 'ignore_all'
        with pytest.raises(ValueError): RuleRevision.compile(value)
    value = deepcopy(rule)
    value['catalog']['templates'][0]['source_metadata_policy']['opt_digest'] = 'sha256:' + '0' * 64
    with pytest.raises(ValueError, match='digest'):
        RuleRevision.compile(value)


def test_new_epoch_upgrade_preserves_choices_and_replay_semantics(sample):
    payload, _, original, path = sample
    strict = compile_draft({**payload, 'edits': {'source_metadata_policy': 'strict'}})['rule']
    updated = upgrade_rule(strict, payload['opt_xml'], epoch=2, effective_at='2026-11-20T00:00:00Z')
    old_template = deepcopy(strict['catalog']['templates'][0])
    new_template = deepcopy(updated['catalog']['templates'][0])
    new_template.pop('source_metadata_policy')
    assert new_template == old_template
    assert updated['catalog']['variables'] == strict['catalog']['variables']
    assert updated['retention'] == strict['retention']
    with pytest.raises(ValueError, match='new rule epoch'):
        upgrade_rule(strict, payload['opt_xml'], epoch=1, effective_at=AT)
    clean = convert(strict, original)
    converter = Converter(RuleSet.compile([strict, updated]), source_namespace='fixture')
    advanced = converter.convert(original, accepted_at='2026-11-20T00:00:00Z')
    assert decide(clean.state(), advanced.state()) == 'write'
    assert decide(advanced.state(), advanced.state()) == 'unchanged'
    document, _ = copied(original, path)
    with pytest.raises(SourceError): converter.convert(document, accepted_at=AT)
    result = converter.convert(document, accepted_at='2026-11-20T00:00:00Z')
    assert result.status == 'projected'
