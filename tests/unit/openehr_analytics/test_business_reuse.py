from copy import deepcopy
from pathlib import Path
import json
import pytest
from kehrnel.engine.domains.openehr.analytics.authoring import compile_draft, proposal_for
from kehrnel.engine.domains.openehr.analytics.business_catalog import catalog_page
from kehrnel.engine.domains.openehr.analytics.business_reuse import source_definition
from kehrnel.engine.domains.openehr.analytics.converter import Converter
from kehrnel.engine.domains.openehr.analytics.rules import RuleRevision, RuleSet
from kehrnel.engine.domains.openehr.analytics.projection import typed
from kehrnel.engine.domains.openehr.analytics.source import SourceError

ROOT = Path(__file__).resolve().parents[3]
REF = ROOT / 'src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference'
AT = '2026-12-01T00:00:00Z'
KEY = 'laboratory_test_analyte.test_result'


@pytest.fixture
def mapping():
    lab = compile_draft({'opt_xml': (REF/'templates/sample_laboratory_v0_4.opt').read_text(), 'effective_at': AT})['rule']
    payload = {'opt_xml': (ROOT/'samples/templates/T-IGR-BIOLOGY.opt').read_text(), 'effective_at': AT}
    proposal = proposal_for(payload, [lab])
    field = next(f for f in proposal['fields'] if f['path'].endswith('items[openEHR-EHR-CLUSTER.laboratory_test_analyte.v1]/items[at0001]'))
    excluded = [x['field_id'] for x in proposal['issues']]
    payload['edits'] = {'exclude_field_ids': excluded, 'reuse_variables': {field['field_id']: {
        'key': KEY, 'rule_revision': lab['revision'], 'shape_policy':'preserve_source',
        'reason': 'Reviewed as the same analyte-result slot; retain source type, analyte context and source unit.'}}}
    return lab, payload, field


def test_two_real_opts_compile_to_one_variable_with_distinct_source_bindings(mapping):
    lab, payload, field = mapping
    prior = deepcopy(lab)
    result = compile_draft(payload, [lab]); bio = result['rule']
    assert lab == prior and result['review']['approved'] is False
    page = catalog_page([lab, bio], as_of=AT, search=KEY)
    shared = next(v for v in page['items'] if v['key'] == KEY)
    assert shared['template_count'] == 2 and shared['fact_count'] is None
    assert shared['value_shapes'] == ['array', 'value']
    bindings = catalog_page([lab, bio], as_of=AT, key=KEY)['items']
    assert len({b['source_semantic_key'] for b in bindings}) == 2
    assert len({b['source_path'] for b in bindings}) == 2
    assert {b['template'] for b in bindings} == {'sample_laboratory_v0.4', 'T-IGR-BIOLOGY'}
    assert len({json.dumps(b['source_definition'],sort_keys=True) for b in bindings}) == 1
    reused = next(b for b in bindings if b['template'] == 'T-IGR-BIOLOGY')
    assert reused['reuse']['target_rule_revision'] == lab['revision']
    assert reused['source_rm_types'] == ['DV_QUANTITY', 'DV_TEXT']
    assert field['business_key'] not in {v['key'] for v in bio['catalog']['variables']}
    assert result['review']['business_reuse'][0]['to_key'] == KEY
    # A standalone worker needs no target catalog read to compile a delivered rule.
    assert RuleRevision.compile(bio).revision == bio['revision']


def test_no_automatic_merge_and_protocol_review_still_required(mapping):
    lab, payload, field = mapping
    ordinary = deepcopy(payload);ordinary['edits'].pop('reuse_variables')
    unshared = compile_draft(ordinary, [lab])['rule']
    assert next(v for v in catalog_page([lab, unshared], as_of=AT, search=KEY)['items'] if v['key']==KEY)['template_count'] == 1
    unresolved = deepcopy(payload);unresolved['edits']['exclude_field_ids'] = []
    with pytest.raises(ValueError, match='requiring review'): compile_draft(unresolved, [lab])
    with pytest.raises(ValueError, match='Different semantic identities'):
        proposal_for({**ordinary,'aliases':{field['semantic_key']:KEY}}, [lab])


@pytest.mark.parametrize('case', ['stale', 'missing_key', 'no_reason', 'wrong_element', 'wrong_representation', 'excluded'])
def test_reuse_rejects_incompatible_or_unreviewed_choices(mapping, case):
    lab, payload, field = mapping
    selection = payload['edits']['reuse_variables'][field['field_id']]
    if case == 'stale': selection['rule_revision'] = '0'*64
    elif case == 'missing_key': selection['key'] = 'unknown.variable'
    elif case == 'no_reason': selection['reason'] = ' '
    elif case == 'wrong_element': selection['key'] = next(v['key'] for v in lab['catalog']['variables'] if v['key']!=KEY)
    elif case == 'wrong_representation': payload['edits']['variables'] = {field['business_key']: {'type':'quantity','unit':'mg/L'}}
    elif case == 'excluded': payload['edits']['exclude_field_ids'].append(field['field_id'])
    with pytest.raises(ValueError): compile_draft(payload, [lab])


def test_persisted_reuse_cannot_change_contract_without_new_review(mapping):
    lab, payload, field = mapping
    rule = compile_draft(payload, [lab])['rule'];rule.pop('revision')
    next(v for v in rule['catalog']['variables'] if v['key']==KEY)['unit'] = 'invented'
    with pytest.raises(ValueError, match='contract changed'): RuleRevision.compile(rule)


def test_raw_reuse_checks_source_variant_and_preserves_typed_units():
    variable = {'key':KEY, 'type':'raw', 'rm_types':['DV_QUANTITY','DV_TEXT','DV_INTERVAL<DV_QUANTITY>']}
    source = {'value':{'_type':'DV_QUANTITY','magnitude':3.5,'units':'mmol/L'}}
    result, _ = typed(source, variable, source_types=['DV_QUANTITY','DV_TEXT'])
    assert result == source['value']
    with pytest.raises(SourceError, match='outside the reviewed'):
        typed({'value':{'_type':'DV_BOOLEAN','value':True}}, variable, source_types=['DV_QUANTITY','DV_TEXT'])


def test_projection_keeps_fact_boundaries_and_populates_shared_path(mapping):
    from kehrnel.engine.domains.openehr.templates.generator import kehrnelGenerator
    from kehrnel.engine.domains.openehr.templates.parser import TemplateParser
    import random
    lab, payload, field = mapping
    plain = deepcopy(payload);plain['edits'].pop('reuse_variables')
    plain_rule = compile_draft(plain, [lab])['rule']
    reused = compile_draft(payload, [lab])['rule']
    random.seed(20261119)
    canonical = kehrnelGenerator(TemplateParser(ROOT/'samples/templates/T-IGR-BIOLOGY.opt')).generate_random()
    document = {'_id':'synthetic-biology','ehr_id':'synthetic-owner','template_name':'T-IGR-BIOLOGY',
                'template_id':'T-IGR-BIOLOGY','composition_date':AT,'composition_version':1,'time_committed':AT,'canonicalJSON':canonical}
    original = deepcopy(document)
    before = Converter(RuleSet.compile([plain_rule]),source_namespace='synthetic').convert(document,accepted_at=AT)
    after = Converter(RuleSet.compile([lab,reused]),source_namespace='synthetic').convert(document,accepted_at=AT)
    assert document == original and after.facts
    assert [f['_id'] for f in after.facts] == [f['_id'] for f in before.facts]
    old_leaf = field['business_key'].split('.')[-1]
    old_values = [f['clinical']['laboratory_test_analyte'][old_leaf] for f in before.facts if old_leaf in f['clinical'].get('laboratory_test_analyte',{})]
    new_values = [f['clinical']['laboratory_test_analyte']['test_result'] for f in after.facts if 'test_result' in f['clinical'].get('laboratory_test_analyte',{})]
    assert old_values and old_values == new_values
    assert all('business_reuse' not in f for f in after.facts)


def test_new_epoch_preserves_history_and_reuse_reason_changes_revision(mapping):
    lab, payload, field = mapping
    ordinary = deepcopy(payload); ordinary['edits'].pop('reuse_variables')
    old = compile_draft(ordinary, [lab])['rule']
    future = {**payload, 'epoch':2, 'effective_at':'2027-01-01T00:00:00Z'}
    result = compile_draft(future, [lab,old])
    retained = RuleSet.compile([lab,old,result['rule']])
    assert retained.select('T-IGR-BIOLOGY',AT).revision == old['revision']
    assert retained.select('T-IGR-BIOLOGY','2027-02-01').revision == result['rule']['revision']
    updated = deepcopy(future)
    updated['edits']['reuse_variables'][field['field_id']]['reason'] += ' Reviewed again.'
    assert compile_draft(updated,[lab,old])['rule']['revision'] != result['rule']['revision']


def test_reuse_cannot_collapse_source_array(mapping):
    lab,payload,field=mapping
    payload['edits']['reuse_variables'][field['field_id']]['shape_policy']='first'
    with pytest.raises(ValueError,match='cannot collapse'): compile_draft(payload,[lab])
