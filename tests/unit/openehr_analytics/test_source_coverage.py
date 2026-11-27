"""Structural coverage tests; no network, CDR persistence or clinical payload logs."""
from copy import deepcopy
from pathlib import Path
import asyncio
import json
import random
import pytest
from kehrnel.engine.domains.openehr.analytics.authoring import compile_draft, proposal_for
from kehrnel.engine.domains.openehr.analytics.converter import Converter
from kehrnel.engine.domains.openehr.analytics.rules import RuleRevision, RuleSet
from kehrnel.engine.domains.openehr.analytics.source import resolve, segments, validate_archetype_identities
from kehrnel.engine.domains.openehr.analytics.source_coverage import CoveragePlan, MAX_ISSUES, upgrade_rule
from kehrnel.engine.domains.openehr.analytics.store import decide

ROOT = Path(__file__).resolve().parents[3]
REF = ROOT / 'src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference'
AT = '2026-12-01T00:00:00Z'

@pytest.fixture
def sample():
    xml = (REF/'templates/sample_laboratory_v0_4.opt').read_text()
    payload = {'opt_xml':xml, 'effective_at':AT}
    rule = compile_draft(payload)['rule']
    document = json.loads((REF/'envelopes/sample_laboratory_v0_4.ndjson').read_text().splitlines()[0])
    return payload, rule, document


def project(rule, document):
    return Converter(RuleSet.compile([rule]), source_namespace='synthetic-coverage').convert(document, accepted_at=AT)


def extra_cluster():
    return {'_type':'CLUSTER','archetype_node_id':'openEHR-EHR-CLUSTER.synthetic_extension.v1',
            'name':{'value':'PRIVATE LABEL'},'items':[
                {'_type':'ELEMENT','archetype_node_id':'at9001','name':{'value':'PRIVATE FIELD'},
                 'value':{'_type':'DV_TEXT','value':'PRIVATE VALUE'}}]}


def test_exact_opt_sample_checked_without_expanding_stored_facts(sample):
    _, rule, document = sample
    result = project(rule,document)
    assert result.status == 'projected' and result.facts
    assert result.coverage['status'] == 'covered'
    assert result.coverage['counts']['observed_elements'] > 0
    assert result.coverage['counts']['unmodelled_elements'] == 0
    assert result.documents()['source_coverage'] == result.coverage
    assert 'source_coverage' not in result.source_ref
    assert all('source_coverage' not in f for f in result.facts)
    assert not result.coverage['conformance_validated']


def test_known_exclusions_and_absent_optional_nodes_are_not_unknown_content(sample):
    payload, _, document = sample
    proposal = proposal_for(payload)
    excluded = [f['field_id'] for f in proposal['fields'] if f['selected']]
    rule = compile_draft({**payload,'edits':{'exclude_field_ids':excluded}})['rule']
    result = project(rule,document)
    assert result.status == 'projected'
    assert result.coverage['counts']['selected_elements'] == 0
    assert result.coverage['counts']['not_selected_elements'] > 0
    document['canonicalJSON']['content'] = []
    assert project(rule,document).coverage['status'] == 'covered'


def test_unmodelled_cluster_holds_facts_and_preserves_composition_and_source(sample):
    _,rule,document=sample
    entry = document['canonicalJSON']['content'][0]['items'][0]
    parent = entry['data']['events'][0]['data']
    parent.setdefault('items',[]).append(extra_cluster())
    original=deepcopy(document)
    result=project(rule,document)
    assert document==original and result.status=='needs_rules' and not result.facts
    assert result.activity and result.source_ref and result.rule_revision==rule['revision']
    assert result.activity['lineage']['clinical_mapping']=='needs_rule_extension'
    assert result.source_ref['source_coverage']['issue_count'] >= 2
    assert result.coverage['counts']['unmodelled_elements']==1
    assert result.diagnostics[0]['code']=='SOURCE_COVERAGE_INCOMPLETE'
    assert 'PRIVATE' not in json.dumps(result.coverage)
    assert 'source_coverage' not in result.state()


def test_real_open_slots_exposed_and_unexpanded_filler_held():
    from kehrnel.engine.domains.openehr.templates.generator import kehrnelGenerator
    from kehrnel.engine.domains.openehr.templates.parser import TemplateParser
    opt=ROOT/'samples/templates/T-IGR-BIOLOGY.opt'
    payload={'opt_xml':opt.read_text(),'effective_at':AT}
    proposal=proposal_for(payload)
    assert len(proposal['slots'])==11
    assert all(s['include_expressions'] for s in proposal['slots'])
    payload['edits']={'exclude_field_ids':[i['field_id'] for i in proposal['issues']]}
    rule=compile_draft(payload)['rule']
    random.seed(20261119)
    canonical=kehrnelGenerator(TemplateParser(opt)).generate_random()
    doc={'_id':'slot-synthetic','ehr_id':'owner','template_name':'T-IGR-BIOLOGY',
         'template_id':'T-IGR-BIOLOGY','composition_version':1,'composition_date':AT,'canonicalJSON':canonical}
    assert project(rule,doc).status=='projected'
    slot=next(s for s in proposal['slots'] if s['rm_type']=='CLUSTER' and s['path'].startswith('/content')
              and resolve(canonical,s['path'].rsplit('/',1)[0]))
    parent=resolve(canonical,slot['path'].rsplit('/',1)[0])[0][0]
    parent.setdefault(segments(slot['path'])[-1][0],[]).append(extra_cluster())
    result=project(rule,doc)
    assert result.status=='needs_rules' and not result.facts
    assert any(slot['path'] in i['candidate_slots'] for i in result.coverage['issues'])
    assert all(i['slot_validity']=='not_evaluated' for i in result.coverage['issues'])


def test_unknown_element_under_known_cluster_and_repetition(sample):
    _,rule,doc=sample
    group=next(g for g in rule['catalog']['templates'][0]['groups'] if g['definition']['entry_type']=='OBSERVATION' and resolve(doc['canonicalJSON'],g['anchor']))
    parent=resolve(doc['canonicalJSON'],group['anchor'])[0][0]
    while 'items' not in parent:
        parent=parent['data']
    parent['items'].extend([extra_cluster()['items'][0]] * (MAX_ISSUES+5))
    result=project(rule,doc)
    assert result.status=='needs_rules'
    assert result.coverage['issue_count']>MAX_ISSUES
    assert len(result.coverage['issues'])==MAX_ISSUES and result.coverage['issues_truncated']
    assert result.coverage['counts']['unmodelled_elements']==MAX_ISSUES+5


def test_legacy_rules_are_explicitly_unchecked_and_upgrade_preserves_choices(sample):
    payload,rule,doc=sample
    legacy=deepcopy(rule);legacy.pop('revision');legacy.pop('_id',None)
    legacy['catalog']['templates'][0].pop('source_coverage')
    legacy=RuleRevision.compile(legacy).document()
    assert project(legacy,doc).documents()['source_coverage']['status']=='not_checked'
    upgraded=upgrade_rule(legacy,payload['opt_xml'],epoch=2,effective_at='2027-01-01T00:00:00Z')
    old=legacy['catalog']['templates'][0];new=upgraded['catalog']['templates'][0]
    assert new['groups']==old['groups'] and new['source_metadata_policy']==old['source_metadata_policy']
    assert upgraded['retention']==legacy['retention'] and upgraded['revision']!=legacy['revision']
    rs=RuleSet.compile([legacy,upgraded])
    assert rs.select(legacy['catalog']['templates'][0]['template_name'],AT).revision==legacy['revision']
    with pytest.raises(ValueError,match='epoch'):
        upgrade_rule(legacy,payload['opt_xml'],epoch=1,effective_at=AT)


def test_wrong_root_identity_and_wrong_structural_type_hold(sample):
    _,rule,doc=sample
    # Remove root archetype_details too, otherwise the independent identity validator rejects first.
    doc['canonicalJSON'].pop('archetype_details',None)
    assert project(rule,doc).status=='needs_rules'


def test_named_siblings_preserve_path_correlation():
    inventory={'contract':'openehr-source-structure/v1','opt_digest':'sha256:'+'a'*64,'template':'T',
               'composition_archetype_id':'openEHR-EHR-COMPOSITION.test.v1',
               'nodes':[{'path':'','rm_type':'COMPOSITION'},
                        {'path':"/content[at0001 and name/value='A']",'rm_type':'CLUSTER'},
                        {'path':"/content[at0001 and name/value='A']/items[at0002]",'rm_type':'ELEMENT'},
                        {'path':"/content[at0001 and name/value='B']",'rm_type':'CLUSTER'},
                        {'path':"/content[at0001 and name/value='B']/items[at0003]",'rm_type':'ELEMENT'}], 'slots':[]}
    plan=CoveragePlan.compile(inventory,{'template_name':'T','groups':[]})
    root={'_type':'COMPOSITION','archetype_node_id':inventory['composition_archetype_id'],
          'archetype_details':{'template_id':{'value':'T'}},
          'content':[{'_type':'CLUSTER','archetype_node_id':'at0001','name':{'value':'B'},
                      'items':[{'_type':'ELEMENT','archetype_node_id':'at0002'}]}]}
    collector=plan.collector();validate_archetype_identities(root,coverage=collector)
    assert collector.result()['counts']['unmodelled_elements']==1


def test_preview_exposes_held_evidence_and_cannot_be_acknowledged(sample):
    from kehrnel.engine.core.types import StrategyContext
    from kehrnel.engine.strategies.openehr.analytics.strategy import OpenEHRAnalyticsStrategy
    payload,_,doc=sample
    doc['canonicalJSON']['content'].append(extra_cluster())
    cfg=json.loads((ROOT/'examples/openehr_analytics/config.canonical.json').read_text())
    cfg['rules']=[]
    strategy=OpenEHRAnalyticsStrategy();ctx=StrategyContext('coverage-test',cfg)
    payload={**payload,'document':doc,'accepted_at':AT}
    result=asyncio.run(strategy.run_op(ctx,'analytics_preview_draft',payload))
    assert result['preview']['state']['status']=='needs_rules'
    assert result['preview']['source_coverage']['status']=='unmodelled_content'
    with pytest.raises(ValueError,match='successful draft preview'):
        asyncio.run(strategy.run_op(ctx,'analytics_review_draft',{**payload,'acknowledgements':{'revision':result['review_requirements']['revision'],'topics':result['review_requirements']['required_topics']}}))


def test_ibm_and_canonical_paths_share_coverage_decisions(sample):
    from test_conversion import ibm_document
    _,rule,document=sample
    document['canonicalJSON']['content'].append(extra_cluster())
    raw,decoder=ibm_document(document)
    actual=Converter(RuleSet.compile([rule]),source_namespace='synthetic-coverage',decoder=decoder).convert(raw,accepted_at=AT)
    expected=project(rule,document)
    assert actual.status==expected.status=='needs_rules' and actual.coverage==expected.coverage
    assert not actual.facts and actual.activity['_id']==expected.activity['_id']


def test_new_source_version_can_withhold_prior_facts_without_changing_activity_count(sample):
    from kehrnel.engine.domains.openehr.analytics.store import activity_dimensions
    _,rule,document=sample
    prior=project(rule,document)
    document['canonicalJSON']['content'].append(extra_cluster())
    document['composition_version']=str(int(document['composition_version'])+1)
    held=project(rule,document)
    assert prior.facts and not held.facts
    assert decide(prior.state(),held.state())=='write'
    assert decide(held.state(),held.state())=='unchanged'
    assert decide(held.state(),prior.state())=='stale'
    assert activity_dimensions(held.activity)==activity_dimensions(prior.activity)


def test_inventory_cannot_be_reused_for_another_template_or_root(sample):
    _,rule,document=sample
    altered=deepcopy(rule);altered.pop('revision')
    altered['catalog']['templates'][0]['template_name']='Other template'
    with pytest.raises(ValueError,match='another template'):RuleRevision.compile(altered)
    document['canonicalJSON']['archetype_details']['template_id']['value']='Other template'
    result=project(rule,document)
    assert result.status=='needs_rules'
    assert any(i['code']=='SOURCE_TEMPLATE_IDENTITY_MISMATCH' for i in result.coverage['issues'])


def test_explicit_wrong_rm_type_is_not_inferred_from_opt(sample):
    _,rule,document=sample
    document['canonicalJSON']['content'][0]['items'][0]['data']['_type']='ELEMENT'
    result=project(rule,document)
    assert result.status=='needs_rules' and not result.facts
    assert any(i['rm_type']=='ELEMENT' for i in result.coverage['issues'])
