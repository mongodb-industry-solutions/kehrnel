"""Record a real two-OPT reuse example with synthetic inputs, no database or network.

PYTHONPATH=src python examples/openehr_analytics/reuse_business_variable.py --output /tmp/business-reuse.json
The JSON is a draft/preview fixture, never an approved or activated release.
"""
import argparse
from contextlib import redirect_stdout
from copy import deepcopy
from hashlib import sha256
import io,json,random
from pathlib import Path
from kehrnel.engine.domains.openehr.analytics.authoring import compile_draft,proposal_for
from kehrnel.engine.domains.openehr.analytics.business_catalog import catalog_page
from kehrnel.engine.domains.openehr.analytics.converter import Converter
from kehrnel.engine.domains.openehr.analytics.rules import RuleSet
from kehrnel.engine.domains.openehr.templates.generator import kehrnelGenerator
from kehrnel.engine.domains.openehr.templates.parser import TemplateParser

ROOT=Path(__file__).resolve().parents[2]
AT='2026-12-01T00:00:00Z'
KEY='laboratory_test_analyte.test_result'


def record():
    ref=ROOT/'src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference'
    paths=[ref/'templates/sample_laboratory_v0_4.opt',ROOT/'samples/templates/T-IGR-BIOLOGY.opt']
    first=compile_draft({'opt_xml':paths[0].read_text(),'effective_at':AT})['rule']
    base={'opt_xml':paths[1].read_text(),'effective_at':AT}
    proposal=proposal_for(base,[first])
    field=next(f for f in proposal['fields'] if f['path'].endswith('items[openEHR-EHR-CLUSTER.laboratory_test_analyte.v1]/items[at0001]'))
    # These explicit exclusions are fixture authoring decisions, not default UI behaviour.
    edits={'exclude_field_ids':[i['field_id'] for i in proposal['issues']],
           'reuse_variables':{field['field_id']:{'key':KEY,'rule_revision':first['revision'],
               'shape_policy':'preserve_source',
               'reason':'Same analyte-result slot; preserve source units, arrays and analyte context. Synthetic demonstration, not clinical approval.'}}}
    result=compile_draft({**base,'edits':edits},[first]);rules=[first,result['rule']]
    documents=[json.loads((ref/'envelopes/sample_laboratory_v0_4.ndjson').read_text().splitlines()[0])]
    random.seed(20261119)
    with redirect_stdout(io.StringIO()): canonical=kehrnelGenerator(TemplateParser(paths[1])).generate_random()
    documents.append({'_id':'synthetic-biology','ehr_id':'synthetic-owner','template_name':'T-IGR-BIOLOGY',
                      'template_id':'T-IGR-BIOLOGY','composition_version':1,'composition_date':AT,'canonicalJSON':canonical})
    converter=Converter(RuleSet.compile(rules),source_namespace='synthetic-reuse-fixture')
    results=[converter.convert(d,accepted_at=AT) for d in documents]
    selected=[]
    for result in results:
        for fact in result.facts:
            value=fact['clinical'].get('laboratory_test_analyte',{}).get('test_result')
            if value is not None:selected.append({'template':result.template,'fact_id':fact['_id'],'value':value})
    assert {r['template'] for r in selected}=={'sample_laboratory_v0.4','T-IGR-BIOLOGY'}
    code=ROOT/'src/kehrnel/engine/domains/openehr/analytics'
    code_digests={p.name:sha256(p.read_bytes()).hexdigest() for p in sorted(code.glob('*.py'))}
    return {'provenance':{'inputs':'real OPTs, synthetic composition values','database_writes':False,'network':False,
                         'as_of':AT,'seed':20261119,'opt_digests':{p.name:sha256(p.read_bytes()).hexdigest() for p in paths},
                         'analytics_code_digests':code_digests},
            'authoring_edits':edits,'proposal_issues':proposal['issues'],'review':'draft; approval not recorded',
            'rules':rules,'catalog':catalog_page(rules,as_of=AT,search=KEY),
            'bindings':catalog_page(rules,as_of=AT,key=KEY),
            'projected_examples':selected,
            'query_note':'Read the common clinical path across templates. For scalar/array variants use an explicit unwind; filter analyte and units before quantitative analysis.'}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True,type=Path);args=parser.parse_args()
    result=record();args.output.write_text(json.dumps(result,indent=2,default=str)+'\n')
    print(json.dumps({'output':str(args.output),'templates':2,'projected_examples':len(result['projected_examples']),'database_writes':False}))
