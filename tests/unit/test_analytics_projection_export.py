from copy import deepcopy
import json
from pathlib import Path
from bson import json_util

import pytest

from kehrnel.contracts.deployment_configuration import DeploymentCompilationRequest
from kehrnel.engine.core.deployment_configuration import compile_deployment_configuration
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.strategies.openehr.rps_dual.config import normalize_config, build_flattener_config, build_coding_opts
from kehrnel.engine.strategies.openehr.rps_dual.ingest.exceptions_g import UnknownCodeError
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.engine.strategies.openehr.rps_dual_ibm.ingest.flattener import IBMCompositionFlattener

ROOT_ID = 'openEHR-EHR-COMPOSITION.export_test.v1'
OBS_ID = 'openEHR-EHR-OBSERVATION.export_test.v1'
TEMPLATE_ID = 'ExportTest.v1'
PACK = Path(__file__).resolve().parents[2] / 'src/kehrnel/engine/strategies/openehr/rps_dual_ibm'


def fields():
    prefix = f"/content[{OBS_ID},'Target/Fetus']/data[at0001]/items"
    types = ['DV_DATE_TIME', 'DV_TEXT', 'DV_BOOLEAN', 'DV_DURATION', 'DV_QUANTITY', 'DV_CODED_TEXT', 'DV_IDENTIFIER', 'DV_URI']
    result = []
    for index, rm_type in enumerate(types, 2):
        selector = f"at{index:04d}" + (" and name/value='Recorded date'" if index == 2 else '')
        result.append({'path': f'{prefix}[{selector}]/value', 'rmType': rm_type})
    return result


async def compile_fields(selected):
    manifest = load_strategy('openehr.rps_dual_ibm', PACK)
    return await compile_deployment_configuration(manifest, DeploymentCompilationRequest(
        config={'database': 'configuration_test'}, dictionaries={'codes': {'ar_code': {ROOT_ID: '1', OBS_ID: '2'}, 'at': {}}},
        templates=[{'templateId': TEMPLATE_ID, 'fields': selected}]))


def flattener(result):
    cfg = normalize_config(result['config'])
    config = build_flattener_config(cfg)
    config['role'] = 'secondary'
    instance = IBMCompositionFlattener(db=None, config=config, mappings_path='', mappings_content=result['projection']['mappings'], coding_opts=build_coding_opts(cfg))
    instance.code_book = deepcopy(result['projection']['codes'])
    instance.shortcut_keys = result['projection']['shortcuts']['keys']
    instance.shortcut_vals = result['projection']['shortcuts']['values']
    instance._refresh_codec()
    return instance


@pytest.mark.asyncio
async def test_export_resolves_every_parent_dv_selection_and_mixed_search_types():
    result = await compile_fields(fields())
    assert result['readiness']['projection'] is True
    assert result['projection']['algorithmVersion'] == '2'
    rules = result['projection']['compiledRulesByTemplate'][TEMPLATE_ID]
    assert len(rules) == 8
    assert [rule['copy'][1] for rule in rules] == ['data.value.value', 'data.value.value', 'data.value.value', 'data.value.value', 'data.value.magnitude', 'data.value.defining_code.code_string', 'data.value.id', 'data.value.value']
    data = result['indexes']['atlasSearch'][0]['definition']['mappings']['fields']['sn']['fields']['data']['fields']['v']['fields']
    assert {entry['type'] for entry in data['v']} == {'date', 'string', 'boolean', 'token'}
    assert data['magnitude'] == {'type': 'number'}
    assert data['df']['fields']['cs'] == {'type': 'token'}
    assert data['id'] == {'type': 'token'}
    assert all('=' not in name and ']' not in name for name in data)


@pytest.mark.asyncio
async def test_named_ancestor_and_leaf_predicates_survive_ingest_and_rebuild():
    result = await compile_fields(fields())
    instance = flattener(result)
    values = [ {'_type': 'DV_DATE_TIME', 'value': '2026-11-05T12:00:00Z'}, {'_type': 'DV_TEXT', 'value': 'selected'}, {'_type': 'DV_BOOLEAN', 'value': True}, {'_type': 'DV_DURATION', 'value': 'P1D'}, {'_type': 'DV_QUANTITY', 'magnitude': 42, 'units': 'kg'}, {'_type': 'DV_CODED_TEXT', 'value': 'display', 'defining_code': {'code_string': 'A'}}, {'_type': 'DV_IDENTIFIER', 'id': 'identifier'}, {'_type': 'DV_URI', 'value': 'https://example.test'} ]
    items = [{'_type': 'ELEMENT', 'archetype_node_id': f'at{i:04d}', 'name': {'_type': 'DV_TEXT', 'value': 'Recorded date' if i == 2 else f'Item {i}'}, 'value': value} for i, value in enumerate(values, 2)]
    wrong = deepcopy(items[0]); wrong['name']['value'] = 'Another date'; items.append(wrong)
    def observation(name):
        return {'_type': 'OBSERVATION', 'archetype_node_id': OBS_ID, 'name': {'_type': 'DV_TEXT', 'value': name}, 'data': {'_type': 'ITEM_TREE', 'archetype_node_id': 'at0001', 'items': deepcopy(items)}}
    raw = {'_id': '00000000-0000-0000-0000-000000000001', 'ehr_id': '00000000-0000-0000-0000-000000000002', 'composition_version': 1,
           'canonicalJSON': {'_type': 'COMPOSITION', 'archetype_node_id': ROOT_ID, 'archetype_details': {'template_id': {'value': TEMPLATE_ID}}, 'content': [observation('Target/Fetus'), observation('Other/Fetus')]}}
    base, search = instance.transform_composition(raw)
    assert search is not None
    assert len(search['sn']) == 8
    rebuilt = instance.project_search_from_flattened(base)
    assert json.loads(json_util.dumps(rebuilt['sn'])) == json.loads(json_util.dumps(search['sn']))


@pytest.mark.asyncio
async def test_scalar_and_document_type_union_keeps_both_nested_leaves():
    selected = [ {'path': '/one', 'extract': 'value', 'rmType': 'DV_TEXT'}, {'path': '/two', 'extract': 'value.defining_code.code_string', 'rmType': 'DV_CODED_TEXT'}, {'path': '/three', 'extract': 'value.magnitude', 'rmType': 'DV_QUANTITY'} ]
    result = await compile_fields(selected)
    types = result['indexes']['atlasSearch'][0]['definition']['mappings']['fields']['sn']['fields']['data']['fields']['v']
    assert {entry['type'] for entry in types} == {'string', 'document'}
    document = next(entry for entry in types if entry['type'] == 'document')
    assert document['fields']['df']['fields']['cs'] == {'type': 'token'}
    assert document['fields']['magnitude'] == {'type': 'number'}


@pytest.mark.parametrize('strategy,code,expected', [('literal', 'at0001', 'at0001'), ('compact_prefix', 'at0001', 'A1'), ('negative_int', 'at0001', -1)])
def test_secondary_deterministic_at_encoding_does_not_allocate_dictionary_entries(strategy, code, expected):
    instance = CompositionFlattener(db=None, config={'role': 'secondary'}, coding_opts={'atcodes': {'strategy': strategy}}, mappings_path='', mappings_content={})
    before = deepcopy(instance.code_book)
    assert instance._at_code_to_int(code) == expected
    assert instance.code_book == before


@pytest.mark.parametrize('strategy,code', [('sequential', 'at0001'), ('negative_int', 'at0001.1')])
def test_secondary_allocated_at_codes_still_require_exact_dictionary_entries(strategy, code):
    instance = CompositionFlattener(db=None, config={'role': 'secondary'}, coding_opts={'atcodes': {'strategy': strategy}}, mappings_path='', mappings_content={})
    with pytest.raises(UnknownCodeError):
        instance._at_code_to_int(code)
