"""Resolve a portable reader from an operational environment's strategy.

This operation performs no I/O or activation. The resulting profile is pinned
in the analytics configuration and can also be used by an isolated worker.
"""
from copy import deepcopy
from pathlib import Path
import json
import jsonschema
from .source import digest

SUPPORTED = {'openehr.rps_dual', 'openehr.rps_dual_ibm'}

def merge(base, override):
    result = deepcopy(base)
    for key, value in override.items():
        result[key] = merge(result[key], value) if isinstance(value, dict) and isinstance(result.get(key), dict) else deepcopy(value)
    return result

def source_from_strategy(*, environment_id, database, strategy_id, config):
    if strategy_id not in SUPPORTED:
        raise ValueError('Select an operational RPS Dual or IBM environment as the source')
    if not environment_id or not database:
        raise ValueError('The source environment needs an identity and database')
    root = Path(__file__).resolve().parents[3] / 'strategies/openehr'
    cfg = merge(json.loads((root / strategy_id.split('.')[-1] / 'defaults.json').read_text()), config)
    jsonschema.validate(cfg, json.loads((root / strategy_id.split('.')[-1] / 'schema.json').read_text()))
    expected_encoding = 'compact_prefix' if strategy_id.endswith('_ibm') else 'negative_int'
    encoding = cfg.get('transform', {}).get('coding', {}).get('atcodes', {}).get('strategy')
    if encoding != expected_encoding:
        raise ValueError('The source at-code encoding is not supported by this analytics reader')
    document, node = cfg['fields']['document'], cfg['fields']['node']
    reader = cfg.get('sourceReader', {})
    ibm = strategy_id == 'openehr.rps_dual_ibm'
    source = {
        'mode': 'mongodb', 'environment_id': environment_id,
        'strategy_id': strategy_id, 'connection_ref': environment_id,
        'database': database, 'collection': cfg['collections']['compositions']['name'],
        'format': 'ibm', 'profile': 'ctti_ibm_legacy_v1' if ibm else 'rps_dual_full_chain_v1',
        'dictionary_format': reader.get('dictionary_format', 'kehrnel_documents'),
        'codes_collection': cfg['collections']['codes']['name'],
        'values_collection': cfg['collections']['shortcuts']['name'],
        'fields': {'composition_id': '_id' if ibm else document['comp_id'], 'patient_id': document['ehr_id'],
                   'template': document['tid'], 'template_id': 'template_id' if ibm else document['tid'],
                   'version': document['v'], 'committed_at': document['time_committed'],
                   **reader.get('fields', {})},
        'nodes': {'nodes': document['cn'], 'path': node['p'], 'instances': node['pi'],
                  'data': node['data'], 'keys': 'kp', 'separator': cfg['paths']['separator'],
                  'instance_mode': 'local_preorder' if ibm else 'full_chain',
                  'value_prefix': '$>' if ibm else '',
                  'at_prefixes': {'D':'at000', 'C':'at00', 'B':'at0', 'A':'at'} if ibm else {}},
    }
    if source['dictionary_format'] == 'ctti_rows' and not ibm:
        raise ValueError('CTTI row dictionaries require the IBM source strategy')
    source['source_config_digest'] = digest(source)
    return source

def configuration_for_link(link):
    result = {}
    # Existing-source registrations deliberately have no runtime activation.
    runtime = link.get('kehrnel') or {}
    values = [link.get('mergedConfig'), link.get('configOverrides')] if link.get('configurationSource') == 'authored' else [runtime.get('config'), link.get('mergedConfig'), link.get('configOverrides')]
    for value in values:
        if isinstance(value, dict): result = merge(result, value)
    return result
