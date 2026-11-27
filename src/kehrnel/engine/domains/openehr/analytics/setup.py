"""One portable source/output contract for the GUI, SDK and command-line worker.

Connection strings are bindings, never configuration. Validation has no I/O.
"""
from copy import deepcopy
import re
import jsonschema


def validate_setup(config, schema):
    jsonschema.validate(config, schema)
    source = config['source']
    mode = source.get('mode', 'upload')
    if mode == 'mongodb':
        for key in ('connection_ref', 'database', 'collection'):
            if not isinstance(source.get(key), str) or not source[key].strip():
                raise ValueError(f'MongoDB source requires {key}')
        if source['database'] == config['database']:
            raise ValueError('Source and analytics output must use different databases')
        if source.get('environment_id') and (not source.get('strategy_id') or not source.get('source_config_digest') or source.get('connection_ref') != source['environment_id']):
            raise ValueError('An environment source requires its pinned operational strategy profile')
    if source['format'] == 'ibm':
        if mode != 'mongodb' and not (source.get('codes') and source.get('shortcuts')):
            raise ValueError('IBM file input requires pinned dictionaries; choose MongoDB or upload canonical compositions')
        if source.get('profile') not in ('ctti_ibm_legacy_v1', 'rps_dual_full_chain_v1'):
            raise ValueError('Select an explicit IBM source profile')
    # Unknown schema properties already fail. Do not accept credentials in free
    # text labels or in dictionaries that could otherwise reach exported bundles.
    def inspect(value):
        if isinstance(value, dict):
            for k, v in value.items():
                normalized = re.sub(r'[_-]', '', k).lower()
                if k not in ('DV_URI', 'DV_EHR_URI') and re.search(r'(?:uri|tokens?|passwords?|passwd|credentials?|secrets?|apikeys?|servicekeys?|privatekeys?|connectionstring|authorization|bindings|bindingsref)$', normalized):
                    raise ValueError('Connection secrets belong in bindings, not configuration')
                inspect(v)
        elif isinstance(value, list):
            for v in value: inspect(v)
        elif isinstance(value, str) and re.search(r'mongodb(?:\+srv)?://|[a-z][a-z0-9+.-]*://[^/\s]*@|-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----', value, re.I):
            raise ValueError('Connection strings belong in bindings, not configuration')
    inspect(config)
    return deepcopy(config)


def worker_environment(environment, config):
    """Resolve portable database/collection settings and reject conflicting inputs."""
    result = dict(environment)
    source = config.get('source', {})
    values = {'ANALYTICS_TARGET_DATABASE': config.get('database'),
              'ANALYTICS_SOURCE_NAMESPACE': config.get('source_namespace'),
              'ANALYTICS_COLLECTION_PREFIX': config.get('collection_prefix')}
    if source.get('mode') == 'mongodb':
        values.update(ANALYTICS_SOURCE_DATABASE=source.get('database'),
                      ANALYTICS_SOURCE_COLLECTION=source.get('collection'))
    for key, value in values.items():
        if value:
            if result.get(key) and result[key] != value:
                raise ValueError({'ANALYTICS_TARGET_DATABASE': 'Configuration and environment target databases differ', 'ANALYTICS_COLLECTION_PREFIX': 'Configuration and environment collection prefixes differ', 'ANALYTICS_SOURCE_NAMESPACE': 'Configuration and environment source namespaces differ'}.get(key, f'{key} conflicts with the reviewed configuration'))
            result[key] = value
    return result
