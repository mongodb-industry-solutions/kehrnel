"""Server-owned SNOMED authority and read-only hosted catalog routing."""
from __future__ import annotations
import asyncio
import copy
import json
import os
import weakref
from urllib.parse import urlsplit
from kehrnel.engine.core.errors import KehrnelError

CATALOG_MUTATIONS = frozenset({
    'snomed_ingest_release', 'snomed_upload_release', 'snomed_initiate_release_upload',
    'snomed_finalize_release_upload', 'snomed_rebuild_sidecar', 'snomed_ensure_indexes',
})
HOSTED_FORBIDDEN_OPS = CATALOG_MUTATIONS | {"snomed_prepare_rf2", 'snomed_inspect_release', 'snomed_diff_release'}
MODES = {'hosted_reference', 'customer_owned'}
_CLIENTS = weakref.WeakKeyDictionary()
DEFAULT_SOURCE = {
    'healthcaredatalab': {
        'database': 'terminology', 'uri_env': 'SNOMED_AUTHORITY_URI',
        'concepts': 'snomed-irbd', 'terms': 'snomed-term-search',
        'release_id': '20260601', 'projection_id': 'snomed-term-search-20260601',
        'lexical_index': 'snomed_text_idx', 'vector_index': 'snomed_voyage_idx',
    }
}

def mode(config):
    # Omitted mode preserves older customer-owned activations. New pack defaults
    # explicitly select hosted_reference, without any collection-copy seed.
    value = (config.get('source') or {}).get('mode', 'customer_owned')
    if value not in MODES:
        raise KehrnelError(code='SNOMED_SOURCE_MODE_INVALID', status=400, message='Choose hosted_reference or customer_owned.')
    return value

def sources():
    raw = os.getenv('KEHRNEL_SNOMED_SOURCES_JSON')
    try:
        registry = json.loads(raw) if raw else DEFAULT_SOURCE
    except ValueError as exc:
        raise KehrnelError(code='SNOMED_SOURCE_REGISTRY_INVALID', status=503, message='The operator SNOMED source registry is invalid.') from exc
    if not isinstance(registry, dict):
        raise KehrnelError(code='SNOMED_SOURCE_REGISTRY_INVALID', status=503, message='The operator SNOMED source registry must be an object.')
    return registry

def reference(config):
    source_id = (config.get('source') or {}).get('id', 'healthcaredatalab')
    entry = sources().get(source_id)
    if not isinstance(entry, dict) or not all(isinstance(entry.get(k), str) and entry[k] for k in ['database', 'uri_env', 'concepts', 'terms', 'release_id', 'projection_id', 'lexical_index']):
        raise KehrnelError(code='SNOMED_REFERENCE_UNKNOWN', status=503, message='The selected hosted terminology authority is not configured.')
    return source_id, entry

def effective_config(config):
    if mode(config) != 'hosted_reference':
        return config
    cfg = copy.deepcopy(config)
    source_id, entry = reference(cfg)
    cfg['source'] = {'mode': 'hosted_reference', 'id': source_id}
    cfg.setdefault('release', {})['id'] = entry['release_id']
    cfg.setdefault('collections', {}).update(concepts=entry['concepts'], terms=entry['terms'], sidecar_enabled=True)
    cfg.setdefault('search', {}).update(lexical_strategy='atlas_search', atlas_index=entry['lexical_index'])
    cfg.setdefault('retrieval', {}).update(vector_index=entry.get('vector_index', ''), vector_path='embedText', vector_mode='autoEmbed')
    return cfg

def evidence(config):
    if mode(config) != 'hosted_reference':
        return {'mode': 'customer_owned', 'releaseId': (config.get('release') or {}).get('id')}
    source_id, entry = reference(config)
    return {'mode': 'hosted_reference', 'id': source_id, 'releaseId': entry['release_id'], 'projectionId': entry['projection_id'], 'readOnly': True}

def reject_catalog_write(config):
    if mode(config) == 'hosted_reference':
        raise KehrnelError(code='SNOMED_HOSTED_READ_ONLY', status=403, message='Hosted terminology is read-only. Ingest your own authorized source in customer-owned mode.')

def safe_pipeline(pipeline, allowed):
    if not isinstance(pipeline, list):
        raise KehrnelError(code='INVALID_PLAN', status=400, message='A terminology pipeline must be an array.')
    def visit(value):
        if isinstance(value, dict):
            if any(k in value for k in ['$out', '$merge']):
                raise KehrnelError(code='SNOMED_HOSTED_READ_ONLY', status=403, message='Hosted terminology pipelines cannot write data.')
            for key, child in value.items():
                if key in {'$lookup', '$unionWith', '$graphLookup'}:
                    name = child if isinstance(child, str) else child.get('from', child.get('coll')) if isinstance(child, dict) else None
                    if not isinstance(name, str) or name not in allowed or (isinstance(child, dict) and "db" in child):
                        raise KehrnelError(code='SNOMED_COLLECTION_NOT_ALLOWED', status=403, message='Hosted terminology joins must remain inside the configured catalog.')
                visit(child)
        elif isinstance(value, list):
            for child in value: visit(child)
    visit(pipeline)

class ReadOnlyCollection:
    ALLOWED = {'find', 'find_one', 'count_documents', 'estimated_document_count', 'distinct', 'list_indexes', 'index_information', 'list_search_indexes'}
    def __init__(self, collection, names): self._collection, self._names = collection, names
    def aggregate(self, pipeline, **kwargs):
        safe_pipeline(pipeline, self._names)
        return self._collection.aggregate(pipeline, **kwargs)
    def __getattr__(self, name):
        if name not in self.ALLOWED:
            raise KehrnelError(code='SNOMED_HOSTED_READ_ONLY', status=403, message='The hosted catalog only supports read operations.')
        return getattr(self._collection, name)

class CatalogDatabase:
    def __init__(self, db, names): self._db, self._names = db, frozenset(names)
    def __getitem__(self, name):
        if name not in self._names:
            raise KehrnelError(code='SNOMED_COLLECTION_NOT_ALLOWED', status=403, message='This collection is outside the configured hosted catalog.')
        return ReadOnlyCollection(self._db[name], self._names)

class RoutedDatabase:
    def __init__(self, catalog, workspace, names, workspace_names):
        self.catalog, self.workspace, self.names = catalog, workspace, frozenset(names)
        self.workspace_names = frozenset(workspace_names)
    def __getitem__(self, name):
        if name in self.names: return self.catalog[name]
        if name not in self.workspace_names:
            raise KehrnelError(code='SNOMED_COLLECTION_NOT_ALLOWED', status=403, message='The terminology collection is not configured for this tenant.')
        return self.workspace[name]

def catalog_database(config):
    _, entry = reference(config)
    uri = os.getenv(entry['uri_env'])
    if not uri:
        raise KehrnelError(code='SNOMED_REFERENCE_NOT_CONNECTED', status=503, message='The hosted terminology authority is not connected.')
    from kehrnel.persistence.mongodb.connection import get_client
    loop = asyncio.get_running_loop()
    clients = _CLIENTS.setdefault(loop, {})
    key = (entry['uri_env'], uri)
    if key not in clients: clients[key] = get_client({'db': {'uri': uri}})
    return CatalogDatabase(clients[key][entry['database']], [entry['concepts'], entry['terms']])

def reject_protected_workspace(config, bindings):
    target = config.get('database') or config.get('database_name')
    uri = (bindings.get('db') or {}).get('uri') if isinstance(bindings, dict) else None
    if not uri: return
    for entry in sources().values():
        authority_uri = os.getenv(entry.get('uri_env', '')) if isinstance(entry, dict) else None
        if authority_uri and target == entry['database'] and urlsplit(uri).hostname == urlsplit(authority_uri).hostname:
            raise KehrnelError(code='SNOMED_REFERENCE_PROTECTED', status=403, message='The shared authority cannot be used as a tenant workspace or customer ingestion destination.')

def routed_database(ctx, config):
    workspace = getattr((ctx.adapters or {}).get('storage'), 'db', None)
    if workspace is None:
        raise KehrnelError(code='MONGODB_ADAPTER_REQUIRED', status=500, message='A tenant workspace storage binding is required.')
    reject_protected_workspace(config, ctx.bindings or {})
    if mode(config) != 'hosted_reference': return workspace
    _, entry = reference(config)
    collections = config.get('collections') or {}
    workspace_names = [collections.get(k, default) for k, default in [('value_sets', 'snomed_value_sets'), ('grounding_reviews', 'snomed_grounding_reviews'), ('gold_notes', 'snomed_gold_notes'), ('usage_events', 'snomed_usage_events')]]
    if set(workspace_names) & {entry['concepts'], entry['terms']}:
        raise KehrnelError(code='SNOMED_COLLECTION_COLLISION', status=400, message='Workspace collections must be separate from the hosted catalog.')
    return RoutedDatabase(catalog_database(config), workspace, [entry['concepts'], entry['terms']], workspace_names)
