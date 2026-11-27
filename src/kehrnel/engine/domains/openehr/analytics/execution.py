"""Bounded source reads and publication shared by hosted and standalone Kehrnel."""
from contextlib import contextmanager
from pymongo import MongoClient
from .converter import Converter, SourceFields
from .rules import RuleSet
from .profiles import load_ibm_decoder, pinned_decoder
from .projection import utc


@contextmanager
def source_connection(config, bindings):
    source = config['source']
    connection = (bindings or {}).get('extras', {}).get('analytics_source') or {}
    if source.get('mode') != 'mongodb':
        raise ValueError('Choose a MongoDB source first')
    if connection.get('connection_ref') != source.get('connection_ref') or not connection.get('uri'):
        raise ValueError('The configured source connection is not available on this Kehrnel instance')
    client = MongoClient(connection['uri'], serverSelectionTimeoutMS=8000,
                         socketTimeoutMS=15000, tz_aware=True,
                         uuidRepresentation='standard', appname='kehrnel-analytics-source')
    try:
        yield client[source['database']][source['collection']]
    finally:
        client.close()


def converter_for(config, source=None):
    profile = config['source']
    decoder = None
    if profile['format'] == 'ibm':
        if profile.get('codes') and profile.get('shortcuts'):
            decoder = pinned_decoder(profile)
        elif source is not None:
            decoder = load_ibm_decoder(source.database, {}, profile)
        else:
            raise ValueError('IBM conversion needs the configured source connection or pinned dictionaries')
    return Converter(RuleSet.compile(config['rules']), source_namespace=config['source_namespace'],
                     fields=SourceFields(**profile.get('fields', {})), decoder=decoder)


def test_source(config, bindings):
    with source_connection(config, bindings) as source:
        if source.name not in source.database.list_collection_names(filter={'name': source.name}):
            raise ValueError('The configured source collection does not exist')
        converter = converter_for(config, source)
        return {'ok': True, 'database': source.database.name, 'collection': source.name,
                'format': config['source']['format'], 'patient_records_read': 0,
                'decoder_revision': converter.decoder.revision if converter.decoder else 'canonical-openEHR/v1'}


def process_documents(config, documents, *, accepted_at, store=None):
    if not config.get('rules'):
        raise ValueError('Approve and apply at least one template rule before processing')
    converter = converter_for(config)
    return _process(converter, documents, accepted_at, store)


def _process(converter, documents, accepted_at, store):
    result = {'read': 0, 'facts': 0, 'written': 0, 'unchanged': 0, 'held': 0,
              'database_writes': store is not None}
    for document in documents:
        projection = converter.convert(document, accepted_at=accepted_at)
        result['read'] += 1
        result['facts'] += len(projection.facts)
        if projection.status != 'projected':
            result['held'] += 1
        if store is not None:
            receipt = store.write(projection)
            if receipt['decision'] == 'write': result['written'] += 1
            elif receipt['decision'] == 'unchanged': result['unchanged'] += 1
    return result


def source_sample(config, bindings, *, template, limit, accepted_at, store=None):
    if not config.get('rules'):
        raise ValueError('Approve and apply at least one template rule before processing')
    with source_connection(config, bindings) as source:
        converter = converter_for(config, source)
        if template not in converter.rules.by_template:
            raise ValueError('Select an exact template with an applied rule')
        documents = source.find({converter.fields.template: template}).sort(converter.fields.composition_id, 1).limit(limit).max_time_ms(10000)
        return _process(converter, documents, accepted_at, store)


def backfill(config, bindings, store, job, max_batches):
    from .jobs import Backfill, run_backfill
    spec = {"batch_size": 25, **job}
    for key in ('accepted_at', 'committed_from', 'committed_until'):
        if spec.get(key): spec[key] = utc(spec[key])
    with source_connection(config, bindings) as source:
        converter = converter_for(config, source)
        return run_backfill(source, store, converter, Backfill(**spec), max_batches=max_batches)
