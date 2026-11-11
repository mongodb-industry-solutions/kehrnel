"""Operator-only projection preparation; never copies a catalog to a tenant.

Default: bounded read-only estimate. --apply writes a NEW versioned collection
inside the configured authority and a build receipt. Publication is separate.
"""
from __future__ import annotations
import argparse
import asyncio
import hashlib
import json
import re
from datetime import datetime, timezone
from kehrnel.engine.domains.snomedct import build_term_documents
from kehrnel.engine.strategies.snomedct.mongodb.source_policy import reference
from kehrnel.persistence.mongodb.connection import get_client
import os
from urllib.parse import urlsplit

async def run(args):
    _, source = reference({'source': {'id': args.source_id}})
    uri = os.environ.get(source['uri_env'])
    if not uri:
        raise ValueError('The operator authority connection is not configured.')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', args.projection_id):
        raise ValueError('Use a versioned projection identifier.')
    target = 'snomed-term-search-' + args.projection_id
    if target in {source['concepts'], source['terms']} or len(target.encode()) > 120:
        raise ValueError('A projection build must use a new collection name.')
    if args.concept_limit < 0 or not 1 <= args.sample_size <= 2000:
        raise ValueError('Use a nonnegative concept limit and a sample size from 1 to 2000.')
    if args.apply:
        operator_uri = os.environ.get('SNOMED_PROJECTION_OPERATOR_URI')
        if not operator_uri or urlsplit(operator_uri).hostname != urlsplit(uri).hostname:
            raise ValueError('Projection writes require the separate operator connection to the same authority cluster.')
        uri = operator_uri
    client = get_client({'db': {'uri': uri}})
    try:
        db = client[source['database']]
        canonical = db[source['concepts']]
        query = {'releaseId': source['release_id'], 'active': True}
        available = await canonical.count_documents(query, maxTimeMS=10000)
        selection_count = min(args.concept_limit, available) if args.concept_limit else available
        plan = {'sourceId': args.source_id, 'releaseId': source['release_id'], 'canonicalCollection': source['concepts'], 'targetCollection': target, 'projectionId': args.projection_id, 'selection': {'activeConcepts': True, 'languages': args.languages, 'order': 'conceptId', 'conceptLimit': args.concept_limit or None}, 'availableConcepts': available, 'selectedConcepts': selection_count, 'consumerDistribution': False}
        if not args.apply:
            sampled = terms = 0
            async for concept in canonical.find(query).sort('conceptId', 1).limit(min(selection_count, args.sample_size)):
                sampled += 1
                terms += len(build_term_documents(concept, release_id=source['release_id'], language_codes=args.languages))
            return {**plan, 'mode': 'read_only_plan', 'sampleConcepts': sampled, 'sampleTerms': terms, 'estimatedTerms': round(terms / sampled * selection_count) if sampled else 0, 'note': 'Estimate only. Existing concepts, projections and indexes remain intact. Search index provisioning and publication require a separate reviewed maintenance action.'}
        existing = await db.list_collection_names()
        if target in existing:
            raise ValueError('The target already exists. Preserve it and choose a new projection version.')
        await db.create_collection(target)
        count = concepts = 0
        digest = hashlib.sha256()
        batch = []
        receipt = {'_id': 'projection:' + args.projection_id, **plan, 'state': 'building', 'startedAt': datetime.now(timezone.utc)}
        await db['snomed-model-state'].insert_one(receipt)
        async for concept in canonical.find(query).sort('conceptId', 1).limit(selection_count):
            concepts += 1
            digest.update((source['release_id'] + '|' + concept['conceptId'] + '\n').encode())
            for term in build_term_documents(concept, release_id=source['release_id'], language_codes=args.languages):
                term['projectionId'] = args.projection_id
                term['sourceCollection'] = source['concepts']
                batch.append(term)
                count += 1
            if len(batch) >= 1000:
                await db[target].insert_many(batch, ordered=True)
                batch = []
        if batch:
            await db[target].insert_many(batch, ordered=True)
        persisted = await db[target].count_documents({})
        if persisted != count or concepts != selection_count or await canonical.count_documents(query) != available:
            raise ValueError('Source or projection counts changed. Preserve the staging collection for inspection.')
        completed = {'state': 'staged', 'conceptCount': concepts, 'termCount': count, 'canonicalKeyDigest': digest.hexdigest(), 'completedAt': datetime.now(timezone.utc), 'searchIndexesProvisioned': False, 'published': False}
        await db['snomed-model-state'].update_one({'_id': receipt['_id']}, {'$set': completed})
        return {**plan, **completed, 'mode': 'staged_projection'}
    finally:
        client.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-id', default='healthcaredatalab')
    parser.add_argument('--projection-id', required=True)
    parser.add_argument('--languages', nargs='+', choices=['en', 'es'], default=['en', 'es'])
    parser.add_argument('--concept-limit', type=int, default=0)
    parser.add_argument('--sample-size', type=int, default=100)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(run(args)), default=str, indent=2))
    except Exception as exc:
        # Mongo errors may contain credentials. Report type/code only.
        print(json.dumps({'errorType': type(exc).__name__, 'errorCode': getattr(exc, 'code', None)}))
        raise SystemExit(1)
